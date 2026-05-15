import asyncio
import json

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from cloudrift.core.exceptions import SecretError, SecretNotFoundError, SecretPermissionError
from cloudrift.secrets.base import SecretBackend


class AWSSecretsManagerBackend(SecretBackend):
    """AWS Secrets Manager backend (native async via ``aioboto3``).

    A single async client is created lazily on first use and reused for the
    lifetime of the backend.

    Use one of the class methods to construct:
    - ``from_access_key`` — static credentials (+ optional session token)
    - ``from_iam_role``   — instance profile / environment / ECS task role
    - ``from_profile``    — named profile from ``~/.aws/credentials``
    """

    def __init__(
        self,
        session: aioboto3.Session,
        *,
        endpoint_url: str | None = None,
        max_pool_connections: int = 25,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
        client_kwargs: dict | None = None,
    ) -> None:
        self._session = session
        self._endpoint_url = endpoint_url
        self._config = Config(
            max_pool_connections=max_pool_connections,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
        self._client_kwargs = client_kwargs or {}
        self._client_cm = None
        self._client = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_access_key(
        cls,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region: str = "us-east-1",
        aws_session_token: str | None = None,
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSSecretsManagerBackend":
        """Authenticate with explicit access key / secret."""
        session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region,
        )
        return cls(session, endpoint_url=endpoint_url, **kwargs)

    @classmethod
    def from_iam_role(
        cls,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSSecretsManagerBackend":
        """Authenticate via IAM role / instance profile / environment variables."""
        session = aioboto3.Session(region_name=region)
        return cls(session, endpoint_url=endpoint_url, **kwargs)

    @classmethod
    def from_profile(
        cls,
        profile_name: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSSecretsManagerBackend":
        """Authenticate using a named profile from ``~/.aws/credentials``."""
        session = aioboto3.Session(profile_name=profile_name, region_name=region)
        return cls(session, endpoint_url=endpoint_url, **kwargs)

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _ensure(self):
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client_cm = self._session.client(
                    "secretsmanager",
                    endpoint_url=self._endpoint_url,
                    config=self._config,
                    **self._client_kwargs,
                )
                try:
                    self._client = await self._client_cm.__aenter__()
                except Exception:
                    self._client_cm = None
                    raise
        return self._client

    async def close(self) -> None:
        client_cm, self._client_cm = self._client_cm, None
        self._client = None
        if client_cm is not None:
            await client_cm.__aexit__(None, None, None)

    # ------------------------------------------------------------------
    # SecretBackend implementation
    # ------------------------------------------------------------------

    async def get_secret(self, name: str) -> str:
        client = await self._ensure()
        try:
            response = await client.get_secret_value(SecretId=name)
            return response["SecretString"]
        except ClientError as e:
            self._raise(e, name)

    async def get_secret_json(self, name: str) -> dict:
        raw = await self.get_secret(name)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError) as e:
            raise SecretError(f"Secret '{name}' is not valid JSON") from e

    async def set_secret(self, name: str, value: str) -> None:
        client = await self._ensure()
        try:
            await client.put_secret_value(SecretId=name, SecretString=value)
        except client.exceptions.ResourceNotFoundException:
            await client.create_secret(Name=name, SecretString=value)
        except ClientError as e:
            self._raise(e, name)

    async def delete_secret(self, name: str) -> None:
        client = await self._ensure()
        try:
            await client.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
        except ClientError as e:
            self._raise(e, name)

    async def list_secrets(self, prefix: str = "") -> list[str]:
        client = await self._ensure()
        try:
            names: list[str] = []
            kwargs: dict = {}
            if prefix:
                kwargs["Filters"] = [{"Key": "name", "Values": [prefix]}]
            paginator = client.get_paginator("list_secrets")
            async for page in paginator.paginate(**kwargs):
                for secret in page.get("SecretList", []):
                    names.append(secret["Name"])
            return names
        except ClientError as e:
            self._raise(e, prefix)

    async def health_check(self) -> bool:
        try:
            client = await self._ensure()
            await client.list_secrets(MaxResults=1)
            return True
        except Exception:
            return False

    def _raise(self, exc: ClientError, name: str):
        code = exc.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            raise SecretNotFoundError(f"Secret not found: {name}") from exc
        if code in ("AccessDeniedException", "UnauthorizedAccess"):
            raise SecretPermissionError(f"Access denied for secret: {name}") from exc
        raise SecretError(str(exc)) from exc
