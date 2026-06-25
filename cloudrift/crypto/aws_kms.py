import asyncio

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from cloudrift.core.exceptions import (
    CryptoError,
    CryptoKeyNotFoundError,
    CryptoPermissionError,
)
from cloudrift.crypto.base import CryptoBackend


class AWSKMSBackend(CryptoBackend):
    """AWS KMS crypto backend (native async via ``aioboto3``).

    Encrypts/decrypts directly against a symmetric KMS key — the same
    ``Encrypt`` / ``Decrypt`` calls the AWS SDK makes, so ciphertext is
    interchangeable with anything encrypted by raw boto3 against the same key.
    A single async client is created lazily and reused.

    Construct via:
    - ``from_access_key`` — static credentials (+ optional session token)
    - ``from_iam_role``   — instance profile / environment / ECS task role
    - ``from_profile``    — named profile from ``~/.aws/credentials``

    ``key_id`` (key id, ARN, or alias) is required to encrypt. It is optional for
    decrypt-only backends — KMS resolves the key from the ciphertext.
    """

    def __init__(
        self,
        session: aioboto3.Session,
        key_id: str | None = None,
        *,
        encryption_context: dict | None = None,
        endpoint_url: str | None = None,
        max_pool_connections: int = 25,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
        client_kwargs: dict | None = None,
    ) -> None:
        self._session = session
        self._key_id = key_id
        self._encryption_context = encryption_context or {}
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
        key_id: str | None = None,
        region: str = "us-east-1",
        aws_session_token: str | None = None,
        **kwargs,
    ) -> "AWSKMSBackend":
        """Authenticate with explicit access key / secret."""
        session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region,
        )
        return cls(session, key_id, **kwargs)

    @classmethod
    def from_iam_role(
        cls,
        key_id: str | None = None,
        region: str = "us-east-1",
        **kwargs,
    ) -> "AWSKMSBackend":
        """Authenticate via IAM role / instance profile / environment variables."""
        session = aioboto3.Session(region_name=region)
        return cls(session, key_id, **kwargs)

    @classmethod
    def from_profile(
        cls,
        profile_name: str,
        key_id: str | None = None,
        region: str = "us-east-1",
        **kwargs,
    ) -> "AWSKMSBackend":
        """Authenticate using a named profile from ``~/.aws/credentials``."""
        session = aioboto3.Session(profile_name=profile_name, region_name=region)
        return cls(session, key_id, **kwargs)

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _ensure(self):
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client_cm = self._session.client(
                    "kms",
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
    # CryptoBackend implementation
    # ------------------------------------------------------------------

    async def encrypt(self, plaintext: bytes) -> bytes:
        if not self._key_id:
            raise CryptoError("AWS KMS key_id is required to encrypt")
        client = await self._ensure()
        try:
            kwargs: dict = {"KeyId": self._key_id, "Plaintext": plaintext}
            if self._encryption_context:
                kwargs["EncryptionContext"] = self._encryption_context
            response = await client.encrypt(**kwargs)
            return response["CiphertextBlob"]
        except ClientError as e:
            self._raise(e)

    async def decrypt(self, ciphertext: bytes) -> bytes:
        client = await self._ensure()
        try:
            kwargs: dict = {"CiphertextBlob": ciphertext}
            if self._encryption_context:
                kwargs["EncryptionContext"] = self._encryption_context
            response = await client.decrypt(**kwargs)
            return response["Plaintext"]
        except ClientError as e:
            self._raise(e)

    def _raise(self, exc: ClientError):
        code = exc.response["Error"]["Code"]
        if code in ("NotFoundException", "KeyUnavailableException"):
            raise CryptoKeyNotFoundError(str(exc)) from exc
        if code in ("AccessDeniedException", "UnauthorizedAccess"):
            raise CryptoPermissionError(str(exc)) from exc
        raise CryptoError(str(exc)) from exc
