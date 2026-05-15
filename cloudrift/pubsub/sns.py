import asyncio
import json
import uuid

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from cloudrift.core.exceptions import PubSubError, PublishError, TopicNotFoundError
from cloudrift.pubsub.base import PubSubBackend


class AWSSNSBackend(PubSubBackend):
    """AWS SNS pub/sub backend (native async via ``aioboto3``).

    A single async SNS client is created lazily on first use and reused for
    the lifetime of the backend.

    Use one of the class methods to construct:
    - ``from_access_key`` — static credentials
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
    ) -> "AWSSNSBackend":
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
    ) -> "AWSSNSBackend":
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
    ) -> "AWSSNSBackend":
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
                    "sns",
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
    # PubSubBackend implementation
    # ------------------------------------------------------------------

    async def publish(
        self, topic: str, message: str, attributes: dict | None = None
    ) -> str:
        client = await self._ensure()
        kwargs: dict = {"TopicArn": topic, "Message": message}
        if attributes:
            kwargs["MessageAttributes"] = {
                k: {"DataType": "String", "StringValue": str(v)}
                for k, v in attributes.items()
            }
        try:
            response = await client.publish(**kwargs)
            return response["MessageId"]
        except ClientError as e:
            self._raise(e, topic)

    async def publish_batch(
        self, topic: str, messages: list[dict]
    ) -> list[str]:
        """Publish up to N messages, automatically chunking in batches of 10."""
        client = await self._ensure()
        all_ids: list[str] = []
        # SNS batch limit is 10
        for i in range(0, len(messages), 10):
            chunk = messages[i : i + 10]
            entries = []
            for j, msg in enumerate(chunk):
                entry: dict = {
                    "Id": str(j),
                    "Message": msg.get("message", json.dumps(msg)),
                }
                attrs = msg.get("attributes")
                if attrs:
                    entry["MessageAttributes"] = {
                        k: {"DataType": "String", "StringValue": str(v)}
                        for k, v in attrs.items()
                    }
                entries.append(entry)
            try:
                response = await client.publish_batch(
                    TopicArn=topic, PublishBatchRequestEntries=entries
                )
                if response.get("Failed"):
                    failed = [f["Id"] for f in response["Failed"]]
                    raise PublishError(f"Failed to publish messages: {failed}")
                all_ids.extend(s["MessageId"] for s in response.get("Successful", []))
            except ClientError as e:
                self._raise(e, topic)
        return all_ids

    async def health_check(self) -> bool:
        try:
            client = await self._ensure()
            await client.list_topics(NextToken="")
            return True
        except Exception:
            return False

    def _raise(self, exc: ClientError, topic: str):
        code = exc.response["Error"]["Code"]
        if code == "NotFound":
            raise TopicNotFoundError(f"Topic not found: {topic}") from exc
        if code in ("AuthorizationError", "AccessDenied"):
            raise PubSubError(f"Access denied for topic: {topic}") from exc
        raise PubSubError(str(exc)) from exc
