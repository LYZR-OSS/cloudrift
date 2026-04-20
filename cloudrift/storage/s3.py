import asyncio

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from cloudrift.core.exceptions import ObjectNotFoundError, StorageError, StoragePermissionError
from cloudrift.storage.base import StorageBackend


class AWSS3Backend(StorageBackend):
    """AWS S3 storage backend (native async via ``aioboto3``).

    A single async S3 client is created lazily on first use and reused for the
    lifetime of the backend. Call ``await backend.close()`` (or use
    ``async with backend:``) to release the underlying connections.

    Use one of the class methods to construct:
    - ``from_access_key`` — static credentials (+ optional session token for assumed roles)
    - ``from_iam_role``   — instance profile / environment / ECS task role
    - ``from_profile``    — named profile from ``~/.aws/credentials``
    """

    def __init__(
        self,
        bucket: str,
        session: aioboto3.Session,
        *,
        endpoint_url: str | None = None,
        max_pool_connections: int = 50,
        connect_timeout: float = 10.0,
        read_timeout: float = 60.0,
        client_kwargs: dict | None = None,
    ) -> None:
        self.bucket = bucket
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
        bucket: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region: str = "us-east-1",
        aws_session_token: str | None = None,
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSS3Backend":
        """Authenticate with explicit access key / secret (+ optional STS session token)."""
        session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region,
        )
        return cls(bucket, session, endpoint_url=endpoint_url, **kwargs)

    @classmethod
    def from_iam_role(
        cls,
        bucket: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSS3Backend":
        """Authenticate via IAM role / instance profile / environment variables."""
        session = aioboto3.Session(region_name=region)
        return cls(bucket, session, endpoint_url=endpoint_url, **kwargs)

    @classmethod
    def from_profile(
        cls,
        bucket: str,
        profile_name: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSS3Backend":
        """Authenticate using a named profile from ``~/.aws/credentials``."""
        session = aioboto3.Session(profile_name=profile_name, region_name=region)
        return cls(bucket, session, endpoint_url=endpoint_url, **kwargs)

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _ensure(self):
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client_cm = self._session.client(
                    "s3",
                    endpoint_url=self._endpoint_url,
                    config=self._config,
                    **self._client_kwargs,
                )
                self._client = await self._client_cm.__aenter__()
        return self._client

    async def close(self) -> None:
        if self._client_cm is not None:
            await self._client_cm.__aexit__(None, None, None)
            self._client = None
            self._client_cm = None

    # ------------------------------------------------------------------
    # StorageBackend implementation
    # ------------------------------------------------------------------

    async def upload(self, key: str, data: bytes, content_type: str | None = None) -> str:
        client = await self._ensure()
        extra = {"ContentType": content_type} if content_type else {}
        try:
            await client.put_object(Bucket=self.bucket, Key=key, Body=data, **extra)
        except ClientError as e:
            self._raise(e, key)
        return key

    async def download(self, key: str) -> bytes:
        client = await self._ensure()
        try:
            response = await client.get_object(Bucket=self.bucket, Key=key)
            async with response["Body"] as stream:
                return await stream.read()
        except ClientError as e:
            self._raise(e, key)

    async def delete(self, key: str) -> None:
        client = await self._ensure()
        try:
            await client.delete_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            self._raise(e, key)

    async def exists(self, key: str) -> bool:
        client = await self._ensure()
        try:
            await client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            self._raise(e, key)

    async def list(self, prefix: str = "") -> list[str]:
        client = await self._ensure()
        try:
            keys: list[str] = []
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
            return keys
        except ClientError as e:
            self._raise(e, prefix)

    async def presigned_url(self, key: str, expires_in: int = 3600) -> str:
        client = await self._ensure()
        try:
            return await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expires_in,
            )
        except ClientError as e:
            self._raise(e, key)

    async def copy(self, src_key: str, dst_key: str) -> str:
        client = await self._ensure()
        try:
            await client.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": src_key},
                Key=dst_key,
            )
        except ClientError as e:
            self._raise(e, src_key)
        return dst_key

    async def get_metadata(self, key: str) -> dict:
        client = await self._ensure()
        try:
            response = await client.head_object(Bucket=self.bucket, Key=key)
            return {
                "content_type": response.get("ContentType"),
                "size": response.get("ContentLength"),
                "last_modified": response.get("LastModified"),
                "etag": response.get("ETag"),
                "metadata": response.get("Metadata", {}),
            }
        except ClientError as e:
            self._raise(e, key)

    async def upload_stream(self, key, stream, content_type=None):
        """Upload from an async byte stream by accumulating chunks."""
        chunks = []
        async for chunk in stream:
            chunks.append(chunk)
        data = b"".join(chunks)
        return await self.upload(key, data, content_type=content_type)

    async def list_iter(self, prefix: str = ""):
        """Yield object keys lazily using S3 paginator (true pagination)."""
        client = await self._ensure()
        try:
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    yield obj["Key"]
        except ClientError as e:
            self._raise(e, prefix)

    def _raise(self, exc: ClientError, key: str):
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            raise ObjectNotFoundError(f"Object not found: {key}") from exc
        if code in ("403", "AccessDenied"):
            raise StoragePermissionError(f"Access denied for key: {key}") from exc
        raise StorageError(str(exc)) from exc
