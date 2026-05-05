import asyncio

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from cloudrift.core.exceptions import ObjectNotFoundError, StorageError, StoragePermissionError
from cloudrift.storage.base import StorageBackend


class AWSS3Client:
    """Account-scoped AWS S3 client.

    Owns one ``aioboto3`` session and one async S3 client (lazily created on
    first use). The same client serves every bucket in the account, so
    callers using multiple buckets share a single connection pool.

    Use ``client.bucket(name)`` to get a per-bucket :class:`StorageBackend`
    handle. Call ``await client.close()`` (or ``async with client:``) once
    when you're done — that tears down the SDK client for every view that
    was issued from it.

    Use one of the class methods to construct:
    - ``from_access_key`` — static credentials (+ optional session token for assumed roles)
    - ``from_iam_role``   — instance profile / environment / ECS task role
    - ``from_profile``    — named profile from ``~/.aws/credentials``
    """

    def __init__(
        self,
        session: aioboto3.Session,
        *,
        endpoint_url: str | None = None,
        max_pool_connections: int = 50,
        connect_timeout: float = 10.0,
        read_timeout: float = 60.0,
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
    ) -> "AWSS3Client":
        """Authenticate with explicit access key / secret (+ optional STS session token)."""
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
    ) -> "AWSS3Client":
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
    ) -> "AWSS3Client":
        """Authenticate using a named profile from ``~/.aws/credentials``."""
        session = aioboto3.Session(profile_name=profile_name, region_name=region)
        return cls(session, endpoint_url=endpoint_url, **kwargs)

    # ------------------------------------------------------------------
    # Bucket view factory
    # ------------------------------------------------------------------

    def bucket(self, name: str) -> "AWSS3Backend":
        """Return a :class:`StorageBackend` view bound to ``name``.

        The view shares this client's connection pool. ``await view.close()``
        is a no-op — call ``await client.close()`` to release sockets.
        """
        return AWSS3Backend(name, self)

    # ------------------------------------------------------------------
    # Lifecycle
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

    async def __aenter__(self) -> "AWSS3Client":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class AWSS3Backend(StorageBackend):
    """Per-bucket :class:`StorageBackend` view over an :class:`AWSS3Client`.

    Holds only ``(client, bucket)`` — all I/O delegates to the shared
    client. ``close()`` is a no-op for views obtained from
    ``client.bucket(...)``; the account client owns the socket lifecycle.

    Views obtained from :func:`cloudrift.storage.get_storage` own their
    underlying client and *do* tear it down on ``close()`` (preserves the
    historical single-bucket contract).
    """

    def __init__(
        self,
        bucket: str,
        client: AWSS3Client,
        *,
        owns_client: bool = False,
    ) -> None:
        self.bucket = bucket
        self._client = client
        self._owns_client = owns_client

    # ------------------------------------------------------------------
    # Backwards-compatible factory constructors
    # ------------------------------------------------------------------
    # These build a one-shot client that the returned view owns. Prefer
    # ``AWSS3Client.from_*`` + ``client.bucket(...)`` when you want to
    # share a connection pool across buckets.

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
        client = AWSS3Client.from_access_key(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region=region,
            aws_session_token=aws_session_token,
            endpoint_url=endpoint_url,
            **kwargs,
        )
        return cls(bucket, client, owns_client=True)

    @classmethod
    def from_iam_role(
        cls,
        bucket: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSS3Backend":
        client = AWSS3Client.from_iam_role(region=region, endpoint_url=endpoint_url, **kwargs)
        return cls(bucket, client, owns_client=True)

    @classmethod
    def from_profile(
        cls,
        bucket: str,
        profile_name: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSS3Backend":
        client = AWSS3Client.from_profile(
            profile_name=profile_name, region=region, endpoint_url=endpoint_url, **kwargs
        )
        return cls(bucket, client, owns_client=True)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        if self._owns_client:
            await self._client.close()

    # ------------------------------------------------------------------
    # StorageBackend implementation
    # ------------------------------------------------------------------

    async def upload(self, key: str, data: bytes, content_type: str | None = None) -> str:
        client = await self._client._ensure()
        extra = {"ContentType": content_type} if content_type else {}
        try:
            await client.put_object(Bucket=self.bucket, Key=key, Body=data, **extra)
        except ClientError as e:
            self._raise(e, key)
        return key

    async def download(self, key: str) -> bytes:
        client = await self._client._ensure()
        try:
            response = await client.get_object(Bucket=self.bucket, Key=key)
            async with response["Body"] as stream:
                return await stream.read()
        except ClientError as e:
            self._raise(e, key)

    async def delete(self, key: str) -> None:
        client = await self._client._ensure()
        try:
            await client.delete_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            self._raise(e, key)

    async def exists(self, key: str) -> bool:
        client = await self._client._ensure()
        try:
            await client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            self._raise(e, key)

    async def list(self, prefix: str = "") -> list[str]:
        client = await self._client._ensure()
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
        client = await self._client._ensure()
        try:
            return await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expires_in,
            )
        except ClientError as e:
            self._raise(e, key)

    async def copy(self, src_key: str, dst_key: str, *, dst_bucket: str | None = None) -> str:
        client = await self._client._ensure()
        target_bucket = dst_bucket or self.bucket
        try:
            await client.copy_object(
                Bucket=target_bucket,
                CopySource={"Bucket": self.bucket, "Key": src_key},
                Key=dst_key,
            )
        except ClientError as e:
            self._raise(e, src_key)
        return dst_key

    async def get_metadata(self, key: str) -> dict:
        client = await self._client._ensure()
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
        client = await self._client._ensure()
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
