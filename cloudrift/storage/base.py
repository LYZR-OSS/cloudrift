from abc import ABC, abstractmethod
from collections.abc import AsyncIterator


class StorageBackend(ABC):
    """Abstract base class for cloud object storage backends.

    Backends hold long-lived async clients. Use ``await backend.close()`` (or
    ``async with backend:``) to release sockets cleanly.
    """

    @abstractmethod
    async def upload(self, key: str, data: bytes, content_type: str | None = None) -> str:
        """Upload bytes to storage. Returns the object key."""

    @abstractmethod
    async def download(self, key: str) -> bytes:
        """Download an object by key. Returns raw bytes."""

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete an object by key."""

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Return True if the object exists."""

    @abstractmethod
    async def list(self, prefix: str = "") -> list[str]:
        """List object keys, optionally filtered by prefix."""

    @abstractmethod
    async def presigned_url(self, key: str, expires_in: int = 3600) -> str:
        """Generate a presigned URL for the object. expires_in is in seconds."""

    @abstractmethod
    async def copy(self, src_key: str, dst_key: str, *, dst_bucket: str | None = None) -> str:
        """Copy an object. Defaults to same-bucket copy.

        If ``dst_bucket`` is provided, copies across buckets/containers within
        the same storage account. Returns the destination key.
        """

    @abstractmethod
    async def get_metadata(self, key: str) -> dict:
        """Return object metadata (content_type, size, last_modified, etag, etc.)."""

    @abstractmethod
    async def upload_stream(
        self, key: str, stream: AsyncIterator[bytes], content_type: str | None = None
    ) -> str:
        """Upload from an async byte stream. Returns the object key."""

    async def move(self, src_key: str, dst_key: str, *, dst_bucket: str | None = None) -> str:
        """Move an object (copy + delete on the source). Returns the destination key."""
        await self.copy(src_key, dst_key, dst_bucket=dst_bucket)
        await self.delete(src_key)
        return dst_key

    async def list_iter(self, prefix: str = "") -> AsyncIterator[str]:
        """Yield object keys lazily. Override for true pagination."""
        for key in await self.list(prefix):
            yield key

    async def health_check(self) -> bool:
        """Return True if the storage backend is reachable."""
        try:
            await self.exists("__cloudrift_health__")
            return True
        except Exception:
            return False

    async def close(self) -> None:
        """Close the underlying client and release sockets. Default is a no-op."""

    async def __aenter__(self) -> "StorageBackend":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()
