from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator


class DocumentBackend(ABC):
    """Abstract base class for cloud document database backends.

    Backends hold long-lived async clients. Use ``await backend.close()`` (or
    ``async with backend:``) to release sockets cleanly.
    """

    @abstractmethod
    async def insert_one(self, collection: str, document: dict) -> str:
        """Insert a single document. Returns the inserted document ID."""

    @abstractmethod
    async def insert_many(self, collection: str, documents: list[dict]) -> list[str]:
        """Insert multiple documents. Returns list of inserted IDs."""

    @abstractmethod
    async def find_one(self, collection: str, query: dict) -> dict | None:
        """Find a single document matching the query. Returns None if not found."""

    @abstractmethod
    async def find(
        self, collection: str, query: dict, limit: int = 100, skip: int = 0
    ) -> list[dict]:
        """Find documents matching the query with optional pagination."""

    @abstractmethod
    async def update_one(self, collection: str, query: dict, update: dict) -> int:
        """Update the first document matching the query. Returns modified count."""

    @abstractmethod
    async def update_many(self, collection: str, query: dict, update: dict) -> int:
        """Update all documents matching the query. Returns modified count."""

    @abstractmethod
    async def delete_one(self, collection: str, query: dict) -> int:
        """Delete the first document matching the query. Returns deleted count."""

    @abstractmethod
    async def delete_many(self, collection: str, query: dict) -> int:
        """Delete all documents matching the query. Returns deleted count."""

    @abstractmethod
    async def count(self, collection: str, query: dict | None = None) -> int:
        """Count documents matching the query."""

    @abstractmethod
    async def create_index(
        self, collection: str, keys: list[tuple[str, int]], unique: bool = False
    ) -> str:
        """Create an index on the collection. Returns the index name."""

    @abstractmethod
    async def aggregate(self, collection: str, pipeline: list[dict]) -> list[dict]:
        """Run an aggregation pipeline. Returns the result documents."""

    @abstractmethod
    async def upsert_one(self, collection: str, query: dict, update: dict) -> str:
        """Insert or update a document. Returns the document ID."""

    async def find_iter(
        self, collection: str, query: dict
    ) -> AsyncGenerator[dict, None]:
        """Yield documents lazily. Override for cursor-based iteration."""
        for doc in await self.find(collection, query, limit=0):
            yield doc

    async def health_check(self) -> bool:
        """Return True if the document backend is reachable."""
        try:
            await self.count("__cloudrift_health__", {})
            return True
        except Exception:
            return False

    async def close(self) -> None:
        """Close the underlying client and release sockets. Default is a no-op."""

    async def __aenter__(self) -> "DocumentBackend":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()
