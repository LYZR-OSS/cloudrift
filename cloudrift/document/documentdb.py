from urllib.parse import quote_plus

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient

from cloudrift.core.exceptions import DocumentConnectionError, DocumentError, DocumentNotFoundError
from cloudrift.document.base import DocumentBackend


class AWSDocumentDBBackend(DocumentBackend):
    """AWS DocumentDB backend (MongoDB-compatible).

    Use one of the class methods to construct:
    - ``from_uri``         — full MongoDB URI (most flexible)
    - ``from_credentials`` — host/port + username/password (URI built internally)
    - ``from_tls_cert``    — mTLS with a PEM client certificate file
    """

    def __init__(self, client: AsyncIOMotorClient, database: str) -> None:
        self._client = client
        self._db = client[database]

    async def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_uri(
        cls,
        uri: str,
        database: str,
        tls_ca_file: str | None = None,
        max_pool_size: int = 100,
        min_pool_size: int = 0,
        **client_kwargs,
    ) -> "AWSDocumentDBBackend":
        """Connect using a full MongoDB-compatible URI."""
        try:
            kwargs: dict = {"maxPoolSize": max_pool_size, "minPoolSize": min_pool_size}
            if tls_ca_file:
                kwargs["tlsCAFile"] = tls_ca_file
            kwargs.update(client_kwargs)
            return cls(AsyncIOMotorClient(uri, **kwargs), database)
        except Exception as e:
            raise DocumentConnectionError(f"Failed to connect to DocumentDB: {e}") from e

    @classmethod
    def from_credentials(
        cls,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        tls: bool = True,
        tls_ca_file: str | None = None,
    ) -> "AWSDocumentDBBackend":
        """Connect using explicit host, port, username, and password.

        Args:
            host: DocumentDB cluster endpoint hostname.
            port: Port number (DocumentDB default: 27017).
            username: Database username.
            password: Database password.
            database: Database name.
            tls: Enable TLS (default ``True``; required for AWS DocumentDB).
            tls_ca_file: Optional path to the CA certificate bundle (PEM).
        """
        uri = f"mongodb://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/"
        try:
            kwargs: dict = {"tls": tls, "maxPoolSize": 100}
            if tls_ca_file:
                kwargs["tlsCAFile"] = tls_ca_file
            return cls(AsyncIOMotorClient(uri, **kwargs), database)
        except Exception as e:
            raise DocumentConnectionError(f"Failed to connect to DocumentDB: {e}") from e

    @classmethod
    def from_tls_cert(
        cls,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        tls_cert_key_file: str,
        tls_ca_file: str | None = None,
    ) -> "AWSDocumentDBBackend":
        """Connect using mutual TLS (mTLS) with a client certificate.

        Args:
            host: DocumentDB cluster endpoint hostname.
            port: Port number (DocumentDB default: 27017).
            username: Database username.
            password: Database password.
            database: Database name.
            tls_cert_key_file: Path to a PEM file containing the client private key
                followed by the client certificate (combined, as required by pymongo/Motor).
            tls_ca_file: Optional path to the CA certificate bundle (PEM).
        """
        uri = f"mongodb://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/"
        try:
            kwargs: dict = {
                "tls": True,
                "tlsCertificateKeyFile": tls_cert_key_file,
                "maxPoolSize": 100,
            }
            if tls_ca_file:
                kwargs["tlsCAFile"] = tls_ca_file
            return cls(AsyncIOMotorClient(uri, **kwargs), database)
        except Exception as e:
            raise DocumentConnectionError(f"Failed to connect to DocumentDB: {e}") from e

    # ------------------------------------------------------------------
    # DocumentBackend implementation
    # ------------------------------------------------------------------

    async def insert_one(self, collection: str, document: dict) -> str:
        try:
            result = await self._db[collection].insert_one(document)
            return str(result.inserted_id)
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def insert_many(self, collection: str, documents: list[dict]) -> list[str]:
        try:
            result = await self._db[collection].insert_many(documents)
            return [str(i) for i in result.inserted_ids]
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def find_one(self, collection: str, query: dict) -> dict | None:
        try:
            doc = await self._db[collection].find_one(_prepare_query(query))
            return _serialize(doc)
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def find(
        self, collection: str, query: dict, limit: int = 100, skip: int = 0
    ) -> list[dict]:
        try:
            cursor = self._db[collection].find(_prepare_query(query)).skip(skip).limit(limit)
            return [_serialize(doc) async for doc in cursor]
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def update_one(self, collection: str, query: dict, update: dict) -> int:
        try:
            result = await self._db[collection].update_one(_prepare_query(query), update)
            return result.modified_count
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def update_many(self, collection: str, query: dict, update: dict) -> int:
        try:
            result = await self._db[collection].update_many(_prepare_query(query), update)
            return result.modified_count
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def delete_one(self, collection: str, query: dict) -> int:
        try:
            result = await self._db[collection].delete_one(_prepare_query(query))
            return result.deleted_count
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def delete_many(self, collection: str, query: dict) -> int:
        try:
            result = await self._db[collection].delete_many(_prepare_query(query))
            return result.deleted_count
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def count(self, collection: str, query: dict | None = None) -> int:
        try:
            return await self._db[collection].count_documents(_prepare_query(query or {}))
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def create_index(
        self, collection: str, keys: list[tuple[str, int]], unique: bool = False
    ) -> str:
        try:
            return await self._db[collection].create_index(keys, unique=unique)
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def aggregate(self, collection: str, pipeline: list[dict]) -> list[dict]:
        try:
            cursor = self._db[collection].aggregate(pipeline)
            return [_serialize(doc) async for doc in cursor]
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def upsert_one(self, collection: str, query: dict, update: dict) -> str:
        try:
            result = await self._db[collection].update_one(
                _prepare_query(query), update, upsert=True
            )
            if result.upserted_id is not None:
                return str(result.upserted_id)
            doc = await self._db[collection].find_one(_prepare_query(query))
            return str(doc["_id"]) if doc else ""
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def find_iter(self, collection, query):
        """Yield documents lazily using Motor cursor (true streaming)."""
        try:
            cursor = self._db[collection].find(_prepare_query(query))
            async for doc in cursor:
                yield _serialize(doc)
        except Exception as e:
            raise DocumentError(str(e)) from e

    async def health_check(self) -> bool:
        try:
            await self._client.admin.command("ping")
            return True
        except Exception:
            return False


def _prepare_query(query: dict) -> dict:
    """Convert string ``_id`` to ObjectId for MongoDB queries."""
    if "_id" in query and isinstance(query["_id"], str):
        query = {**query, "_id": ObjectId(query["_id"])}
    return query


def _serialize(doc: dict | None) -> dict | None:
    """Convert ObjectId fields to strings for external use."""
    if doc is None:
        return None
    if "_id" in doc:
        doc = {**doc, "_id": str(doc["_id"])}
    return doc
