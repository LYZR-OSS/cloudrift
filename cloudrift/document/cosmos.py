from azure.cosmos import PartitionKey
from azure.cosmos.aio import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError, CosmosHttpResponseError

from cloudrift.core.exceptions import DocumentConnectionError, DocumentError, DocumentNotFoundError
from cloudrift.document.base import DocumentBackend


class AzureCosmosDBBackend(DocumentBackend):
    """Azure Cosmos DB backend (Core/SQL API).

    Use one of the class methods to construct:
    - ``from_connection_string``  — Cosmos DB connection string
    - ``from_account_key``        — account URL + account key
    - ``from_managed_identity``   — Azure Managed Identity (system or user-assigned)
    - ``from_service_principal``  — Azure AD service principal (client secret)
    """

    def __init__(
        self,
        client: CosmosClient,
        database: str,
        partition_key: str = "/id",
        credential=None,
    ) -> None:
        self._client = client
        self._database_name = database
        self._partition_key = partition_key
        self._credential = credential
        self._db = client.get_database_client(database)
        self._containers: dict[str, object] = {}

    async def close(self) -> None:
        await self._client.close()
        if self._credential is not None and hasattr(self._credential, "close"):
            await self._credential.close()

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_connection_string(
        cls,
        connection_string: str,
        database: str,
        partition_key: str = "/id",
    ) -> "AzureCosmosDBBackend":
        """Authenticate with a Cosmos DB connection string."""
        try:
            return cls(CosmosClient.from_connection_string(connection_string), database, partition_key)
        except Exception as e:
            raise DocumentConnectionError(f"Failed to initialize Cosmos DB client: {e}") from e

    @classmethod
    def from_account_key(
        cls,
        url: str,
        account_key: str,
        database: str,
        partition_key: str = "/id",
    ) -> "AzureCosmosDBBackend":
        """Authenticate with a Cosmos DB account URL and account key.

        Args:
            url: e.g. ``https://<account>.documents.azure.com:443/``
            account_key: Primary or secondary account key.
            database: Database name.
            partition_key: Partition key path (default ``/id``).
        """
        try:
            return cls(CosmosClient(url, credential=account_key), database, partition_key)
        except Exception as e:
            raise DocumentConnectionError(f"Failed to initialize Cosmos DB client: {e}") from e

    @classmethod
    def from_managed_identity(
        cls,
        url: str,
        database: str,
        partition_key: str = "/id",
        client_id: str | None = None,
    ) -> "AzureCosmosDBBackend":
        """Authenticate via Azure Managed Identity.

        Args:
            url: e.g. ``https://<account>.documents.azure.com:443/``
            database: Database name.
            partition_key: Partition key path (default ``/id``).
            client_id: Optional client ID for a user-assigned managed identity.
                       Omit to use the system-assigned identity.
        """
        try:
            from azure.identity.aio import ManagedIdentityCredential
            credential = ManagedIdentityCredential(client_id=client_id) if client_id else ManagedIdentityCredential()
            return cls(
                CosmosClient(url, credential=credential),
                database,
                partition_key,
                credential=credential,
            )
        except Exception as e:
            raise DocumentConnectionError(f"Failed to initialize Cosmos DB client: {e}") from e

    @classmethod
    def from_service_principal(
        cls,
        url: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        database: str,
        partition_key: str = "/id",
    ) -> "AzureCosmosDBBackend":
        """Authenticate via Azure AD service principal (client secret).

        Args:
            url: e.g. ``https://<account>.documents.azure.com:443/``
            tenant_id: Azure AD tenant ID.
            client_id: Service principal application (client) ID.
            client_secret: Service principal client secret.
            database: Database name.
            partition_key: Partition key path (default ``/id``).
        """
        try:
            from azure.identity.aio import ClientSecretCredential
            credential = ClientSecretCredential(
                tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
            )
            return cls(
                CosmosClient(url, credential=credential),
                database,
                partition_key,
                credential=credential,
            )
        except Exception as e:
            raise DocumentConnectionError(f"Failed to initialize Cosmos DB client: {e}") from e

    # ------------------------------------------------------------------
    # DocumentBackend implementation
    # ------------------------------------------------------------------

    def _get_container(self, collection: str):
        cached = self._containers.get(collection)
        if cached is None:
            cached = self._db.get_container_client(collection)
            self._containers[collection] = cached
        return cached

    async def insert_one(self, collection: str, document: dict) -> str:
        try:
            container = self._get_container(collection)
            result = await container.create_item(body=document)
            return result["id"]
        except CosmosHttpResponseError as e:
            raise DocumentError(str(e)) from e

    async def insert_many(self, collection: str, documents: list[dict]) -> list[str]:
        ids = []
        for doc in documents:
            ids.append(await self.insert_one(collection, doc))
        return ids

    async def find_one(self, collection: str, query: dict) -> dict | None:
        results = await self.find(collection, query, limit=1)
        return results[0] if results else None

    async def find(
        self, collection: str, query: dict, limit: int = 100, skip: int = 0
    ) -> list[dict]:
        try:
            container = self._get_container(collection)
            where_clause, params = _build_where(query)
            sql = f"SELECT * FROM c{where_clause} OFFSET {skip} LIMIT {limit}"
            items = []
            async for item in container.query_items(query=sql, parameters=params):
                items.append(item)
            return items
        except CosmosHttpResponseError as e:
            raise DocumentError(str(e)) from e

    async def update_one(self, collection: str, query: dict, update: dict) -> int:
        doc = await self.find_one(collection, query)
        if doc is None:
            return 0
        try:
            container = self._get_container(collection)
            merged = {**doc, **update.get("$set", update)}
            await container.replace_item(item=doc["id"], body=merged)
            return 1
        except CosmosResourceNotFoundError:
            return 0
        except CosmosHttpResponseError as e:
            raise DocumentError(str(e)) from e

    async def update_many(self, collection: str, query: dict, update: dict) -> int:
        docs = await self.find(collection, query, limit=1000)
        count = 0
        for doc in docs:
            count += await self.update_one(collection, {"id": doc["id"]}, update)
        return count

    async def delete_one(self, collection: str, query: dict) -> int:
        doc = await self.find_one(collection, query)
        if doc is None:
            return 0
        try:
            container = self._get_container(collection)
            await container.delete_item(
                item=doc["id"],
                partition_key=doc.get(self._partition_key.lstrip("/"), doc["id"]),
            )
            return 1
        except CosmosResourceNotFoundError:
            return 0
        except CosmosHttpResponseError as e:
            raise DocumentError(str(e)) from e

    async def delete_many(self, collection: str, query: dict) -> int:
        docs = await self.find(collection, query, limit=1000)
        count = 0
        for doc in docs:
            count += await self.delete_one(collection, {"id": doc["id"]})
        return count

    async def count(self, collection: str, query: dict | None = None) -> int:
        try:
            container = self._get_container(collection)
            where_clause, params = _build_where(query or {})
            sql = f"SELECT VALUE COUNT(1) FROM c{where_clause}"
            async for result in container.query_items(query=sql, parameters=params):
                return result
            return 0
        except CosmosHttpResponseError as e:
            raise DocumentError(str(e)) from e

    async def create_index(
        self, collection: str, keys: list[tuple[str, int]], unique: bool = False
    ) -> str:
        if unique:
            raise NotImplementedError(
                "Cosmos DB unique key policies must be defined at container creation time "
                "and cannot be added dynamically."
            )
        try:
            container = self._get_container(collection)
            props = await container.read()
            policy = props.get("indexingPolicy", {
                "indexingMode": "consistent",
                "includedPaths": [],
                "excludedPaths": [{"path": '/"_etag"/?'}],
            })
            existing = {p["path"] for p in policy.get("includedPaths", [])}
            name_parts: list[str] = []
            for field, direction in keys:
                entry_path = "/" + field.lstrip("/") + "/?"
                if entry_path not in existing:
                    policy.setdefault("includedPaths", []).append({"path": entry_path})
                name_parts.append(f"{field}_{direction}")
            await self._db.replace_container(
                container=collection,
                partition_key=PartitionKey(path=self._partition_key),
                indexing_policy=policy,
            )
            return "_".join(name_parts)
        except CosmosHttpResponseError as e:
            raise DocumentError(str(e)) from e

    async def aggregate(self, collection: str, pipeline: list[dict]) -> list[dict]:
        """Aggregation support: translates $match, $count, $sort, $project, $limit, $skip to SQL."""
        try:
            container = self._get_container(collection)
            select_clause = "SELECT * FROM c"
            where_clause = ""
            params: list[dict] = []
            order_by = ""
            limit_val: int | None = None
            skip_val: int = 0

            for stage in pipeline:
                if "$match" in stage:
                    where_clause, params = _build_where(stage["$match"])
                elif "$count" in stage:
                    sql = f"SELECT VALUE COUNT(1) FROM c{where_clause}"
                    results = []
                    async for result in container.query_items(query=sql, parameters=params):
                        results.append({stage["$count"]: result})
                    return results
                elif "$sort" in stage:
                    parts = [
                        f"c.{f} {'ASC' if d == 1 else 'DESC'}"
                        for f, d in stage["$sort"].items()
                    ]
                    order_by = " ORDER BY " + ", ".join(parts)
                elif "$project" in stage:
                    fields = [
                        f"c.{f}"
                        for f, inc in stage["$project"].items()
                        if inc and f != "_id"
                    ]
                    if fields:
                        select_clause = "SELECT " + ", ".join(fields) + " FROM c"
                elif "$limit" in stage:
                    limit_val = stage["$limit"]
                elif "$skip" in stage:
                    skip_val = stage["$skip"]
                else:
                    supported = "$match, $count, $sort, $project, $limit, $skip"
                    raise NotImplementedError(
                        f"Cosmos DB aggregate supports: {supported}. "
                        f"Unsupported stage: {list(stage.keys())}"
                    )

            sql = f"{select_clause}{where_clause}{order_by}"
            # Cosmos SQL requires LIMIT when OFFSET is used
            if limit_val is not None or skip_val:
                sql += f" OFFSET {skip_val} LIMIT {limit_val if limit_val is not None else 999999}"

            items = []
            async for item in container.query_items(query=sql, parameters=params):
                items.append(item)
            return items
        except CosmosHttpResponseError as e:
            raise DocumentError(str(e)) from e

    async def upsert_one(self, collection: str, query: dict, update: dict) -> str:
        try:
            container = self._get_container(collection)
            # Find existing doc and merge, or create new
            existing = await self.find_one(collection, query)
            if existing:
                merged = {**existing, **update.get("$set", update)}
                result = await container.upsert_item(body=merged)
            else:
                body = {**query, **update.get("$set", update)}
                result = await container.upsert_item(body=body)
            return result["id"]
        except CosmosHttpResponseError as e:
            raise DocumentError(str(e)) from e

    async def find_iter(self, collection, query):
        """Yield documents lazily using Cosmos query iterator (true streaming)."""
        try:
            container = self._get_container(collection)
            where_clause, params = _build_where(query)
            sql = f"SELECT * FROM c{where_clause}"
            async for item in container.query_items(query=sql, parameters=params):
                yield item
        except CosmosHttpResponseError as e:
            raise DocumentError(str(e)) from e

    async def health_check(self) -> bool:
        try:
            async for _ in self._client.list_databases(max_item_count=1):
                break
            return True
        except Exception:
            return False


def _build_where(query: dict) -> tuple[str, list[dict]]:
    """Build a simple Cosmos DB SQL WHERE clause from a dict query."""
    if not query:
        return "", []
    conditions = []
    params = []
    for i, (key, value) in enumerate(query.items()):
        param_name = f"@p{i}"
        conditions.append(f"c.{key} = {param_name}")
        params.append({"name": param_name, "value": value})
    return " WHERE " + " AND ".join(conditions), params
