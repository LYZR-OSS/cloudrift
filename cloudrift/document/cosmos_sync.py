"""Azure Cosmos DB (MongoDB API) connection factory (synchronous).

Returns a configured :class:`pymongo.MongoClient` — the blocking counterpart of
:mod:`cloudrift.document.cosmos` for services that don't run an event loop.
Identical in shape to :mod:`cloudrift.document.documentdb_sync`.

Only key-based auth is supported here: Cosmos for MongoDB (RU) does not accept
Azure AD tokens at the Mongo wire-protocol layer. Use a connection string from
the portal or build one from the account name + key.

Lifecycle is caller-managed: call ``client.close()`` at shutdown.
"""
from urllib.parse import quote_plus

from pymongo import MongoClient

from cloudrift.core.exceptions import DocumentConnectionError


def connect_connection_string(
    connection_string: str,
    *,
    max_pool_size: int = 100,
    min_pool_size: int = 0,
) -> MongoClient:
    """Connect using a Cosmos MongoDB-API connection string from the Azure portal.

    The portal exposes this under *Connection strings* on a Cosmos account
    configured for the MongoDB API.

    Args:
        connection_string: Mongo-format URI from the Cosmos portal.
        max_pool_size: Max connection pool size. Overrides any ``maxPoolSize``
            in the connection string.
        min_pool_size: Min connection pool size. Overrides any ``minPoolSize``
            in the connection string.
    """
    try:
        return MongoClient(
            connection_string,
            maxPoolSize=max_pool_size,
            minPoolSize=min_pool_size,
        )
    except Exception as e:
        raise DocumentConnectionError(f"Failed to connect to Cosmos DB: {e}") from e


def connect_account_key(
    account: str,
    account_key: str,
    *,
    port: int = 10255,
    app_name: str | None = None,
    max_pool_size: int = 100,
    min_pool_size: int = 0,
) -> MongoClient:
    """Build a Cosmos MongoDB-API URI from the account name and key.

    Args:
        account: Cosmos account name (the leftmost label of the host, i.e.
            ``<account>.mongo.cosmos.azure.com``).
        account_key: Primary or secondary account key.
        port: Mongo-API port (default ``10255``).
        app_name: Optional ``appName`` URI parameter (Cosmos uses it for
            telemetry and routing). Defaults to ``@<account>@``.
        max_pool_size: Max connection pool size.
        min_pool_size: Min connection pool size.
    """
    user = quote_plus(account)
    pwd = quote_plus(account_key)
    host = f"{account}.mongo.cosmos.azure.com"
    app = app_name if app_name is not None else f"@{account}@"
    query = (
        "ssl=true"
        "&replicaSet=globaldb"
        "&retryWrites=false"
        "&maxIdleTimeMS=120000"
        f"&appName={quote_plus(app)}"
    )
    uri = f"mongodb://{user}:{pwd}@{host}:{port}/?{query}"
    try:
        return MongoClient(
            uri,
            maxPoolSize=max_pool_size,
            minPoolSize=min_pool_size,
        )
    except Exception as e:
        raise DocumentConnectionError(f"Failed to connect to Cosmos DB: {e}") from e
