"""Document database connection factory.

Returns a configured :class:`motor.motor_asyncio.AsyncIOMotorClient` regardless
of provider — both AWS DocumentDB and Azure Cosmos DB (MongoDB API) speak the
MongoDB wire protocol, so the caller uses Motor's native async API directly.

    from cloudrift.document import get_mongodb

    client = get_mongodb("documentdb", uri="mongodb://...")
    await client["mydb"]["users"].insert_one({"name": "Alice"})
    client.close()
"""
from motor.motor_asyncio import AsyncIOMotorClient


def get_mongodb(provider: str, **kwargs) -> AsyncIOMotorClient:
    """Factory to build an async MongoDB client for the given provider.

    Args:
        provider: ``"documentdb"`` or ``"cosmos"``.
        **kwargs: Provider-specific config. Routed to the appropriate
            ``connect_*`` function based on which keys are present.

    Examples:
        get_mongodb("documentdb", uri="mongodb://...")
        get_mongodb("documentdb", host="...", port=27017,
                    username="u", password="p")
        get_mongodb("documentdb", host="...", port=27017,
                    username="u", password="p",
                    tls_cert_key_file="/path/to/client.pem")
        get_mongodb("cosmos", connection_string="mongodb://...")
        get_mongodb("cosmos", account="myacct", account_key="...")
    """
    if provider == "documentdb":
        from cloudrift.document import documentdb

        if "uri" in kwargs:
            return documentdb.connect_uri(**kwargs)
        if "tls_cert_key_file" in kwargs:
            return documentdb.connect_tls_cert(**kwargs)
        return documentdb.connect_credentials(**kwargs)

    if provider == "cosmos":
        from cloudrift.document import cosmos

        if "connection_string" in kwargs:
            return cosmos.connect_connection_string(**kwargs)
        return cosmos.connect_account_key(**kwargs)

    raise ValueError(
        f"Unknown document DB provider: {provider!r}. Choose 'documentdb' or 'cosmos'."
    )


__all__ = ["get_mongodb"]
