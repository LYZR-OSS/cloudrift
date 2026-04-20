from cloudrift.document.base import DocumentBackend


def get_mongodb(provider: str, **kwargs) -> DocumentBackend:
    """Factory to instantiate a document database backend.

    Args:
        provider: ``"documentdb"`` or ``"cosmos"``.
        **kwargs: Provider-specific config. Routed to the appropriate ``from_*``
            classmethod based on which credential keys are present.

    Examples:
        get_mongodb("documentdb", uri="mongodb://...", database="mydb")
        get_mongodb("documentdb", host="...", port=27017,
                    username="u", password="p", database="mydb")
        get_mongodb("cosmos", connection_string="...", database="mydb")
        get_mongodb("cosmos", url="...", account_key="...", database="mydb")
    """
    if provider == "documentdb":
        from cloudrift.document.documentdb import AWSDocumentDBBackend

        if "uri" in kwargs:
            return AWSDocumentDBBackend.from_uri(**kwargs)
        if "tls_cert_key_file" in kwargs:
            return AWSDocumentDBBackend.from_tls_cert(**kwargs)
        return AWSDocumentDBBackend.from_credentials(**kwargs)

    if provider == "cosmos":
        from cloudrift.document.cosmos import AzureCosmosDBBackend

        if "connection_string" in kwargs:
            return AzureCosmosDBBackend.from_connection_string(**kwargs)
        if "account_key" in kwargs:
            return AzureCosmosDBBackend.from_account_key(**kwargs)
        if "client_secret" in kwargs:
            return AzureCosmosDBBackend.from_service_principal(**kwargs)
        return AzureCosmosDBBackend.from_managed_identity(**kwargs)

    raise ValueError(
        f"Unknown document DB provider: {provider!r}. Choose 'documentdb' or 'cosmos'."
    )


__all__ = ["DocumentBackend", "get_mongodb"]
