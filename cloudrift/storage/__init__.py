from cloudrift.storage.base import StorageBackend


def get_storage(provider: str, **kwargs) -> StorageBackend:
    """Factory to instantiate a single-bucket storage backend.

    The returned backend owns its underlying SDK client — ``await backend.close()``
    tears it down. Use :func:`get_storage_client` when you need to share one
    connection pool across multiple buckets/containers.

    Args:
        provider: ``"s3"`` or ``"azure_blob"``.
        **kwargs: Provider-specific config. The factory routes to the appropriate
            ``from_*`` classmethod based on which credential keys are present.

    Returns:
        A StorageBackend instance bound to the given bucket/container.

    Examples:
        get_storage("s3", bucket="my-bucket", region="us-east-1")  # IAM/env
        get_storage("s3", bucket="b", aws_access_key_id="AKIA...", aws_secret_access_key="...")
        get_storage("s3", bucket="b", profile_name="dev")
        get_storage("azure_blob", connection_string="...", container="my-container")
        get_storage("azure_blob", account_url="...", account_key="...", container="c")
        get_storage("azure_blob", account_url="...", sas_token="...", container="c")
    """
    if provider == "s3":
        from cloudrift.storage.s3 import AWSS3Backend

        if "aws_access_key_id" in kwargs:
            return AWSS3Backend.from_access_key(**kwargs)
        if "profile_name" in kwargs:
            return AWSS3Backend.from_profile(**kwargs)
        return AWSS3Backend.from_iam_role(**kwargs)

    if provider == "azure_blob":
        from cloudrift.storage.azure_blob import AzureBlobBackend

        if "connection_string" in kwargs:
            return AzureBlobBackend.from_connection_string(**kwargs)
        if "account_key" in kwargs:
            return AzureBlobBackend.from_account_key(**kwargs)
        if "sas_token" in kwargs:
            return AzureBlobBackend.from_sas_token(**kwargs)
        if "client_secret" in kwargs:
            return AzureBlobBackend.from_service_principal(**kwargs)
        return AzureBlobBackend.from_managed_identity(**kwargs)

    raise ValueError(f"Unknown storage provider: {provider!r}. Choose 's3' or 'azure_blob'.")


def get_storage_client(provider: str, **kwargs):
    """Factory to instantiate an account-scoped storage client.

    Returns an :class:`AWSS3Client` or :class:`AzureBlobClient` that can serve
    multiple buckets/containers from a single connection pool. Get a
    :class:`StorageBackend` view via ``client.bucket(name)`` (S3) or
    ``client.container(name)`` (Azure).

    Args:
        provider: ``"s3"`` or ``"azure_blob"``.
        **kwargs: Same auth-method routing as :func:`get_storage`, but without
            ``bucket`` / ``container``.

    Examples:
        client = get_storage_client("s3", region="us-east-1")
        logs = client.bucket("logs")
        uploads = client.bucket("uploads")
        # ... use them ...
        await client.close()  # tears down the single shared client
    """
    if provider == "s3":
        from cloudrift.storage.s3 import AWSS3Client

        if "aws_access_key_id" in kwargs:
            return AWSS3Client.from_access_key(**kwargs)
        if "profile_name" in kwargs:
            return AWSS3Client.from_profile(**kwargs)
        return AWSS3Client.from_iam_role(**kwargs)

    if provider == "azure_blob":
        from cloudrift.storage.azure_blob import AzureBlobClient

        if "connection_string" in kwargs:
            return AzureBlobClient.from_connection_string(**kwargs)
        if "account_key" in kwargs:
            return AzureBlobClient.from_account_key(**kwargs)
        if "sas_token" in kwargs:
            return AzureBlobClient.from_sas_token(**kwargs)
        if "client_secret" in kwargs:
            return AzureBlobClient.from_service_principal(**kwargs)
        return AzureBlobClient.from_managed_identity(**kwargs)

    raise ValueError(f"Unknown storage provider: {provider!r}. Choose 's3' or 'azure_blob'.")


__all__ = ["StorageBackend", "get_storage", "get_storage_client"]
