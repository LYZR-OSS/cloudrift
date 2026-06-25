from cloudrift.crypto.base import CryptoBackend


def get_crypto(provider: str, **kwargs) -> CryptoBackend:
    """Factory to instantiate a key-management crypto backend.

    The cross-cloud analog of AWS KMS: encrypt/decrypt small payloads against a
    managed key. AWS KMS ↔ Azure Key Vault keys.

    Args:
        provider: ``"aws_kms"`` or ``"azure_keyvault"``.
        **kwargs: Provider-specific config. The AWS factory routes to the
            appropriate ``from_*`` classmethod based on which credential keys are
            present; the Azure factory routes by whether ``client_secret`` is set.

    Returns:
        A :class:`CryptoBackend` instance.

    Examples:
        get_crypto("aws_kms", key_id="arn:aws:kms:...:key/abc", region="us-east-1")  # IAM/env
        get_crypto("aws_kms", key_id="alias/my-key", aws_access_key_id="AKIA...",
                   aws_secret_access_key="...", region="us-east-1")
        get_crypto("azure_keyvault",
                   key_id="https://myvault.vault.azure.net/keys/mykey",
                   tenant_id="...", client_id="...", client_secret="...")
        get_crypto("azure_keyvault",
                   key_id="https://myvault.vault.azure.net/keys/mykey")  # managed identity
    """
    if provider in ("aws_kms", "kms"):
        from cloudrift.crypto.aws_kms import AWSKMSBackend

        if "aws_access_key_id" in kwargs:
            return AWSKMSBackend.from_access_key(**kwargs)
        if "profile_name" in kwargs:
            return AWSKMSBackend.from_profile(**kwargs)
        return AWSKMSBackend.from_iam_role(**kwargs)

    if provider in ("azure_keyvault", "azure_keyvault_keys"):
        from cloudrift.crypto.azure_keyvault_keys import AzureKeyVaultKeysBackend

        if "client_secret" in kwargs:
            return AzureKeyVaultKeysBackend.from_service_principal(**kwargs)
        return AzureKeyVaultKeysBackend.from_managed_identity(**kwargs)

    raise ValueError(
        f"Unknown crypto provider: {provider!r}. Choose 'aws_kms' or 'azure_keyvault'."
    )


__all__ = ["CryptoBackend", "get_crypto"]
