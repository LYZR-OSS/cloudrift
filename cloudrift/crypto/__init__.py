from cloudrift.crypto.base import CryptoBackend


def get_crypto(provider: str, **kwargs) -> CryptoBackend:
    """Factory to instantiate a key-management crypto backend.

    The cross-cloud analog of AWS KMS: encrypt/decrypt small payloads against a
    managed key. AWS KMS ↔ Azure Key Vault keys ↔ GCP Cloud KMS.

    Args:
        provider: ``"aws_kms"``, ``"azure_keyvault"``, or ``"gcp_kms"``.
        **kwargs: Provider-specific config. The AWS and GCP factories route to
            the appropriate ``from_*`` classmethod based on which credential keys
            are present; the Azure factory routes by whether ``client_secret`` is
            set.

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
        get_crypto("gcp_kms",
                   key_id="projects/p/locations/us/keyRings/r/cryptoKeys/k")  # ADC
        get_crypto("gcp_kms", key_id="projects/p/...",
                   service_account_file="/etc/gcp/sa.json")
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

    if provider in ("gcp_kms", "cloud_kms"):
        from cloudrift.crypto.gcp_kms import GCPKMSBackend

        if "service_account_info" in kwargs:
            return GCPKMSBackend.from_service_account_info(**kwargs)
        if "service_account_file" in kwargs:
            return GCPKMSBackend.from_service_account_file(**kwargs)
        return GCPKMSBackend.from_application_default(**kwargs)

    raise ValueError(
        f"Unknown crypto provider: {provider!r}. Choose 'aws_kms', 'azure_keyvault', or 'gcp_kms'."
    )


__all__ = ["CryptoBackend", "get_crypto"]
