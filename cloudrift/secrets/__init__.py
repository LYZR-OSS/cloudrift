from cloudrift.secrets.base import SecretBackend


def get_secrets(provider: str, **kwargs) -> SecretBackend:
    """Factory to instantiate a secret management backend.

    Args:
        provider: ``"aws_secrets_manager"`` or ``"azure_keyvault"``.
        **kwargs: Provider-specific config. The factory routes to the appropriate
            ``from_*`` classmethod based on which credential keys are present.

    Returns:
        A SecretBackend instance.

    Examples:
        get_secrets("aws_secrets_manager", region="us-east-1")  # IAM/env
        get_secrets("aws_secrets_manager", aws_access_key_id="AKIA...",
                    aws_secret_access_key="...", region="us-east-1")
        get_secrets("azure_keyvault", vault_url="https://myvault.vault.azure.net")
        get_secrets("azure_keyvault", vault_url="...", tenant_id="...",
                    client_id="...", client_secret="...")
    """
    if provider == "aws_secrets_manager":
        from cloudrift.secrets.aws_secrets_manager import AWSSecretsManagerBackend

        if "aws_access_key_id" in kwargs:
            return AWSSecretsManagerBackend.from_access_key(**kwargs)
        if "profile_name" in kwargs:
            return AWSSecretsManagerBackend.from_profile(**kwargs)
        return AWSSecretsManagerBackend.from_iam_role(**kwargs)

    if provider == "azure_keyvault":
        from cloudrift.secrets.azure_keyvault import AzureKeyVaultBackend

        if "client_secret" in kwargs:
            return AzureKeyVaultBackend.from_service_principal(**kwargs)
        return AzureKeyVaultBackend.from_managed_identity(**kwargs)

    raise ValueError(
        f"Unknown secrets provider: {provider!r}. "
        "Choose 'aws_secrets_manager' or 'azure_keyvault'."
    )


__all__ = ["SecretBackend", "get_secrets"]
