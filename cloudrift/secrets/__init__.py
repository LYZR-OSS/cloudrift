from cloudrift.secrets.base import SecretBackend


def get_secrets(provider: str, **kwargs) -> SecretBackend:
    """Factory to instantiate a secret management backend.

    Args:
        provider: ``"aws_secrets_manager"``, ``"azure_keyvault"``,
            ``"gcp_secret_manager"``, or a non-cloud source — ``"env"``
            (environment variables), ``"file"`` (a JSON file), or
            ``"memory"``/``"local"`` (in-memory mapping, mainly dev/tests).
        **kwargs: Provider-specific config. The cloud factories route to the
            appropriate ``from_*`` classmethod based on which credential keys are
            present; the local backends take their own simple kwargs.

    Returns:
        A SecretBackend instance.

    Examples:
        get_secrets("aws_secrets_manager", region="us-east-1")  # IAM/env
        get_secrets("aws_secrets_manager", aws_access_key_id="AKIA...",
                    aws_secret_access_key="...", region="us-east-1")
        get_secrets("azure_keyvault", vault_url="https://myvault.vault.azure.net")
        get_secrets("azure_keyvault", vault_url="...", tenant_id="...",
                    client_id="...", client_secret="...")
        get_secrets("gcp_secret_manager", project="my-project")  # ADC
        get_secrets("gcp_secret_manager", project="p",
                    service_account_file="/etc/gcp/sa.json")
        get_secrets("env", prefix="SECRET_")          # read SECRET_<name> env vars
        get_secrets("file", path="/run/secrets.json") # JSON {name: value}
        get_secrets("memory", mapping={"db": "..."})  # in-memory (dev/tests)
    """
    if provider == "env":
        from cloudrift.secrets.local import EnvSecretBackend

        return EnvSecretBackend(**kwargs)

    if provider == "file":
        from cloudrift.secrets.local import FileSecretBackend

        return FileSecretBackend(**kwargs)

    if provider in ("memory", "local"):
        from cloudrift.secrets.local import MappingSecretBackend

        return MappingSecretBackend(**kwargs)

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

    if provider == "gcp_secret_manager":
        from cloudrift.secrets.gcp_secret_manager import GCPSecretManagerBackend

        if "service_account_info" in kwargs:
            return GCPSecretManagerBackend.from_service_account_info(**kwargs)
        if "service_account_file" in kwargs:
            return GCPSecretManagerBackend.from_service_account_file(**kwargs)
        return GCPSecretManagerBackend.from_application_default(**kwargs)

    raise ValueError(
        f"Unknown secrets provider: {provider!r}. Choose 'aws_secrets_manager', "
        "'azure_keyvault', 'gcp_secret_manager', 'env', 'file', or 'memory'."
    )


__all__ = ["SecretBackend", "get_secrets"]
