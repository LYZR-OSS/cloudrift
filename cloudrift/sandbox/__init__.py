from cloudrift.sandbox.base import ExecResult, FileEntry, SandboxBackend


def get_sandbox(provider: str, **kwargs) -> SandboxBackend:
    """Factory to instantiate a sandbox backend.

    Args:
        provider: ``"lambda_microvm"`` (AWS Lambda MicroVMs), ``"aca_sessions"``
            (Azure Container Apps custom container session pool), or ``"e2b"``.
        **kwargs: Provider-specific config. For ``lambda_microvm`` the factory
            routes to the appropriate ``from_*`` classmethod based on which
            credential keys are present.

    Examples:
        get_sandbox("lambda_microvm",
                    image_identifier="arn:aws:lambda:us-east-1:123456789012:microvm-image:lyzr-sandbox",
                    image_version="1.0", region="us-east-1")
        get_sandbox("aca_sessions", pool_endpoint="https://pool.env-id.eastus.azurecontainerapps.io")
        get_sandbox("e2b", api_key="e2b_...")
    """
    if provider == "lambda_microvm":
        from cloudrift.sandbox.aws_microvm import AWSMicroVMSandboxBackend

        if "aws_access_key_id" in kwargs:
            return AWSMicroVMSandboxBackend.from_access_key(**kwargs)
        if "profile_name" in kwargs:
            return AWSMicroVMSandboxBackend.from_profile(**kwargs)
        return AWSMicroVMSandboxBackend.from_iam_role(**kwargs)

    if provider == "aca_sessions":
        from cloudrift.sandbox.azure_aca import AzureACASessionsBackend

        return AzureACASessionsBackend.from_managed_identity(**kwargs)

    if provider == "e2b":
        from cloudrift.sandbox.e2b import E2BSandboxBackend

        return E2BSandboxBackend.from_api_key(**kwargs)

    raise ValueError(
        f"Unknown sandbox provider: {provider!r}. Choose 'lambda_microvm', 'aca_sessions', or 'e2b'."
    )


__all__ = ["ExecResult", "FileEntry", "SandboxBackend", "get_sandbox"]
