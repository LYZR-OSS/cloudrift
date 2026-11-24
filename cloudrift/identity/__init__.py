from cloudrift.identity.base import (
    OIDCWorkloadVerifier,
    WorkloadIdentityVerifier,
    WorkloadPrincipal,
)


def get_workload_verifier(provider: str, **kwargs) -> WorkloadIdentityVerifier:
    """Factory to instantiate a workload identity token verifier.

    Verifies that a caller is a particular cloud-managed workload identity, so
    a service can authenticate another service with no shared secret. The
    workload obtains a token from its own cloud platform at runtime; this
    checks it and reports the principal.

    Args:
        provider: ``"entra"`` (Microsoft Entra -- Azure managed identity and
            workload identity federation).
        **kwargs: Provider-specific config, routed to the matching ``from_*``
            constructor.

    Returns:
        A WorkloadIdentityVerifier instance. Construct once at startup and
        reuse: the signing-key cache lives on the instance.

    Raises:
        ValueError: unknown provider.

    Examples:
        get_workload_verifier(
            "entra",
            tenant_id="<directory id>",
            accepted_audiences=["api://my-service"],
        )

        # Sovereign cloud
        get_workload_verifier(
            "entra",
            tenant_id="...",
            accepted_audiences=["api://my-service"],
            jwks_url="https://login.microsoftonline.us/<tid>/discovery/v2.0/keys",
            extra_issuers=["https://login.microsoftonline.us/<tid>/v2.0"],
        )

    Note:
        AWS (EKS/IRSA) and GCP (GKE Workload Identity) issue comparable OIDC
        tokens and fit ``OIDCWorkloadVerifier`` without changes to the base --
        they are simply not implemented yet, and a provider is added here
        rather than by subclassing outside the package.
    """
    if provider == "entra":
        from cloudrift.identity.entra import EntraWorkloadVerifier

        return EntraWorkloadVerifier.from_tenant(**kwargs)

    raise ValueError(f"Unsupported workload identity provider: {provider}")


__all__ = [
    "get_workload_verifier",
    "OIDCWorkloadVerifier",
    "WorkloadIdentityVerifier",
    "WorkloadPrincipal",
]
