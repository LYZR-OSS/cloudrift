from cloudrift.pubsub.base import PubSubBackend


def get_pubsub(provider: str, **kwargs) -> PubSubBackend:
    """Factory to instantiate a pub/sub backend.

    Args:
        provider: ``"sns"`` or ``"azure_eventgrid"``.
        **kwargs: Provider-specific config. The factory routes to the appropriate
            ``from_*`` classmethod based on which credential keys are present.

    Returns:
        A PubSubBackend instance.

    Examples:
        get_pubsub("sns", region="us-east-1")  # IAM/env
        get_pubsub("sns", aws_access_key_id="AKIA...", aws_secret_access_key="...")
        get_pubsub("azure_eventgrid", endpoint="https://...", access_key="...")
        get_pubsub("azure_eventgrid", endpoint="...", tenant_id="...",
                    client_id="...", client_secret="...")
    """
    if provider == "sns":
        from cloudrift.pubsub.sns import AWSSNSBackend

        if "aws_access_key_id" in kwargs:
            return AWSSNSBackend.from_access_key(**kwargs)
        if "profile_name" in kwargs:
            return AWSSNSBackend.from_profile(**kwargs)
        return AWSSNSBackend.from_iam_role(**kwargs)

    if provider == "azure_eventgrid":
        from cloudrift.pubsub.azure_eventgrid import AzureEventGridBackend

        if "access_key" in kwargs:
            return AzureEventGridBackend.from_access_key(**kwargs)
        if "client_secret" in kwargs:
            return AzureEventGridBackend.from_service_principal(**kwargs)
        return AzureEventGridBackend.from_managed_identity(**kwargs)

    raise ValueError(
        f"Unknown pubsub provider: {provider!r}. "
        "Choose 'sns' or 'azure_eventgrid'."
    )


__all__ = ["PubSubBackend", "get_pubsub"]
