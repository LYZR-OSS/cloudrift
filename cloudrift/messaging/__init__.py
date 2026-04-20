from cloudrift.messaging.base import Message, MessagingBackend


def get_queue(provider: str, **kwargs) -> MessagingBackend:
    """Factory to instantiate a messaging backend.

    Args:
        provider: ``"sqs"`` or ``"azure_bus"``.
        **kwargs: Provider-specific config. The factory routes to the appropriate
            ``from_*`` classmethod based on which credential keys are present.

    Returns:
        A MessagingBackend instance.

    Examples:
        get_queue("sqs", queue_url="https://sqs...", region="us-east-1")
        get_queue("sqs", queue_url="...", aws_access_key_id="...", aws_secret_access_key="...")
        get_queue("azure_bus", connection_string="...", queue_name="my-queue")
        get_queue("azure_bus", fully_qualified_namespace="ns.servicebus.windows.net",
                   queue_name="q", client_id="...", client_secret="...", tenant_id="...")
    """
    if provider == "sqs":
        from cloudrift.messaging.sqs import AWSSQSBackend

        if "aws_access_key_id" in kwargs:
            return AWSSQSBackend.from_access_key(**kwargs)
        if "profile_name" in kwargs:
            return AWSSQSBackend.from_profile(**kwargs)
        return AWSSQSBackend.from_iam_role(**kwargs)

    if provider == "azure_bus":
        from cloudrift.messaging.azure_bus import AzureServiceBusBackend

        if "connection_string" in kwargs:
            return AzureServiceBusBackend.from_connection_string(**kwargs)
        if "client_secret" in kwargs:
            return AzureServiceBusBackend.from_service_principal(**kwargs)
        return AzureServiceBusBackend.from_managed_identity(**kwargs)

    raise ValueError(f"Unknown messaging provider: {provider!r}. Choose 'sqs' or 'azure_bus'.")


__all__ = ["Message", "MessagingBackend", "get_queue"]
