from cloudrift.email.base import Attachment, EmailBackend, EmailMessage


def get_email(provider: str, **kwargs) -> EmailBackend:
    """Factory to instantiate an email backend.

    Args:
        provider: ``"ses"``, ``"azure_acs"``, or ``"smtp"``.
        **kwargs: Provider-specific config. The factory routes to the
            appropriate ``from_*`` classmethod based on which credential keys
            are present.

    Returns:
        An :class:`EmailBackend` instance.

    Examples:
        get_email("ses", region="us-east-1", default_from="noreply@example.com")
        get_email("ses", aws_access_key_id="AKIA...", aws_secret_access_key="...",
                  default_from="noreply@example.com")
        get_email("azure_acs", connection_string="endpoint=https://...;accesskey=...",
                  default_from="DoNotReply@example.com")
        get_email("azure_acs", endpoint="https://...communication.azure.com",
                  default_from="DoNotReply@example.com")
        get_email("smtp", host="smtp.sendgrid.net", username="apikey", password="...",
                  default_from="noreply@example.com")
        get_email("smtp", mode="tls", host="smtp.example.com", port=465,
                  username="user", password="pw", default_from="...")
    """
    if provider == "ses":
        from cloudrift.email.ses import AWSSESBackend

        if "aws_access_key_id" in kwargs:
            return AWSSESBackend.from_access_key(**kwargs)
        if "profile_name" in kwargs:
            return AWSSESBackend.from_profile(**kwargs)
        return AWSSESBackend.from_iam_role(**kwargs)

    if provider == "azure_acs":
        from cloudrift.email.azure_acs import AzureACSEmailBackend

        if "connection_string" in kwargs:
            return AzureACSEmailBackend.from_connection_string(**kwargs)
        if "client_secret" in kwargs:
            return AzureACSEmailBackend.from_service_principal(**kwargs)
        return AzureACSEmailBackend.from_managed_identity(**kwargs)

    if provider == "smtp":
        from cloudrift.email.smtp import SMTPEmailBackend

        mode = kwargs.pop("mode", "starttls")
        if mode == "tls":
            return SMTPEmailBackend.from_tls(**kwargs)
        if mode == "plaintext":
            return SMTPEmailBackend.from_plaintext(**kwargs)
        if mode == "starttls":
            return SMTPEmailBackend.from_starttls(**kwargs)
        raise ValueError(
            f"Unknown SMTP mode: {mode!r}. Choose 'plaintext', 'starttls', or 'tls'."
        )

    raise ValueError(
        f"Unknown email provider: {provider!r}. "
        "Choose 'ses', 'azure_acs', or 'smtp'."
    )


__all__ = ["Attachment", "EmailBackend", "EmailMessage", "get_email"]
