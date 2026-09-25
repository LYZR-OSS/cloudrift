"""Shared aiobotocore session construction for cloudrift AWS backends.

Every AWS backend authenticates the same way, so the session builder is
defined once here instead of being copy-pasted into each provider module —
the AWS counterpart of how ``core/azure_credentials.py`` centralizes the
Azure credential chain.
"""

from aiobotocore.session import AioSession


def build_session(
    *,
    region: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_session_token: str | None = None,
    profile_name: str | None = None,
    exclude_env_credentials: bool = False,
) -> AioSession:
    """Build an aiobotocore session for a cloudrift AWS backend.

    Explicit keys win; otherwise the ambient chain is used. When
    ``exclude_env_credentials`` is set, the ``env`` provider is dropped so a
    stray process environment cannot shadow an auto-refreshing container or
    instance role.
    """
    session = AioSession(profile=profile_name)
    if aws_access_key_id and aws_secret_access_key:
        session.set_credentials(aws_access_key_id, aws_secret_access_key, aws_session_token)
    elif exclude_env_credentials:
        session.get_component("credential_provider").remove("env")
    if region:
        session.set_config_variable("region", region)
    return session
