"""Cloud SQL IAM database authentication token minting.

Cloud SQL IAM database authentication replaces the database password with a
short-lived OAuth 2.0 access token — the same shape as AWS RDS IAM auth, with
one difference: minting a GCP token is a real network round trip to the OAuth
endpoint, unlike RDS's local SigV4 presign, so the credential is built once (by
:func:`build_cloud_sql_credentials`, called lazily on first use and cached on
the backend) and :func:`cloud_sql_access_token` refreshes it only when the
cached token has actually expired — never unconditionally. This is the same
cache-with-conditional-refresh shape as
:class:`cloudrift.cache.redis_memorystore._GCPIAMCredentialProvider`, for the
same reason.

``google.auth``'s refresh path is synchronous, so it is offloaded to a worker
thread exactly as the RDS variant offloads ``generate_db_auth_token``.
"""

import asyncio

from cloudrift.core.exceptions import SQLAuthError

#: The only scope Cloud SQL IAM database authentication accepts. Notably *not*
#: ``cloud-platform``: a token minted for the broad scope is rejected at login.
CLOUD_SQL_LOGIN_SCOPE = "https://www.googleapis.com/auth/sqlservice.login"


def build_cloud_sql_credentials(
    service_account_file: str | None = None,
    service_account_info: dict | None = None,
    prefer_metadata: bool = False,
):
    """Build the ``google.auth`` credentials for Cloud SQL IAM auth.

    Call once — at first use, not per connection — and hold the result; see
    :func:`cloud_sql_access_token`. Building touches only local state (parsing a
    key file, or resolving ADC from a local file/the metadata server), so this is
    a plain synchronous call, matching every other GCP backend's `from_*`
    classmethods.
    """
    from cloudrift.core.gcp_credentials import build_credentials

    return build_credentials(
        service_account_file=service_account_file,
        service_account_info=service_account_info,
        scopes=[CLOUD_SQL_LOGIN_SCOPE],
        prefer_metadata=prefer_metadata,
    )


async def cloud_sql_access_token(credentials) -> str:
    """Return a valid access token from ``credentials``, refreshing only if the
    cached token has expired.

    ``credentials`` must be built once via :func:`build_cloud_sql_credentials`
    and reused across calls — refreshing unconditionally would mean a live OAuth
    round trip on every database connection instead of only once per token
    lifetime.

    Raises:
        SQLAuthError: if ``google-auth`` is missing or the refresh fails.
    """
    if credentials.valid:
        return credentials.token

    def _refresh() -> None:
        import google.auth.transport.requests

        credentials.refresh(google.auth.transport.requests.Request())

    try:
        await asyncio.to_thread(_refresh)
    except ImportError as e:  # pragma: no cover - import guard
        raise SQLAuthError(
            "Cloud SQL IAM auth requires google-auth. Install cloudrift[gcp]."
        ) from e
    except Exception as e:
        raise SQLAuthError(f"Failed to mint Cloud SQL IAM access token: {e}") from e
    return credentials.token
