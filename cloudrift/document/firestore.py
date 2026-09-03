"""Firestore with MongoDB compatibility connection factory.

Firestore's MongoDB-compatible mode exposes a MongoDB wire-protocol endpoint, so
this connects with Motor and returns a
:class:`motor.motor_asyncio.AsyncIOMotorClient` — identical in shape to
:mod:`cloudrift.document.documentdb` and :mod:`cloudrift.document.cosmos`. No
wrapper: the caller uses Motor's native async API directly.

This is *not* classic Firestore (the document API reached through
``google-cloud-firestore``). It is a database created in MongoDB-compatibility
mode, which is the only Firestore flavor that speaks the Mongo wire protocol and
therefore the only one that fits this category's contract.

Three auth paths, in order of preference for a service:

- :func:`connect_oidc` — Google Cloud credentials (ADC / Workload Identity), no
  secret to store or rotate. Needs ``pymongo>=4.7``.
- :func:`connect_scram` — username/password credential on the database.
- :func:`connect_access_token` — a short-lived OAuth 2.0 token you mint yourself.

:func:`connect_uri` takes a full connection string, e.g. the output of
``gcloud firestore databases connection-string``.

Lifecycle is caller-managed: call ``client.close()`` at shutdown.
"""

from motor.motor_asyncio import AsyncIOMotorClient

from cloudrift.core.exceptions import DocumentConnectionError
from cloudrift.document._firestore_uri import (
    build_access_token_uri,
    build_oidc_uri,
    build_scram_uri,
    ensure_required_params,
)


def connect_uri(
    uri: str,
    *,
    max_pool_size: int = 100,
    min_pool_size: int = 0,
    **client_kwargs,
) -> AsyncIOMotorClient:
    """Connect using a full Firestore MongoDB-compatibility connection string.

    Firestore's three mandatory URI options (``loadBalanced``, ``tls``,
    ``retryWrites=false``) are filled in if absent — see
    :mod:`cloudrift.document._firestore_uri` for why they are not optional.

    Args:
        uri: Connection string, e.g. from
            ``gcloud firestore databases connection-string``.
        max_pool_size: Max connection pool size.
        min_pool_size: Min connection pool size.
        **client_kwargs: Extra keyword arguments passed to Motor.
    """
    return _client(ensure_required_params(uri), max_pool_size, min_pool_size, client_kwargs)


def connect_oidc(
    uid: str,
    location: str,
    database: str,
    *,
    port: int = 443,
    max_pool_size: int = 100,
    min_pool_size: int = 0,
    **client_kwargs,
) -> AsyncIOMotorClient:
    """Connect with Google Cloud credentials via OIDC — no stored secret.

    The driver obtains and refreshes an identity token itself, so this works
    under GKE Workload Identity, Cloud Run, and a GCE VM with an attached
    service account. The caller needs ``roles/datastore.user`` on the database.

    Requires ``pymongo>=4.7`` (the release that added Google Cloud OIDC support);
    cloudrift's floor is ``4.6.3`` because the other document providers do not
    need it.

    Args:
        uid: Database UID — the system-generated UUID in the endpoint hostname,
            not the database ID.
        location: Database location, e.g. ``nam5`` or ``us-central1``.
        database: Database ID.
        port: Endpoint port (default ``443``).
        max_pool_size: Max connection pool size.
        min_pool_size: Min connection pool size.
        **client_kwargs: Extra keyword arguments passed to Motor.
    """
    uri = build_oidc_uri(uid, location, database, port=port)
    return _client(uri, max_pool_size, min_pool_size, client_kwargs)


def connect_scram(
    uid: str,
    location: str,
    database: str,
    username: str,
    password: str,
    *,
    port: int = 443,
    max_pool_size: int = 100,
    min_pool_size: int = 0,
    **client_kwargs,
) -> AsyncIOMotorClient:
    """Connect with a SCRAM-SHA-256 user credential created on the database.

    Args:
        uid: Database UID — the UUID in the endpoint hostname.
        location: Database location, e.g. ``nam5``.
        database: Database ID.
        username: User credential name.
        password: User credential password. GCP displays this once at creation
            and cannot show it again.
        port: Endpoint port (default ``443``).
        max_pool_size: Max connection pool size.
        min_pool_size: Min connection pool size.
        **client_kwargs: Extra keyword arguments passed to Motor.
    """
    uri = build_scram_uri(uid, location, database, username, password, port=port)
    return _client(uri, max_pool_size, min_pool_size, client_kwargs)


def connect_access_token(
    uid: str,
    location: str,
    database: str,
    access_token: str,
    *,
    port: int = 443,
    max_pool_size: int = 100,
    min_pool_size: int = 0,
    **client_kwargs,
) -> AsyncIOMotorClient:
    """Connect with a short-lived OAuth 2.0 access token.

    The token is embedded in the connection URI, so this client stops
    authenticating once it expires — build a new one, or prefer
    :func:`connect_oidc`, which refreshes itself. Useful when the token comes
    from impersonating another service account.

    Args:
        uid: Database UID — the UUID in the endpoint hostname.
        location: Database location, e.g. ``nam5``.
        database: Database ID.
        access_token: OAuth 2.0 access token for an identity holding
            ``roles/datastore.user``.
        port: Endpoint port (default ``443``).
        max_pool_size: Max connection pool size.
        min_pool_size: Min connection pool size.
        **client_kwargs: Extra keyword arguments passed to Motor.
    """
    uri = build_access_token_uri(uid, location, database, access_token, port=port)
    return _client(uri, max_pool_size, min_pool_size, client_kwargs)


def _client(
    uri: str,
    max_pool_size: int,
    min_pool_size: int,
    client_kwargs: dict,
) -> AsyncIOMotorClient:
    kwargs: dict = {"maxPoolSize": max_pool_size, "minPoolSize": min_pool_size}
    kwargs.update(client_kwargs)
    try:
        return AsyncIOMotorClient(uri, **kwargs)
    except Exception as e:
        raise DocumentConnectionError(f"Failed to connect to Firestore: {e}") from e
