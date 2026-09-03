"""Firestore with MongoDB compatibility connection factory (synchronous).

Returns a configured :class:`pymongo.MongoClient` — the blocking counterpart of
:mod:`cloudrift.document.firestore` for services that don't run an event loop.
Identical in shape to :mod:`cloudrift.document.documentdb_sync` and
:mod:`cloudrift.document.cosmos_sync`.

URI construction (including Firestore's three mandatory options) is shared with
the async factory via :mod:`cloudrift.document._firestore_uri`, so the two cannot
drift.

Lifecycle is caller-managed: call ``client.close()`` at shutdown.
"""

from pymongo import MongoClient

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
) -> MongoClient:
    """Connect using a full Firestore MongoDB-compatibility connection string.

    Firestore's mandatory URI options are filled in if absent — see
    :mod:`cloudrift.document._firestore_uri`.
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
) -> MongoClient:
    """Connect with Google Cloud credentials via OIDC — no stored secret.

    Requires ``pymongo>=4.7``. See :func:`cloudrift.document.firestore.connect_oidc`
    for the argument semantics.
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
) -> MongoClient:
    """Connect with a SCRAM-SHA-256 user credential created on the database."""
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
) -> MongoClient:
    """Connect with a short-lived OAuth 2.0 access token.

    The token is embedded in the URI and is not refreshed — prefer
    :func:`connect_oidc` for a long-lived client.
    """
    uri = build_access_token_uri(uid, location, database, access_token, port=port)
    return _client(uri, max_pool_size, min_pool_size, client_kwargs)


def _client(
    uri: str,
    max_pool_size: int,
    min_pool_size: int,
    client_kwargs: dict,
) -> MongoClient:
    kwargs: dict = {"maxPoolSize": max_pool_size, "minPoolSize": min_pool_size}
    kwargs.update(client_kwargs)
    try:
        return MongoClient(uri, **kwargs)
    except Exception as e:
        raise DocumentConnectionError(f"Failed to connect to Firestore: {e}") from e
