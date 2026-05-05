"""AWS DocumentDB connection factory.

Returns a configured :class:`motor.motor_asyncio.AsyncIOMotorClient`. The caller
selects database and collection (``client[db][collection]``) and uses Motor's
native async API directly.

Lifecycle is caller-managed: call ``client.close()`` at shutdown.
"""
from urllib.parse import quote_plus

from motor.motor_asyncio import AsyncIOMotorClient

from cloudrift.core.exceptions import DocumentConnectionError


def connect_uri(
    uri: str,
    *,
    tls_ca_file: str | None = None,
    max_pool_size: int = 100,
    min_pool_size: int = 0,
    **client_kwargs,
) -> AsyncIOMotorClient:
    """Connect using a full MongoDB-compatible URI."""
    kwargs: dict = {"maxPoolSize": max_pool_size, "minPoolSize": min_pool_size}
    if tls_ca_file:
        kwargs["tlsCAFile"] = tls_ca_file
    kwargs.update(client_kwargs)
    try:
        return AsyncIOMotorClient(uri, **kwargs)
    except Exception as e:
        raise DocumentConnectionError(f"Failed to connect to DocumentDB: {e}") from e


def connect_credentials(
    host: str,
    port: int,
    username: str,
    password: str,
    *,
    tls: bool = True,
    tls_ca_file: str | None = None,
    max_pool_size: int = 100,
    min_pool_size: int = 0,
) -> AsyncIOMotorClient:
    """Connect using explicit host, port, username, and password.

    Args:
        host: DocumentDB cluster endpoint hostname.
        port: Port number (DocumentDB default: 27017).
        username: Database username.
        password: Database password.
        tls: Enable TLS (default ``True``; required for AWS DocumentDB).
        tls_ca_file: Optional path to the CA certificate bundle (PEM).
        max_pool_size: Max connection pool size.
        min_pool_size: Min connection pool size.
    """
    uri = f"mongodb://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/"
    kwargs: dict = {
        "tls": tls,
        "maxPoolSize": max_pool_size,
        "minPoolSize": min_pool_size,
    }
    if tls_ca_file:
        kwargs["tlsCAFile"] = tls_ca_file
    try:
        return AsyncIOMotorClient(uri, **kwargs)
    except Exception as e:
        raise DocumentConnectionError(f"Failed to connect to DocumentDB: {e}") from e


def connect_tls_cert(
    host: str,
    port: int,
    username: str,
    password: str,
    *,
    tls_cert_key_file: str,
    tls_ca_file: str | None = None,
    max_pool_size: int = 100,
    min_pool_size: int = 0,
) -> AsyncIOMotorClient:
    """Connect using mutual TLS (mTLS) with a client certificate.

    Args:
        host: DocumentDB cluster endpoint hostname.
        port: Port number (DocumentDB default: 27017).
        username: Database username.
        password: Database password.
        tls_cert_key_file: Path to a PEM file containing the client private key
            followed by the client certificate (combined, as required by
            pymongo/Motor).
        tls_ca_file: Optional path to the CA certificate bundle (PEM).
        max_pool_size: Max connection pool size.
        min_pool_size: Min connection pool size.
    """
    uri = f"mongodb://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/"
    kwargs: dict = {
        "tls": True,
        "tlsCertificateKeyFile": tls_cert_key_file,
        "maxPoolSize": max_pool_size,
        "minPoolSize": min_pool_size,
    }
    if tls_ca_file:
        kwargs["tlsCAFile"] = tls_ca_file
    try:
        return AsyncIOMotorClient(uri, **kwargs)
    except Exception as e:
        raise DocumentConnectionError(f"Failed to connect to DocumentDB: {e}") from e
