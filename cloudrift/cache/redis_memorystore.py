import redis.asyncio as aioredis
from redis.credentials import CredentialProvider

from cloudrift.cache.base import CacheBackend, _RedisMixin, resilient_client_kwargs
from cloudrift.core.exceptions import CacheConnectionError


class GCPMemorystoreBackend(_RedisMixin, CacheBackend):
    """Google Cloud Memorystore (Redis / Valkey) backend.

    Use one of the class methods to construct:
    - ``from_auth_string``     — Memorystore AUTH string (shared secret)
    - ``from_iam_auth``        — IAM authentication (OAuth2 access token as password)
    - ``from_server_ca_cert``  — in-transit encryption pinned to the server CA

    Unlike ElastiCache and Azure Cache for Redis, Memorystore Standard leaves
    both AUTH and in-transit encryption **off** by default, so the defaults here
    do not assume TLS — ``ssl`` defaults to ``False`` on
    :meth:`from_auth_string`. Memorystore for Redis *Cluster* and Valkey require
    TLS with IAM auth, which :meth:`from_iam_auth` reflects.
    """

    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_auth_string(
        cls,
        host: str,
        port: int = 6379,
        auth_string: str | None = None,
        db: int = 0,
        ssl: bool = False,
        ssl_ca_certs: str | None = None,
        decode_responses: bool = False,
        **client_kwargs,
    ) -> "GCPMemorystoreBackend":
        """Connect using the Memorystore AUTH string (shared secret).

        Args:
            host: Memorystore primary endpoint IP or hostname.
            port: Redis port (default 6379).
            auth_string: The instance's AUTH string. Omit for an instance with
                AUTH disabled.
            db: Database index (default 0).
            ssl: Enable TLS. Defaults to ``False`` because Memorystore
                in-transit encryption is opt-in; set ``True`` (and normally
                ``ssl_ca_certs``) for an instance that has it enabled.
            ssl_ca_certs: Path to the instance's server CA certificate. Memorystore
                signs its server certificate with a per-instance CA that is not in
                the system trust store, so TLS without this fails verification —
                use :meth:`from_server_ca_cert`, which requires it.
            decode_responses: When ``True``, reads return ``str`` instead of ``bytes``.
            **client_kwargs: Overrides for the connection-resilience defaults
                documented on ``resilient_client_kwargs``.
        """
        try:
            client = aioredis.Redis(
                host=host,
                port=port,
                password=auth_string,
                db=db,
                ssl=ssl,
                ssl_ca_certs=ssl_ca_certs,
                **resilient_client_kwargs(decode_responses=decode_responses, **client_kwargs),
            )
            return cls(client)
        except Exception as e:
            raise CacheConnectionError(f"Failed to connect to Memorystore: {e}") from e

    @classmethod
    def from_server_ca_cert(
        cls,
        host: str,
        ssl_ca_certs: str,
        port: int = 6378,
        auth_string: str | None = None,
        db: int = 0,
        decode_responses: bool = False,
        **client_kwargs,
    ) -> "GCPMemorystoreBackend":
        """Connect with in-transit encryption, verifying the per-instance server CA.

        Memorystore's server certificate is signed by a CA unique to the
        instance (downloadable from the instance details), not by a public root,
        so ``ssl_ca_certs`` is required rather than optional here.

        Args:
            host: Memorystore primary endpoint IP or hostname.
            ssl_ca_certs: Path to the instance's server CA certificate (PEM).
            port: TLS port (default 6378 — Memorystore's in-transit encryption port).
            auth_string: Optional AUTH string, if the instance also has AUTH enabled.
            db: Database index (default 0).
            decode_responses: When ``True``, reads return ``str`` instead of ``bytes``.
            **client_kwargs: Overrides for the connection-resilience defaults
                documented on ``resilient_client_kwargs``.
        """
        try:
            client = aioredis.Redis(
                host=host,
                port=port,
                password=auth_string,
                db=db,
                ssl=True,
                ssl_ca_certs=ssl_ca_certs,
                **resilient_client_kwargs(decode_responses=decode_responses, **client_kwargs),
            )
            return cls(client)
        except Exception as e:
            raise CacheConnectionError(
                f"Failed to connect to Memorystore (in-transit encryption): {e}"
            ) from e

    @classmethod
    def from_iam_auth(
        cls,
        host: str,
        port: int = 6379,
        db: int = 0,
        ssl: bool = True,
        ssl_ca_certs: str | None = None,
        username: str = "default",
        service_account_file: str | None = None,
        service_account_info: dict | None = None,
        prefer_metadata: bool = False,
        decode_responses: bool = False,
        **client_kwargs,
    ) -> "GCPMemorystoreBackend":
        """Connect using IAM authentication (Memorystore for Redis Cluster / Valkey).

        The password is a short-lived OAuth2 access token for the caller's
        service account, refreshed automatically on reconnect via a
        ``CredentialProvider`` — the same mechanism as ElastiCache SigV4 and
        Azure Entra token auth.

        Requires the instance to have IAM auth enabled and the caller to hold
        ``roles/redis.dbConnectionUser``.

        Args:
            host: Memorystore endpoint IP or hostname.
            port: Redis port (default 6379).
            db: Database index (default 0).
            ssl: Enable TLS (default ``True``; required alongside IAM auth).
            ssl_ca_certs: Path to the server CA certificate (PEM), when the
                instance uses a per-instance CA.
            username: Redis ACL username. Memorystore expects ``"default"``.
            service_account_file: Path to a service-account JSON key file.
            service_account_info: Parsed service-account JSON, for keys held in a
                secret store.
            prefer_metadata: Read the attached service account straight from the
                metadata server — see :mod:`cloudrift.core.gcp_credentials`.
            decode_responses: When ``True``, reads return ``str`` instead of ``bytes``.
            **client_kwargs: Overrides for the connection-resilience defaults
                documented on ``resilient_client_kwargs``.
        """
        try:
            from cloudrift.core.gcp_credentials import build_credentials

            credentials = build_credentials(
                service_account_file=service_account_file,
                service_account_info=service_account_info,
                prefer_metadata=prefer_metadata,
            )
            provider = _GCPIAMCredentialProvider(credentials, username)
            client = aioredis.Redis(
                host=host,
                port=port,
                db=db,
                ssl=ssl,
                ssl_ca_certs=ssl_ca_certs,
                credential_provider=provider,
                **resilient_client_kwargs(decode_responses=decode_responses, **client_kwargs),
            )
            return cls(client)
        except Exception as e:
            raise CacheConnectionError(f"Failed to connect to Memorystore (IAM): {e}") from e


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


class _GCPIAMCredentialProvider(CredentialProvider):
    """Fetches and refreshes an OAuth2 access token for Memorystore IAM auth.

    Uses the *sync* ``google.auth`` refresh path so that ``get_credentials()``
    (which redis-py calls synchronously on each new connection) stays
    synchronous. Access tokens last ~1 hour; the ``valid`` check below refreshes
    only when the cached one has expired, so steady-state reconnects cost
    nothing.
    """

    def __init__(self, credentials, username: str) -> None:
        self._credentials = credentials
        self._username = username

    def get_credentials(self) -> tuple[str, str]:
        if not self._credentials.valid:
            import google.auth.transport.requests

            self._credentials.refresh(google.auth.transport.requests.Request())
        return self._username, self._credentials.token
