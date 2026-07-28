import redis.asyncio as aioredis

from cloudrift.cache.base import CacheBackend, _RedisMixin, resilient_client_kwargs
from cloudrift.core.exceptions import CacheConnectionError


class StandaloneRedisBackend(_RedisMixin, CacheBackend):
    """Redis backend for self-hosted Redis (e.g. on EC2 or bare-metal).

    Use one of the class methods to construct:
    - ``from_url``         — full Redis URL (most flexible)
    - ``from_credentials`` — host/port + optional password/username/TLS
    - ``from_tls_cert``    — mTLS with client certificate and key files
    """

    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_url(
        cls,
        url: str,
        ssl_ca_certs: str | None = None,
        decode_responses: bool = False,
        **client_kwargs,
    ) -> "StandaloneRedisBackend":
        """Connect using a Redis URL.

        Args:
            url: e.g. ``redis://user:pass@localhost:6379/0`` or
                 ``rediss://user:pass@localhost:6380/0`` (TLS).
            ssl_ca_certs: Optional path to the CA certificate bundle (PEM) for TLS.
            decode_responses: When ``True``, read operations return ``str`` instead
                of ``bytes`` (mirrors redis-py's ``decode_responses``). Default
                ``False`` keeps the cache-backend contract of returning ``bytes``.
            **client_kwargs: Overrides for the connection-resilience defaults
                documented on ``resilient_client_kwargs``.
        """
        try:
            kwargs = resilient_client_kwargs(
                decode_responses=decode_responses, **client_kwargs
            )
            if ssl_ca_certs:
                kwargs["ssl_ca_certs"] = ssl_ca_certs
            return cls(aioredis.from_url(url, **kwargs))
        except Exception as e:
            raise CacheConnectionError(f"Failed to connect to Redis: {e}") from e

    @classmethod
    def from_credentials(
        cls,
        host: str,
        port: int = 6379,
        password: str | None = None,
        username: str | None = None,
        db: int = 0,
        ssl: bool = False,
        ssl_cert_reqs: str = "required",
        ssl_ca_certs: str | None = None,
        decode_responses: bool = False,
        **client_kwargs,
    ) -> "StandaloneRedisBackend":
        """Connect using explicit host, port, and optional credentials.

        Args:
            host: Redis server hostname or IP.
            port: Redis port (default 6379).
            password: Optional AUTH password.
            username: Optional ACL username (Redis 6+).
            db: Database index (default 0).
            ssl: Enable TLS (default ``False``). Required by managed Redis that
                only exposes a TLS port — e.g. Azure Managed Redis on 10000 or
                Azure Cache for Redis on 6380. Connecting in plaintext to a
                TLS-only port gets the connection closed by the server, which
                surfaces as ``ConnectionError: Connection closed by server.``
            ssl_cert_reqs: Server-certificate policy: ``"required"`` (default),
                ``"optional"``, or ``"none"``. Ignored unless ``ssl`` is true.
                Do not pass ``None`` — redis-py resolves that to ``CERT_NONE``,
                silently disabling verification.
            ssl_ca_certs: Optional path to the CA certificate bundle (PEM).
            decode_responses: When ``True``, read operations return ``str`` instead
                of ``bytes`` (mirrors redis-py's ``decode_responses``).
            **client_kwargs: Overrides for the connection-resilience defaults
                documented on ``resilient_client_kwargs``.
        """
        try:
            client = aioredis.Redis(
                host=host,
                port=port,
                password=password,
                username=username,
                db=db,
                ssl=ssl,
                ssl_cert_reqs=ssl_cert_reqs,
                ssl_ca_certs=ssl_ca_certs,
                **resilient_client_kwargs(
                    decode_responses=decode_responses, **client_kwargs
                ),
            )
            return cls(client)
        except Exception as e:
            raise CacheConnectionError(f"Failed to connect to Redis: {e}") from e

    @classmethod
    def from_tls_cert(
        cls,
        host: str,
        port: int = 6380,
        password: str | None = None,
        username: str | None = None,
        db: int = 0,
        ssl_certfile: str | None = None,
        ssl_keyfile: str | None = None,
        ssl_ca_certs: str | None = None,
        decode_responses: bool = False,
        **client_kwargs,
    ) -> "StandaloneRedisBackend":
        """Connect using mutual TLS (mTLS) with client certificate and key files.

        Args:
            host: Redis server hostname or IP.
            port: Redis TLS port (default 6380).
            password: Optional AUTH password.
            username: Optional ACL username (Redis 6+).
            db: Database index (default 0).
            ssl_certfile: Path to the client certificate PEM file.
            ssl_keyfile: Path to the client private key PEM file.
            ssl_ca_certs: Path to the CA certificate bundle (PEM) for server verification.
            decode_responses: When ``True``, read operations return ``str`` instead
                of ``bytes`` (mirrors redis-py's ``decode_responses``).
            **client_kwargs: Overrides for the connection-resilience defaults
                documented on ``resilient_client_kwargs``.
        """
        try:
            client = aioredis.Redis(
                host=host,
                port=port,
                password=password,
                username=username,
                db=db,
                ssl=True,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
                ssl_ca_certs=ssl_ca_certs,
                **resilient_client_kwargs(
                    decode_responses=decode_responses, **client_kwargs
                ),
            )
            return cls(client)
        except Exception as e:
            raise CacheConnectionError(f"Failed to connect to Redis (mTLS): {e}") from e
