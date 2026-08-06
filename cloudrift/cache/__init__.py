from urllib.parse import quote

from cloudrift.cache.base import CacheBackend, resilient_client_kwargs

_VALID_SSL_CERT_REQS = ("CERT_NONE", "CERT_OPTIONAL", "CERT_REQUIRED")


def cache_broker_url(
    provider: str,
    host: str,
    port: int,
    password: str = "",
    db: int = 0,
    ssl_cert_reqs: str = "CERT_NONE",
) -> str:
    """Return a Redis URL (``redis://`` or ``rediss://``) suitable for clients
    that require URL-based configuration — most notably Celery, which cannot
    consume a :class:`CacheBackend` directly.

    Args:
        provider: ``"redis"`` (self-hosted), ``"elasticache"`` (AWS),
            ``"azure_redis"``, or ``"memorystore"`` (GCP).
        host: Redis host.
        port: Redis port (6379 for plain, 6380 for TLS, 10000 for some Azure
            tiers, 6378 for Memorystore in-transit encryption — pass the value
            the cluster actually listens on).
        password: Optional. Omit (or pass empty string) for unauthenticated
            self-hosted Redis. For ``elasticache`` / ``azure_redis`` /
            ``memorystore`` this is the AUTH token / access key / AUTH string.
        db: Redis database index.
        ssl_cert_reqs: TLS verification mode for cloud providers. One of
            ``CERT_NONE`` / ``CERT_OPTIONAL`` / ``CERT_REQUIRED`` (defaults to
            ``CERT_REQUIRED``). Ignored when ``provider == "redis"``.

    Notes:
        Token-based auth (ElastiCache IAM, Azure Managed Identity / Service
        Principal, Memorystore IAM) cannot be expressed in a static URL — for
        those, configure the consumer (e.g. Celery) with a CredentialProvider
        instead of a URL.

        Memorystore leaves in-transit encryption off by default, so a
        ``memorystore`` URL is only correct for an instance that has it enabled;
        the CA cannot be expressed in the URL either, so a consumer verifying
        the per-instance CA needs its own TLS configuration.

        Celery's Redis transport does not forward the ``ssl_cert_reqs`` query
        parameter to redis-py; when used as a Celery broker URL the value is
        silently ignored. To enforce non-default cert verification with Celery,
        set ``broker_use_ssl`` (e.g. ``{"ssl_cert_reqs": ssl.CERT_REQUIRED}``)
        on the Celery app config in addition to passing this URL.
    """
    # Validate eagerly so a bad value fails at the call site rather than at
    # connection time — applies to every provider, even where it's unused.
    if ssl_cert_reqs not in _VALID_SSL_CERT_REQS:
        raise ValueError(
            f"Invalid ssl_cert_reqs: {ssl_cert_reqs!r}. "
            f"Must be one of: {', '.join(_VALID_SSL_CERT_REQS)}."
        )
    if not isinstance(db, int) or isinstance(db, bool) or db < 0:
        raise ValueError(f"Invalid db: {db!r}. Must be a non-negative integer.")

    # When a password is present, include the ``default`` username so the URL is
    # valid against Redis 6+ ACL deployments (``redis://default:pw@host``).
    # Without it, ``redis://:pw@host`` can silently fail to authenticate. The
    # password is percent-encoded so special characters (@ : / ? # %) don't
    # corrupt the URL.
    auth = f"default:{quote(password, safe='')}@" if password else ""

    if provider == "redis":
        return f"redis://{auth}{host}:{port}/{db}"
    if provider in ("elasticache", "azure_redis", "memorystore"):
        return f"rediss://{auth}{host}:{port}/{db}?ssl_cert_reqs={ssl_cert_reqs}"
    raise ValueError(
        f"Unsupported cache provider for broker URL: {provider!r}. "
        "Must be one of: 'redis', 'elasticache', 'azure_redis', 'memorystore'."
    )


def get_cache(provider: str, auth_method: str, **kwargs) -> CacheBackend:
    """Factory to instantiate a cache backend.

    Args:
        provider: ``"redis"``, ``"elasticache"``, ``"azure_redis"``, or
            ``"memorystore"``
        auth_method: The factory classmethod to call on the backend class.
            See each backend for supported methods.
        **kwargs: Arguments forwarded to the chosen factory method.

    Returns:
        A ``CacheBackend`` instance.

    Examples::

        get_cache("redis", "from_credentials", host="localhost", port=6379)
        get_cache("redis", "from_url", url="rediss://user:pass@host:6380/0")
        get_cache("elasticache", "from_auth_token", host="...", auth_token="...")
        get_cache("elasticache", "from_iam_auth", host="...", username="...", region="us-east-1")
        get_cache("azure_redis", "from_access_key", host="...", access_key="...")
        get_cache("azure_redis", "from_managed_identity", host="...", username="...")
        get_cache("memorystore", "from_auth_string", host="10.0.0.3", auth_string="...")
        get_cache("memorystore", "from_iam_auth", host="10.0.0.3")
    """
    if provider == "redis":
        from cloudrift.cache.redis_standalone import StandaloneRedisBackend as _Backend
    elif provider == "elasticache":
        from cloudrift.cache.redis_elasticache import AWSElastiCacheBackend as _Backend
    elif provider == "azure_redis":
        from cloudrift.cache.redis_azure import AzureRedisCacheBackend as _Backend
    elif provider == "memorystore":
        from cloudrift.cache.redis_memorystore import GCPMemorystoreBackend as _Backend
    else:
        raise ValueError(
            f"Unknown cache provider: {provider!r}. "
            "Choose 'redis', 'elasticache', 'azure_redis', or 'memorystore'."
        )

    factory = getattr(_Backend, auth_method, None)
    if factory is None:
        raise ValueError(f"{_Backend.__name__} has no auth method {auth_method!r}.")
    return factory(**kwargs)


__all__ = ["CacheBackend", "get_cache", "cache_broker_url", "resilient_client_kwargs"]
