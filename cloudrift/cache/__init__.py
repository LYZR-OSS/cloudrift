from cloudrift.cache.base import CacheBackend

_VALID_SSL_CERT_REQS = ("CERT_NONE", "CERT_OPTIONAL", "CERT_REQUIRED")


def cache_broker_url(
    provider: str,
    host: str,
    port: int,
    password: str = "",
    db: int = 0,
    ssl_cert_reqs: str = "CERT_REQUIRED",
) -> str:
    """Return a Redis URL (``redis://`` or ``rediss://``) suitable for clients
    that require URL-based configuration — most notably Celery, which cannot
    consume a :class:`CacheBackend` directly.

    Args:
        provider: ``"redis"`` (self-hosted), ``"elasticache"`` (AWS), or
            ``"azure_redis"``.
        host: Redis host.
        port: Redis port (6379 for plain, 6380 for TLS, 10000 for some Azure
            tiers — pass the value the cluster actually listens on).
        password: Optional. Omit (or pass empty string) for unauthenticated
            self-hosted Redis. For ``elasticache`` / ``azure_redis`` this is the
            AUTH token / access key.
        db: Redis database index.
        ssl_cert_reqs: TLS verification mode for cloud providers. One of
            ``CERT_NONE`` / ``CERT_OPTIONAL`` / ``CERT_REQUIRED`` (defaults to
            ``CERT_REQUIRED``). Ignored when ``provider == "redis"``.

    Notes:
        Token-based auth (ElastiCache IAM, Azure Managed Identity / Service
        Principal) cannot be expressed in a static URL — for those, configure
        the consumer (e.g. Celery) with a CredentialProvider instead of a URL.
    """
    # When a password is present, include the ``default`` username so the URL is
    # valid against Redis 6+ ACL deployments (``redis://default:pw@host``).
    # Without it, ``redis://:pw@host`` can silently fail to authenticate.
    auth = f"default:{password}@" if password else ""

    if provider == "redis":
        return f"redis://{auth}{host}:{port}/{db}"
    if provider in ("elasticache", "azure_redis"):
        if ssl_cert_reqs not in _VALID_SSL_CERT_REQS:
            raise ValueError(
                f"Invalid ssl_cert_reqs: {ssl_cert_reqs!r}. "
                f"Must be one of: {', '.join(_VALID_SSL_CERT_REQS)}."
            )
        return (
            f"rediss://{auth}{host}:{port}/{db}"
            f"?ssl_cert_reqs={ssl_cert_reqs}"
        )
    raise ValueError(
        f"Unsupported cache provider for broker URL: {provider!r}. "
        "Must be one of: 'redis', 'elasticache', 'azure_redis'."
    )


def get_cache(provider: str, auth_method: str, **kwargs) -> CacheBackend:
    """Factory to instantiate a cache backend.

    Args:
        provider: ``"redis"``, ``"elasticache"``, or ``"azure_redis"``
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
    """
    if provider == "redis":
        from cloudrift.cache.redis_standalone import StandaloneRedisBackend as _Backend
    elif provider == "elasticache":
        from cloudrift.cache.redis_elasticache import AWSElastiCacheBackend as _Backend
    elif provider == "azure_redis":
        from cloudrift.cache.redis_azure import AzureRedisCacheBackend as _Backend
    else:
        raise ValueError(
            f"Unknown cache provider: {provider!r}. "
            "Choose 'redis', 'elasticache', or 'azure_redis'."
        )

    factory = getattr(_Backend, auth_method, None)
    if factory is None:
        raise ValueError(f"{_Backend.__name__} has no auth method {auth_method!r}.")
    return factory(**kwargs)


__all__ = ["CacheBackend", "get_cache", "cache_broker_url"]
