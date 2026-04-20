from cloudrift.cache.base import CacheBackend


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


__all__ = ["CacheBackend", "get_cache"]
