from abc import ABC, abstractmethod
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from cloudrift.core.exceptions import CacheError


class CacheBackend(ABC):
    """Abstract base class for cloud cache backends."""

    @abstractmethod
    async def get(self, key: str) -> bytes | None:
        """Return the value for *key*, or ``None`` if it does not exist."""

    @abstractmethod
    async def set(self, key: str, value: bytes | str, ttl: int | None = None) -> None:
        """Set *key* to *value*. *ttl* is the expiry in seconds (``None`` = no expiry)."""

    @abstractmethod
    async def delete(self, *keys: str) -> int:
        """Delete one or more keys. Returns the number of keys removed."""

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Return ``True`` if *key* exists."""

    @abstractmethod
    async def expire(self, key: str, seconds: int) -> bool:
        """Set a timeout on *key*. Returns ``True`` if the timeout was set."""

    @abstractmethod
    async def ttl(self, key: str) -> int:
        """Return remaining TTL in seconds. -1 = no expiry, -2 = key missing."""

    @abstractmethod
    async def keys(self, pattern: str = "*") -> list[str]:
        """Return all keys matching *pattern*. Avoid on large keyspaces in production."""

    @abstractmethod
    async def hget(self, key: str, field: str) -> bytes | None:
        """Return the value of *field* in the hash stored at *key*."""

    @abstractmethod
    async def hset(self, key: str, field: str, value: bytes | str) -> int:
        """Set *field* in the hash at *key*. Returns 1 if new, 0 if updated."""

    @abstractmethod
    async def hgetall(self, key: str) -> dict[bytes, bytes]:
        """Return all fields and values of the hash at *key*."""

    @abstractmethod
    async def hdel(self, key: str, *fields: str) -> int:
        """Delete fields from the hash at *key*. Returns number of fields removed."""

    @abstractmethod
    async def lpush(self, key: str, *values: bytes | str) -> int:
        """Prepend values to the list at *key*. Returns new list length."""

    @abstractmethod
    async def rpush(self, key: str, *values: bytes | str) -> int:
        """Append values to the list at *key*. Returns new list length."""

    @abstractmethod
    async def lrange(self, key: str, start: int, stop: int) -> list[bytes]:
        """Return the slice [*start*, *stop*] of the list at *key*."""

    @abstractmethod
    async def llen(self, key: str) -> int:
        """Return the length of the list at *key*."""

    @abstractmethod
    async def incr(self, key: str) -> int:
        """Increment the integer value of *key* by 1. Returns the new value."""

    @abstractmethod
    async def decr(self, key: str) -> int:
        """Decrement the integer value of *key* by 1. Returns the new value."""

    @abstractmethod
    async def ping(self) -> bool:
        """Return ``True`` if the cache server is reachable."""

    @abstractmethod
    async def flush(self) -> None:
        """Flush all keys from the current database. Use with caution."""

    @abstractmethod
    async def close(self) -> None:
        """Close the underlying connection pool."""

    @abstractmethod
    async def mget(self, *keys: str) -> list[bytes | None]:
        """Return values for multiple keys at once."""

    @abstractmethod
    async def mset(self, mapping: dict[str, bytes | str]) -> None:
        """Set multiple key-value pairs at once."""

    async def setex(self, key: str, value: bytes | str, ttl: int) -> None:
        """Atomic set-with-TTL. Default delegates to ``set(key, value, ttl=ttl)``."""
        await self.set(key, value, ttl=ttl)

    async def health_check(self) -> bool:
        """Return True if the cache server is reachable."""
        try:
            return await self.ping()
        except Exception:
            return False

    async def __aenter__(self) -> "CacheBackend":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class _RedisMixin:
    """Concrete Redis implementation shared by all Redis-backed cache backends.

    Subclasses must set ``self._client`` to an ``aioredis.Redis`` instance.
    """

    _client: aioredis.Redis

    async def get(self, key: str) -> bytes | None:
        try:
            return await self._client.get(key)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def set(self, key: str, value: bytes | str, ttl: int | None = None) -> None:
        try:
            await self._client.set(key, value, ex=ttl)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def delete(self, *keys: str) -> int:
        try:
            return await self._client.delete(*keys)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def exists(self, key: str) -> bool:
        try:
            return bool(await self._client.exists(key))
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def expire(self, key: str, seconds: int) -> bool:
        try:
            return bool(await self._client.expire(key, seconds))
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def ttl(self, key: str) -> int:
        try:
            return await self._client.ttl(key)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def keys(self, pattern: str = "*") -> list[str]:
        try:
            result = await self._client.keys(pattern)
            return [k.decode() if isinstance(k, bytes) else k for k in result]
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def hget(self, key: str, field: str) -> bytes | None:
        try:
            return await self._client.hget(key, field)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def hset(self, key: str, field: str, value: bytes | str) -> int:
        try:
            return await self._client.hset(key, field, value)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def hgetall(self, key: str) -> dict[bytes, bytes]:
        try:
            return await self._client.hgetall(key)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def hdel(self, key: str, *fields: str) -> int:
        try:
            return await self._client.hdel(key, *fields)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def lpush(self, key: str, *values: bytes | str) -> int:
        try:
            return await self._client.lpush(key, *values)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def rpush(self, key: str, *values: bytes | str) -> int:
        try:
            return await self._client.rpush(key, *values)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def lrange(self, key: str, start: int, stop: int) -> list[bytes]:
        try:
            return await self._client.lrange(key, start, stop)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def llen(self, key: str) -> int:
        try:
            return await self._client.llen(key)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def incr(self, key: str) -> int:
        try:
            return await self._client.incr(key)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def decr(self, key: str) -> int:
        try:
            return await self._client.decr(key)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def ping(self) -> bool:
        try:
            return await self._client.ping()
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def flush(self) -> None:
        try:
            await self._client.flushdb()
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def close(self) -> None:
        try:
            await self._client.aclose()
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def mget(self, *keys: str) -> list[bytes | None]:
        try:
            return await self._client.mget(*keys)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def mset(self, mapping: dict[str, bytes | str]) -> None:
        try:
            await self._client.mset(mapping)
        except RedisError as e:
            raise CacheError(str(e)) from e

    @asynccontextmanager
    async def pipeline(self):
        """Return a Redis pipeline context manager. Call ``execute()`` on exit."""
        try:
            pipe = self._client.pipeline(transaction=True)
            yield pipe
            await pipe.execute()
        except RedisError as e:
            raise CacheError(str(e)) from e
        finally:
            await pipe.reset()
