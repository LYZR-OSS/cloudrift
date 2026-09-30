import asyncio
import contextlib
import secrets
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError, RedisError, WatchError
from redis.exceptions import TimeoutError as RedisTimeoutError

from cloudrift.core.exceptions import CacheError, CacheLockError

# Namespace prefix for lock keys, so `lock("orders")` can never collide with a
# plain cache key named "orders".
_LOCK_KEY_PREFIX = "cloudrift:lock:"


@dataclass
class Lock:
    """Handle to a held distributed lock, returned by :meth:`CacheBackend.lock`.

    ``token`` is the fencing token proving ownership — required by
    :meth:`CacheBackend.extend_lock` / :meth:`CacheBackend.release_lock` so a
    caller can only act on a lock it actually still holds, never one that
    expired and was re-acquired by someone else in the meantime.
    """

    key: str
    token: str
    ttl: float
    _extend_task: "asyncio.Task | None" = field(default=None, repr=False, compare=False)


# Defaults for every Redis-backed cache client.
#
# redis-py ships with `health_check_interval=0` and `Retry(NoBackoff(), 0)` —
# i.e. no health checks and no retries. Managed Redis (Azure Cache for Redis,
# ElastiCache) reaps idle connections and drops every connection during
# maintenance, failover, and scale operations, so those defaults surface
# `ConnectionError: Connection closed by server.` to the caller on the first
# command after the server hangs up. These values make a pooled client ride
# through a sub-second disruption instead. Override per call site by passing the
# same keyword to any factory.
#
# `ReadOnlyError` is in the retry set for Azure Cache for Redis specifically:
# failover promotes the replica, and a pooled connection can still be pointed at
# the node that just became read-only, which answers writes with `-READONLY`.
# redis-py raises that as a `ResponseError` subclass, not a `ConnectionError`,
# so it is only retried if listed. Retrying disconnects and reconnects, which
# re-resolves DNS to the newly promoted primary.
DEFAULT_HEALTH_CHECK_INTERVAL = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_SOCKET_TIMEOUT = 5.0
DEFAULT_SOCKET_CONNECT_TIMEOUT = 5.0
DEFAULT_MAX_CONNECTIONS = 100


def resilient_client_kwargs(**overrides) -> dict:
    """Return `aioredis.Redis` kwargs that survive managed-Redis disruptions.

    Backoff is 0.1s, 0.2s, 0.4s — roughly 0.7s of retry budget per command,
    enough for a fast failover without stalling a request behind a hard outage.
    `Retry` is deep-copied per connection by redis-py, so sharing one instance
    across a pool is safe.

    The cache API exposes no blocking commands, so a socket read timeout cannot
    cut short a legitimately long call.
    """
    kwargs = {
        "retry": Retry(ExponentialBackoff(cap=1.0, base=0.1), DEFAULT_MAX_RETRIES),
        "retry_on_error": [RedisConnectionError, RedisTimeoutError, ReadOnlyError],
        "health_check_interval": DEFAULT_HEALTH_CHECK_INTERVAL,
        "socket_keepalive": True,
        "socket_timeout": DEFAULT_SOCKET_TIMEOUT,
        "socket_connect_timeout": DEFAULT_SOCKET_CONNECT_TIMEOUT,
        "max_connections": DEFAULT_MAX_CONNECTIONS,
    }
    kwargs.update(overrides)
    return kwargs


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
    async def expire(
        self,
        key: str,
        seconds: int,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Set a timeout on *key*. Returns ``True`` if the timeout was set.

        Args:
            key: Target key.
            seconds: TTL in seconds.
            nx: Only set the TTL if the key has no existing TTL.
            xx: Only set the TTL if the key already has a TTL.

        ``nx`` and ``xx`` are mutually exclusive. Backends that don't support
        these flags natively should emulate them via ``ttl(key)`` and document
        that the operation is not atomic.
        """

    @abstractmethod
    async def ttl(self, key: str) -> int:
        """Return remaining TTL in seconds. -1 = no expiry, -2 = key missing."""

    @abstractmethod
    async def keys(self, pattern: str = "*") -> list[str]:
        """Return all keys matching *pattern*. Avoid on large keyspaces in production."""

    @abstractmethod
    async def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, list[bytes]]:
        """Incremental keyspace iteration.

        Returns ``(next_cursor, keys)``. Iterate until ``next_cursor == 0``.
        Preferred over :meth:`keys` in production: bounded per-call work.
        """

    @abstractmethod
    async def getdel(self, key: str) -> bytes | None:
        """Atomically get and delete *key*. Returns the value, or ``None``
        if the key did not exist. Requires Redis ≥ 6.2."""

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
    async def sadd(self, key: str, *members: bytes | str) -> int:
        """Add one or more *members* to the set at *key*.

        Returns the number of members that were newly added (i.e. not already
        present). This "was-new" signal is the foundation of unique-element
        deduplication patterns (e.g. DAU/MAU tracking).
        """

    @abstractmethod
    async def srem(self, key: str, *members: bytes | str) -> int:
        """Remove one or more *members* from the set at *key*. Returns the number removed."""

    @abstractmethod
    async def scard(self, key: str) -> int:
        """Return the number of elements in the set at *key*."""

    @abstractmethod
    async def sismember(self, key: str, member: bytes | str) -> bool:
        """Return ``True`` if *member* is in the set at *key*."""

    @abstractmethod
    async def smembers(self, key: str) -> "set[bytes]":
        """Return all members of the set at *key*."""

    @abstractmethod
    async def sinter(self, *keys: str) -> "set[bytes]":
        """Return the members common to all sets at *keys* (set intersection).

        With a single key this is equivalent to :meth:`smembers`. A missing key
        is treated as an empty set, so any missing key yields an empty result.
        """

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

    @asynccontextmanager
    async def lock(
        self,
        key: str,
        ttl: float = 10.0,
        *,
        blocking_timeout: float | None = 10.0,
        retry_interval: float = 0.1,
        auto_extend: bool = True,
    ) -> "AsyncIterator[Lock]":
        """Acquire a distributed lock scoped to *key*, released on context exit.

        Usage::

            async with cache.lock("invoice:42:close"):
                ...  # only one process/replica runs this at a time

        Backed by ``SET key token NX PX ttl`` plus a random *fencing token* —
        never a bare ``DEL`` — so a caller can only release or extend the lock
        it actually holds. A slow holder that outlives ``ttl`` cannot delete a
        lock some other process has since acquired for the same key, which is
        the classic bug naive "set-then-delete" locks have.

        Args:
            key: Lock name. Namespaced internally so it can't collide with a
                regular cache key of the same name.
            ttl: Seconds the lock is held for before it expires unclaimed.
                Must outlast the critical section, or set ``auto_extend=True``
                (the default) to have it refreshed automatically in the
                background for as long as the ``async with`` block runs.
            blocking_timeout: Max seconds to wait for the lock before giving up.
                ``0`` or ``None`` means try once and fail immediately if held.
            retry_interval: Seconds between acquisition attempts while blocking.
            auto_extend: Refresh the TTL from a background task at roughly
                ``ttl / 3`` while the lock is held, so a critical section
                running longer than expected doesn't have its lock silently
                expire and get acquired by someone else mid-operation. The
                watchdog stops as soon as the block exits; it never keeps a
                lock alive past that.

        Raises:
            CacheLockError: if the lock could not be acquired within
                ``blocking_timeout``.

        Backends that don't support atomic conditional writes should not
        override this — the default raises ``NotImplementedError``.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support lock()")

    async def extend_lock(self, lock: "Lock", ttl: float = 10.0) -> bool:
        """Refresh a held lock's TTL. Returns ``False`` if *lock* is no longer held.

        Rarely needed directly — :meth:`lock` auto-extends by default. Useful
        for callers managing a lock's lifetime manually instead of via the
        ``async with`` block.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support extend_lock()")

    async def release_lock(self, lock: "Lock") -> bool:
        """Release a held lock. Returns ``False`` if *lock* was already lost
        (expired, or released/stolen by another caller) — safe to call in a
        ``finally`` without first checking ownership."""
        raise NotImplementedError(f"{type(self).__name__} does not support release_lock()")

    @asynccontextmanager
    async def pipeline(self):
        """Batch multiple commands.

        Usage:
            async with cache.pipeline() as pipe:
                pipe.sadd("k", "m")
                pipe.expire("k", 60)
            # commands execute on context exit

        The default implementation queues calls and replays them sequentially
        on exit — it provides no atomicity and no round-trip savings. Redis
        backends override this with a true server-side pipeline. Callers that
        depend on atomicity or batching performance must check the backend.
        """
        pipe = _SequentialPipeline(self)
        try:
            yield pipe
        finally:
            await pipe.execute()

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


class _SequentialPipeline:
    """Default pipeline that records calls and replays them sequentially on execute.

    Provides no atomicity and no round-trip savings — exists so the
    ``pipeline()`` API works on every backend. Redis backends bypass this with
    a true server-side pipeline.
    """

    def __init__(self, backend: "CacheBackend") -> None:
        self._backend = backend
        self._ops: list[tuple[str, tuple, dict]] = []

    def __getattr__(self, name: str):
        def queue(*args, **kwargs):
            self._ops.append((name, args, kwargs))
            return self

        return queue

    async def execute(self) -> list:
        results = []
        ops, self._ops = self._ops, []
        for name, args, kwargs in ops:
            method = getattr(self._backend, name)
            results.append(await method(*args, **kwargs))
        return results


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

    async def expire(
        self,
        key: str,
        seconds: int,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        if nx and xx:
            raise ValueError("expire() flags `nx` and `xx` are mutually exclusive")
        try:
            return bool(await self._client.expire(key, seconds, nx=nx, xx=xx))
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def sadd(self, key: str, *members: bytes | str) -> int:
        try:
            return await self._client.sadd(key, *members)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def srem(self, key: str, *members: bytes | str) -> int:
        try:
            return await self._client.srem(key, *members)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def scard(self, key: str) -> int:
        try:
            return await self._client.scard(key)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def sismember(self, key: str, member: bytes | str) -> bool:
        try:
            return bool(await self._client.sismember(key, member))
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def smembers(self, key: str) -> "set[bytes]":
        try:
            return await self._client.smembers(key)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def sinter(self, *keys: str) -> "set[bytes]":
        if not keys:
            raise ValueError("sinter() requires at least one key")
        try:
            return set(await self._client.sinter(*keys))
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def ttl(self, key: str) -> int:
        try:
            return await self._client.ttl(key)
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, list[bytes]]:
        kwargs: dict = {}
        if match is not None:
            kwargs["match"] = match
        if count is not None:
            kwargs["count"] = count
        try:
            next_cursor, keys = await self._client.scan(cursor=cursor, **kwargs)
            return int(next_cursor), keys
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def getdel(self, key: str) -> bytes | None:
        try:
            return await self._client.getdel(key)
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

    async def _cas_key(self, redis_key: str, token: str, then) -> bool:
        """WATCH ``redis_key``; if its value still equals ``token``, run ``then``
        on a MULTI pipeline and EXEC it. Returns whether the mutation happened.

        This is the compare-and-mutate primitive both :meth:`release_lock` and
        :meth:`extend_lock` are built on. It's expressed with WATCH/MULTI rather
        than a Lua script (``EVAL``) so it works unmodified against any
        Redis-protocol server that restricts scripting (some managed offerings
        do) and against fakeredis in tests.
        """
        try:
            async with self._client.pipeline(transaction=True) as pipe:
                await pipe.watch(redis_key)
                current = await pipe.get(redis_key)
                if current is None or current.decode() != token:
                    await pipe.reset()
                    return False
                pipe.multi()
                then(pipe)
                await pipe.execute()
                return True
        except WatchError:
            # Someone else mutated the key between WATCH and EXEC — by
            # construction that can only mean our lock already expired and was
            # re-acquired, so treat it the same as "no longer held".
            return False
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def release_lock(self, lock: "Lock") -> bool:
        task, lock._extend_task = lock._extend_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        redis_key = _LOCK_KEY_PREFIX + lock.key
        return await self._cas_key(redis_key, lock.token, lambda pipe: pipe.delete(redis_key))

    async def extend_lock(self, lock: "Lock", ttl: float = 10.0) -> bool:
        redis_key = _LOCK_KEY_PREFIX + lock.key
        extended = await self._cas_key(
            redis_key, lock.token, lambda pipe: pipe.pexpire(redis_key, int(ttl * 1000))
        )
        if extended:
            lock.ttl = ttl
        return extended

    async def _watchdog(self, lock: "Lock") -> None:
        """Background task: refresh *lock*'s TTL at roughly a third of its
        length for as long as it's held. Cancelled by :meth:`release_lock`."""
        while True:
            await asyncio.sleep(lock.ttl / 3)
            if not await self.extend_lock(lock, lock.ttl):
                return

    @asynccontextmanager
    async def lock(
        self,
        key: str,
        ttl: float = 10.0,
        *,
        blocking_timeout: float | None = 10.0,
        retry_interval: float = 0.1,
        auto_extend: bool = True,
    ) -> "AsyncIterator[Lock]":
        redis_key = _LOCK_KEY_PREFIX + key
        token = secrets.token_hex(16)
        loop = asyncio.get_running_loop()
        deadline = None if blocking_timeout is None else loop.time() + blocking_timeout
        try:
            while True:
                acquired = await self._client.set(redis_key, token, nx=True, px=int(ttl * 1000))
                if acquired:
                    break
                if deadline is not None and loop.time() >= deadline:
                    raise CacheLockError(
                        f"Could not acquire lock {key!r} within {blocking_timeout}s"
                    )
                await asyncio.sleep(retry_interval)
        except RedisError as e:
            raise CacheError(str(e)) from e

        held = Lock(key=key, token=token, ttl=ttl)
        if auto_extend:
            held._extend_task = asyncio.ensure_future(self._watchdog(held))
        try:
            yield held
        finally:
            await self.release_lock(held)

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
