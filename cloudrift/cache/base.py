import functools
import json
import logging
import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError, RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from cloudrift.core.exceptions import CacheError

logger = logging.getLogger(__name__)

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
    """Abstract base class for cloud cache backends.

    Every method below takes a ``raise_exception: bool | None = None``
    kwarg. ``None`` (the default) means "use this instance's default",
    set once at construction via ``raise_exception=`` (itself defaulting to
    ``True``) and stored as ``self._raise_exception`` — pass it explicitly
    per call to override that instance default for just that one call.
    ``raise_exception=False`` makes the call fail open: any failure (the
    backend down, a circuit breaker open, corrupted data, anything) returns
    that method's documented "nothing happened" value instead of raising —
    the same value used for a legitimate miss/empty result where one exists
    (e.g. ``get``'s ``default``). Concrete backends implement the behavior;
    this base class just documents the contract.
    """

    def __init__(self, raise_exception: bool = True) -> None:
        self._raise_exception = raise_exception

    def _resolve_raise_exception(self, raise_exception: bool | None) -> bool:
        return self._raise_exception if raise_exception is None else raise_exception

    @abstractmethod
    async def get(
        self, key: str, default: bytes | str | None = None, raise_exception: bool | None = None
    ) -> bytes | None:
        """Return the value for *key*, or *default* if it does not exist (or, with
        raise_exception=False, if the call failed)."""

    @abstractmethod
    async def set(
        self, key: str, value: bytes | str, ttl: int | None = None, raise_exception: bool | None = None, **kwargs
    ) -> None:
        """Set *key* to *value*.

        *ttl* is a convenience expiry-in-seconds kept for backward
        compatibility; it's equivalent to (and overridden by) passing
        ``ex=ttl`` directly. Prefer **kwargs for everything else — most
        commonly ``ex``/``px`` (expiry) and ``nx``/``xx`` (conditional set).
        See redis-py's ``Redis.set`` for the full option set.
        """

    @abstractmethod
    async def delete(self, *keys: str, raise_exception: bool | None = None) -> int:
        """Delete one or more keys. Returns the number of keys removed (0 if none/failed)."""

    @abstractmethod
    async def exists(self, key: str, raise_exception: bool | None = None) -> bool:
        """Return ``True`` if *key* exists."""

    @abstractmethod
    async def expire(
        self,
        key: str,
        seconds: int,
        nx: bool = False,
        xx: bool = False,
        raise_exception: bool | None = None,
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
    async def ttl(self, key: str, raise_exception: bool | None = None) -> int:
        """Return remaining TTL in seconds. -1 = no expiry, -2 = key missing (or, with
        raise_exception=False, the call failed)."""

    @abstractmethod
    async def keys(self, pattern: str = "*", raise_exception: bool | None = None) -> list[str]:
        """Return all keys matching *pattern*. Avoid on large keyspaces in production."""

    @abstractmethod
    async def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        raise_exception: bool | None = None,
    ) -> tuple[int, list[bytes]]:
        """Incremental keyspace iteration.

        Returns ``(next_cursor, keys)``. Iterate until ``next_cursor == 0``.
        Preferred over :meth:`keys` in production: bounded per-call work.
        """

    @abstractmethod
    async def getdel(self, key: str, raise_exception: bool | None = None) -> bytes | None:
        """Atomically get and delete *key*. Returns the value, or ``None``
        if the key did not exist. Requires Redis ≥ 6.2."""

    @abstractmethod
    async def hget(self, key: str, field: str, raise_exception: bool | None = None) -> bytes | None:
        """Return the value of *field* in the hash stored at *key*."""

    @abstractmethod
    async def hset(self, key: str, field: str, value: bytes | str, raise_exception: bool | None = None) -> int:
        """Set *field* in the hash at *key*. Returns 1 if new, 0 if updated."""

    @abstractmethod
    async def hgetall(self, key: str, raise_exception: bool | None = None) -> dict[bytes, bytes]:
        """Return all fields and values of the hash at *key*."""

    @abstractmethod
    async def hdel(self, key: str, *fields: str, raise_exception: bool | None = None) -> int:
        """Delete fields from the hash at *key*. Returns number of fields removed."""

    @abstractmethod
    async def sadd(self, key: str, *members: bytes | str, raise_exception: bool | None = None) -> int:
        """Add one or more *members* to the set at *key*.

        Returns the number of members that were newly added (i.e. not already
        present). This "was-new" signal is the foundation of unique-element
        deduplication patterns (e.g. DAU/MAU tracking).
        """

    @abstractmethod
    async def srem(self, key: str, *members: bytes | str, raise_exception: bool | None = None) -> int:
        """Remove one or more *members* from the set at *key*. Returns the number removed."""

    @abstractmethod
    async def scard(self, key: str, raise_exception: bool | None = None) -> int:
        """Return the number of elements in the set at *key*."""

    @abstractmethod
    async def sismember(self, key: str, member: bytes | str, raise_exception: bool | None = None) -> bool:
        """Return ``True`` if *member* is in the set at *key*."""

    @abstractmethod
    async def smembers(self, key: str, raise_exception: bool | None = None) -> "set[bytes]":
        """Return all members of the set at *key*."""

    @abstractmethod
    async def sinter(self, *keys: str, raise_exception: bool | None = None) -> "set[bytes]":
        """Return the members common to all sets at *keys* (set intersection).

        With a single key this is equivalent to :meth:`smembers`. A missing key
        is treated as an empty set, so any missing key yields an empty result.
        """

    @abstractmethod
    async def lpush(self, key: str, *values: bytes | str, raise_exception: bool | None = None) -> int:
        """Prepend values to the list at *key*. Returns new list length."""

    @abstractmethod
    async def rpush(self, key: str, *values: bytes | str, raise_exception: bool | None = None) -> int:
        """Append values to the list at *key*. Returns new list length."""

    @abstractmethod
    async def lrange(self, key: str, start: int, stop: int, raise_exception: bool | None = None) -> list[bytes]:
        """Return the slice [*start*, *stop*] of the list at *key*."""

    @abstractmethod
    async def llen(self, key: str, raise_exception: bool | None = None) -> int:
        """Return the length of the list at *key*."""

    @abstractmethod
    async def incr(self, key: str, raise_exception: bool | None = None) -> int:
        """Increment the integer value of *key* by 1. Returns the new value."""

    @abstractmethod
    async def decr(self, key: str, raise_exception: bool | None = None) -> int:
        """Decrement the integer value of *key* by 1. Returns the new value."""

    @abstractmethod
    async def ping(self, raise_exception: bool | None = None) -> bool:
        """Return ``True`` if the cache server is reachable."""

    @abstractmethod
    async def flush(self, raise_exception: bool | None = None) -> None:
        """Flush all keys from the current database. Use with caution."""

    @abstractmethod
    async def close(self) -> None:
        """Close the underlying connection pool."""

    @abstractmethod
    async def mget(self, *keys: str, raise_exception: bool | None = None) -> list[bytes | None]:
        """Return values for multiple keys at once."""

    @abstractmethod
    async def mset(self, mapping: dict[str, bytes | str], raise_exception: bool | None = None) -> None:
        """Set multiple key-value pairs at once."""

    async def setex(self, key: str, value: bytes | str, ttl: int, raise_exception: bool | None = None) -> None:
        """Atomic set-with-TTL. Default delegates to ``set(key, value, ex=ttl)``."""
        await self.set(key, value, ex=ttl, raise_exception=raise_exception)

    async def get_json(self, key: str, default=None, raise_exception: bool | None = None):
        """GET *key* and ``json.loads()`` the result.

        Returns *default* on a miss. A malformed cached value is treated as
        just another cache failure — same raise_exception contract as every
        other method: raises (wrapped as ``CacheError``) by default, or
        returns *default* with raise_exception=False, so callers get one
        consistent fail-open story for "no data" vs. "corrupt data" instead
        of having to json.loads() and catch decode errors themselves.
        """
        value = await self.get(key, raise_exception=raise_exception)
        if value is None:
            return default
        try:
            return json.loads(value)
        except (TypeError, ValueError) as e:
            logger.warning("Corrupt JSON for key %s: %s", key, e)
            if not self._resolve_raise_exception(raise_exception):
                return default
            raise CacheError(f"Corrupt JSON for key {key!r}: {e}") from e

    async def set_json(
        self, key: str, value, ttl: int | None = None, raise_exception: bool | None = None, **kwargs
    ) -> None:
        """``json.dumps()`` *value* (non-serializable types stringified via
        ``default=str``) and SET it at *key*. Mirrors :meth:`get_json`."""
        try:
            payload = json.dumps(value, default=str)
        except (TypeError, ValueError) as e:
            logger.warning("Could not JSON-encode value for key %s: %s", key, e)
            if not self._resolve_raise_exception(raise_exception):
                return
            raise CacheError(f"Could not JSON-encode value for key {key!r}: {e}") from e
        await self.set(key, payload, ttl=ttl, raise_exception=raise_exception, **kwargs)

    @asynccontextmanager
    async def pipeline(self, raise_exception: bool | None = None):
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
        pipe = _SequentialPipeline(self, raise_exception=raise_exception)
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

    def __init__(self, backend: "CacheBackend", raise_exception: bool | None = None) -> None:
        self._backend = backend
        self._raise_exception = raise_exception
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
            kwargs.setdefault("raise_exception", self._raise_exception)
            results.append(await method(*args, **kwargs))
        return results


class _RedisMixin:
    """Concrete Redis implementation shared by all Redis-backed cache backends.

    Subclasses must set ``self._client`` to an ``aioredis.Redis`` instance.

    Every data-plane call (except ``close``) is routed through ``_guarded``,
    which layers a minimal circuit breaker on top of the usual
    ``RedisError`` -> ``CacheError`` translation. Without it, a real Redis
    outage means every caller pays the client's full retry budget
    (``resilient_client_kwargs`` — socket timeout plus ~0.7s of backoff) on
    every single call before it raises; under load, enough concurrent
    callers blocked that long can exhaust worker/connection capacity on
    their own, independent of Redis itself. Once ``_BREAKER_FAILURE_THRESHOLD``
    calls in a row fail, further calls raise ``CacheError`` immediately for
    ``_BREAKER_RECOVERY_SECONDS`` instead of retrying a connection already
    known to be down. The first call after that window elapses is a live
    trial: success closes the breaker, failure reopens it for another
    window — a plain two-state (closed/open) breaker, no separate
    half-open state, which is enough to stop paying the timeout tax while
    Redis is down. Breaker state lives per-instance (one breaker per
    backend/connection pool), lazily attached so subclasses don't need to
    call a mixin ``__init__``.
    """

    _client: aioredis.Redis

    _BREAKER_FAILURE_THRESHOLD = 5
    _BREAKER_RECOVERY_SECONDS = 30.0

    def _breaker_state(self) -> dict:
        # Lazy, per-instance state via __dict__ instead of __init__ — subclasses
        # never call a mixin __init__, so this is the only safe place to set it up.
        state = self.__dict__.get("_breaker")
        if state is None:
            state = {"failures": 0, "open_until": 0.0}
            self.__dict__["_breaker"] = state
        return state

    def _breaker_open(self) -> bool:
        # open_until == 0 means "never tripped"; otherwise open until that timestamp.
        state = self._breaker_state()
        return state["open_until"] > 0.0 and time.monotonic() < state["open_until"]

    @staticmethod
    def _describe(fn) -> str:
        """Render fn (a bound method or functools.partial of one) as 'NAME(args, kw=kw)' for logging.

        No separate op/args bookkeeping at each call site: a partial already
        carries its target's name plus the exact arguments it was called
        with, so there's nothing to duplicate or let drift out of sync.
        """
        if isinstance(fn, functools.partial):
            name, args, kwargs = fn.func.__name__, fn.args, fn.keywords or {}
        else:
            name, args, kwargs = getattr(fn, "__name__", repr(fn)), (), {}
        parts = [repr(a) for a in args] + [f"{k}={v!r}" for k, v in kwargs.items()]
        return f"{name.upper()}({', '.join(parts)})"

    async def _guarded(self, fn, raise_exception: bool | None = None):
        """Run fn() with circuit-breaker protection and RedisError -> CacheError translation.

        `fn` is a niladic callable — a bound method (``self._client.ping``)
        or a ``functools.partial`` of one (``functools.partial(self._client.get, key)``)
        — both so it can be called with no arguments here and so `_describe`
        can log exactly what was attempted on failure.

        raise_exception=None (default) inherits this instance's default
        (set at construction, see CacheBackend.__init__). Resolved False
        makes this call fail open, returning ``None`` instead of raising on
        any failure (breaker open, a RedisError, or anything else fn()
        raises). ``None`` is only a placeholder here — this method knows
        nothing about what a given command's "nothing happened" value
        should look like (0? False? an empty set?); each public method
        below is responsible for turning this ``None`` into its own
        documented default. Only RedisError counts against the breaker — a
        non-Redis failure (e.g. a decode error on corrupted stored data)
        doesn't mean Redis itself is unhealthy.
        """
        raise_exception = self._resolve_raise_exception(raise_exception)
        if self._breaker_open():
            if not raise_exception:
                return None
            # Fail fast without touching the client — no point retrying a
            # connection already known to be down.
            raise CacheError("Redis circuit breaker open — skipping call, Redis has failed repeatedly")
        state = self._breaker_state()
        try:
            result = await fn()
        except RedisError as e:
            state["failures"] += 1
            # `not self._breaker_open()` keeps this a one-time trip: once open, further
            # failures don't keep pushing open_until back out.
            if state["failures"] >= self._BREAKER_FAILURE_THRESHOLD and not self._breaker_open():
                state["open_until"] = time.monotonic() + self._BREAKER_RECOVERY_SECONDS
            logger.warning("Redis %s failed: %s", self._describe(fn), e)
            if not raise_exception:
                return None
            raise CacheError(str(e)) from e
        except Exception as e:
            logger.warning("Cache %s failed (non-Redis error): %s", self._describe(fn), e)
            if not raise_exception:
                return None
            raise
        else:
            # Any success resets the count — closes the breaker if it was open
            # (this is the "half-open trial" call) and forgives past failures otherwise.
            state["failures"] = 0
            state["open_until"] = 0.0
            return result

    async def get(
        self, key: str, default: bytes | str | None = None, raise_exception: bool | None = None
    ) -> bytes | None:
        value = await self._guarded(functools.partial(self._client.get, key), raise_exception=raise_exception)
        return value if value is not None else default

    async def set(
        self, key: str, value: bytes | str, ttl: int | None = None, raise_exception: bool | None = None, **kwargs
    ) -> None:
        # Backward-compat: ttl= is the pre-**kwargs calling convention. An
        # explicit ex= in kwargs (the redis-py-native spelling) wins if both
        # are somehow passed.
        if ttl is not None:
            kwargs.setdefault("ex", ttl)
        await self._guarded(
            functools.partial(self._client.set, key, value, **kwargs), raise_exception=raise_exception
        )

    async def delete(self, *keys: str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(functools.partial(self._client.delete, *keys), raise_exception=raise_exception)
        return result if result is not None else 0

    async def exists(self, key: str, raise_exception: bool | None = None) -> bool:
        # bool(None) is already False, exists()'s own fail-open value — no
        # separate None check needed.
        return bool(await self._guarded(functools.partial(self._client.exists, key), raise_exception=raise_exception))

    async def expire(
        self,
        key: str,
        seconds: int,
        nx: bool = False,
        xx: bool = False,
        raise_exception: bool | None = None,
    ) -> bool:
        if nx and xx:
            raise ValueError("expire() flags `nx` and `xx` are mutually exclusive")
        return bool(await self._guarded(
            functools.partial(self._client.expire, key, seconds, nx=nx, xx=xx), raise_exception=raise_exception
        ))

    async def sadd(self, key: str, *members: bytes | str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(
            functools.partial(self._client.sadd, key, *members), raise_exception=raise_exception
        )
        return result if result is not None else 0

    async def srem(self, key: str, *members: bytes | str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(
            functools.partial(self._client.srem, key, *members), raise_exception=raise_exception
        )
        return result if result is not None else 0

    async def scard(self, key: str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(functools.partial(self._client.scard, key), raise_exception=raise_exception)
        return result if result is not None else 0

    async def sismember(self, key: str, member: bytes | str, raise_exception: bool | None = None) -> bool:
        return bool(await self._guarded(
            functools.partial(self._client.sismember, key, member), raise_exception=raise_exception
        ))

    async def smembers(self, key: str, raise_exception: bool | None = None) -> "set[bytes]":
        result = await self._guarded(functools.partial(self._client.smembers, key), raise_exception=raise_exception)
        return result if result is not None else set()

    async def sinter(self, *keys: str, raise_exception: bool | None = None) -> "set[bytes]":
        if not keys:
            raise ValueError("sinter() requires at least one key")
        result = await self._guarded(functools.partial(self._client.sinter, *keys), raise_exception=raise_exception)
        return set(result) if result is not None else set()

    async def ttl(self, key: str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(functools.partial(self._client.ttl, key), raise_exception=raise_exception)
        # -2 is the contract's own "key doesn't exist" value — a fitting
        # fail-open default too.
        return result if result is not None else -2

    async def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        raise_exception: bool | None = None,
    ) -> tuple[int, list[bytes]]:
        kwargs: dict = {}
        if match is not None:
            kwargs["match"] = match
        if count is not None:
            kwargs["count"] = count
        result = await self._guarded(
            functools.partial(self._client.scan, cursor=cursor, **kwargs), raise_exception=raise_exception
        )
        if result is None:
            return 0, []
        next_cursor, found = result
        return int(next_cursor), found

    async def getdel(self, key: str, raise_exception: bool | None = None) -> bytes | None:
        return await self._guarded(functools.partial(self._client.getdel, key), raise_exception=raise_exception)

    async def keys(self, pattern: str = "*", raise_exception: bool | None = None) -> list[str]:
        result = await self._guarded(functools.partial(self._client.keys, pattern), raise_exception=raise_exception)
        if result is None:
            return []
        return [k.decode() if isinstance(k, bytes) else k for k in result]

    async def hget(self, key: str, field: str, raise_exception: bool | None = None) -> bytes | None:
        return await self._guarded(
            functools.partial(self._client.hget, key, field), raise_exception=raise_exception
        )

    async def hset(self, key: str, field: str, value: bytes | str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(
            functools.partial(self._client.hset, key, field, value), raise_exception=raise_exception
        )
        return result if result is not None else 0

    async def hgetall(self, key: str, raise_exception: bool | None = None) -> dict[bytes, bytes]:
        result = await self._guarded(functools.partial(self._client.hgetall, key), raise_exception=raise_exception)
        return result if result is not None else {}

    async def hdel(self, key: str, *fields: str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(
            functools.partial(self._client.hdel, key, *fields), raise_exception=raise_exception
        )
        return result if result is not None else 0

    async def lpush(self, key: str, *values: bytes | str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(
            functools.partial(self._client.lpush, key, *values), raise_exception=raise_exception
        )
        return result if result is not None else 0

    async def rpush(self, key: str, *values: bytes | str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(
            functools.partial(self._client.rpush, key, *values), raise_exception=raise_exception
        )
        return result if result is not None else 0

    async def lrange(self, key: str, start: int, stop: int, raise_exception: bool | None = None) -> list[bytes]:
        result = await self._guarded(
            functools.partial(self._client.lrange, key, start, stop), raise_exception=raise_exception
        )
        return result if result is not None else []

    async def llen(self, key: str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(functools.partial(self._client.llen, key), raise_exception=raise_exception)
        return result if result is not None else 0

    async def incr(self, key: str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(functools.partial(self._client.incr, key), raise_exception=raise_exception)
        return result if result is not None else 0

    async def decr(self, key: str, raise_exception: bool | None = None) -> int:
        result = await self._guarded(functools.partial(self._client.decr, key), raise_exception=raise_exception)
        return result if result is not None else 0

    async def ping(self, raise_exception: bool | None = None) -> bool:
        # bool(None) is already False — a failed ping correctly reports unreachable.
        return bool(await self._guarded(self._client.ping, raise_exception=raise_exception))

    async def flush(self, raise_exception: bool | None = None) -> None:
        await self._guarded(self._client.flushdb, raise_exception=raise_exception)

    async def close(self) -> None:
        # Deliberately not routed through _guarded: shutdown must be able to
        # release the connection pool even while the breaker is open. No
        # raise_exception param — callers always need to know if cleanup failed.
        try:
            await self._client.aclose()
        except RedisError as e:
            raise CacheError(str(e)) from e

    async def mget(self, *keys: str, raise_exception: bool | None = None) -> list[bytes | None]:
        result = await self._guarded(functools.partial(self._client.mget, *keys), raise_exception=raise_exception)
        return result if result is not None else [None for _ in keys]

    async def mset(self, mapping: dict[str, bytes | str], raise_exception: bool | None = None) -> None:
        await self._guarded(functools.partial(self._client.mset, mapping), raise_exception=raise_exception)

    @asynccontextmanager
    async def pipeline(self, raise_exception: bool | None = None):
        """Return a Redis pipeline context manager. Call ``execute()`` on exit.

        Queuing commands on the pipeline (``pipe.set(...)`` etc.) is local
        and never touches the network, so only ``execute()`` — the actual
        round trip — goes through the breaker. raise_exception=False makes a
        failed execute() a silent no-op instead of raising.
        """
        pipe = self._client.pipeline(transaction=True)
        try:
            yield pipe
            await self._guarded(pipe.execute, raise_exception=raise_exception)
        finally:
            await pipe.reset()
