from cloudrift.cache.base import CacheBackend


class NullCacheBackend(CacheBackend):
    """No-op cache backend for when no real cache is configured.

    Every read returns the "nothing here" value for its type (``None``,
    ``0``, ``False``, an empty collection); every write is accepted and
    silently discarded. This lets callers treat the cache client as always
    present — `if cache: ...` guards become unnecessary, since a missing
    cache and a present-but-empty cache now behave identically from the
    caller's point of view. Compose this with the fail-open pattern used
    throughout the Redis backends: a cache-aside read (get → miss → compute
    → set) still does the right thing when this is the active backend, it
    just never actually caches anything.

    ``ping()``/``health_check()`` return ``True``: "no cache configured" is
    a deliberate deployment choice, not an outage, so it shouldn't trip
    connectivity health checks the way a real unreachable Redis should.

    Every method accepts (and ignores) ``raise_exception`` for signature
    compatibility with the Redis backends — this backend never raises
    regardless, so there's nothing for the flag to change.

    ``pipeline()`` needs no override — the base class's default
    (``_SequentialPipeline``) just replays queued calls against these
    no-op methods one at a time, which is already correct and cheap here.
    """

    async def get(self, key: str, default: bytes | str | None = None, raise_exception: bool | None = None) -> bytes | None:
        return default

    async def set(self, key: str, value: bytes | str, ttl: int | None = None, raise_exception: bool | None = None, **kwargs) -> None:
        return None

    async def delete(self, *keys: str, raise_exception: bool | None = None) -> int:
        return 0

    async def exists(self, key: str, raise_exception: bool | None = None) -> bool:
        return False

    async def expire(
        self, key: str, seconds: int, nx: bool = False, xx: bool = False, raise_exception: bool | None = None
    ) -> bool:
        if nx and xx:
            raise ValueError("expire() flags `nx` and `xx` are mutually exclusive")
        return False

    async def ttl(self, key: str, raise_exception: bool | None = None) -> int:
        return -2  # per CacheBackend.ttl's contract: -2 means the key doesn't exist

    async def keys(self, pattern: str = "*", raise_exception: bool | None = None) -> list[str]:
        return []

    async def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
        raise_exception: bool | None = None,
    ) -> tuple[int, list[bytes]]:
        return 0, []

    async def getdel(self, key: str, raise_exception: bool | None = None) -> bytes | None:
        return None

    async def hget(self, key: str, field: str, raise_exception: bool | None = None) -> bytes | None:
        return None

    async def hset(self, key: str, field: str, value: bytes | str, raise_exception: bool | None = None) -> int:
        return 0

    async def hgetall(self, key: str, raise_exception: bool | None = None) -> dict[bytes, bytes]:
        return {}

    async def hdel(self, key: str, *fields: str, raise_exception: bool | None = None) -> int:
        return 0

    async def sadd(self, key: str, *members: bytes | str, raise_exception: bool | None = None) -> int:
        return 0

    async def srem(self, key: str, *members: bytes | str, raise_exception: bool | None = None) -> int:
        return 0

    async def scard(self, key: str, raise_exception: bool | None = None) -> int:
        return 0

    async def sismember(self, key: str, member: bytes | str, raise_exception: bool | None = None) -> bool:
        return False

    async def smembers(self, key: str, raise_exception: bool | None = None) -> "set[bytes]":
        return set()

    async def sinter(self, *keys: str, raise_exception: bool | None = None) -> "set[bytes]":
        if not keys:
            raise ValueError("sinter() requires at least one key")
        return set()

    async def lpush(self, key: str, *values: bytes | str, raise_exception: bool | None = None) -> int:
        return 0

    async def rpush(self, key: str, *values: bytes | str, raise_exception: bool | None = None) -> int:
        return 0

    async def lrange(self, key: str, start: int, stop: int, raise_exception: bool | None = None) -> list[bytes]:
        return []

    async def llen(self, key: str, raise_exception: bool | None = None) -> int:
        return 0

    async def incr(self, key: str, raise_exception: bool | None = None) -> int:
        return 1  # matches real INCR on a missing key: created at 0, then incremented

    async def decr(self, key: str, raise_exception: bool | None = None) -> int:
        return -1

    async def ping(self, raise_exception: bool | None = None) -> bool:
        return True

    async def flush(self, raise_exception: bool | None = None) -> None:
        return None

    async def close(self) -> None:
        return None

    async def mget(self, *keys: str, raise_exception: bool | None = None) -> list[bytes | None]:
        return [None for _ in keys]

    async def mset(self, mapping: dict[str, bytes | str], raise_exception: bool | None = None) -> None:
        return None
