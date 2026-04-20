import pytest
import fakeredis.aioredis

from cloudrift.cache import get_cache
from cloudrift.cache.redis_standalone import StandaloneRedisBackend


@pytest.fixture
async def cache():
    """StandaloneRedisBackend backed by an in-process fakeredis instance."""
    fake = fakeredis.aioredis.FakeRedis()
    backend = StandaloneRedisBackend(fake)
    yield backend
    await backend.flush()
    await backend.close()


# ---------------------------------------------------------------------------
# get / set / delete / exists
# ---------------------------------------------------------------------------

async def test_set_and_get(cache):
    await cache.set("k1", b"hello")
    assert await cache.get("k1") == b"hello"


async def test_get_missing_returns_none(cache):
    assert await cache.get("nope") is None


async def test_set_with_ttl(cache):
    await cache.set("k_ttl", b"v", ttl=60)
    val = await cache.get("k_ttl")
    assert val == b"v"


async def test_delete(cache):
    await cache.set("del_me", b"x")
    removed = await cache.delete("del_me")
    assert removed == 1
    assert await cache.get("del_me") is None


async def test_delete_multiple(cache):
    await cache.set("a", b"1")
    await cache.set("b", b"2")
    removed = await cache.delete("a", "b", "missing")
    assert removed == 2


async def test_exists(cache):
    assert not await cache.exists("ghost")
    await cache.set("ghost", b"boo")
    assert await cache.exists("ghost")


# ---------------------------------------------------------------------------
# expire / ttl
# ---------------------------------------------------------------------------

async def test_expire_and_ttl(cache):
    await cache.set("ex_key", b"v")
    assert await cache.expire("ex_key", 120)
    remaining = await cache.ttl("ex_key")
    assert 0 < remaining <= 120


async def test_ttl_no_expiry(cache):
    await cache.set("no_exp", b"v")
    assert await cache.ttl("no_exp") == -1


async def test_ttl_missing_key(cache):
    assert await cache.ttl("absent") == -2


# ---------------------------------------------------------------------------
# keys
# ---------------------------------------------------------------------------

async def test_keys_pattern(cache):
    await cache.set("foo:1", b"a")
    await cache.set("foo:2", b"b")
    await cache.set("bar:1", b"c")
    found = await cache.keys("foo:*")
    assert set(found) == {"foo:1", "foo:2"}


# ---------------------------------------------------------------------------
# Hash commands
# ---------------------------------------------------------------------------

async def test_hset_hget(cache):
    result = await cache.hset("myhash", "field1", b"val1")
    assert result == 1  # new field
    assert await cache.hget("myhash", "field1") == b"val1"


async def test_hget_missing_field(cache):
    await cache.hset("h", "f", b"v")
    assert await cache.hget("h", "missing") is None


async def test_hgetall(cache):
    await cache.hset("h2", "a", b"1")
    await cache.hset("h2", "b", b"2")
    all_fields = await cache.hgetall("h2")
    assert all_fields[b"a"] == b"1"
    assert all_fields[b"b"] == b"2"


async def test_hdel(cache):
    await cache.hset("h3", "x", b"1")
    await cache.hset("h3", "y", b"2")
    removed = await cache.hdel("h3", "x", "missing")
    assert removed == 1
    assert await cache.hget("h3", "x") is None


# ---------------------------------------------------------------------------
# List commands
# ---------------------------------------------------------------------------

async def test_lpush_lrange_llen(cache):
    await cache.lpush("mylist", b"c", b"b", b"a")
    assert await cache.llen("mylist") == 3
    items = await cache.lrange("mylist", 0, -1)
    assert items == [b"a", b"b", b"c"]


async def test_rpush(cache):
    await cache.rpush("rlist", b"1", b"2", b"3")
    items = await cache.lrange("rlist", 0, -1)
    assert items == [b"1", b"2", b"3"]


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

async def test_incr(cache):
    await cache.set("counter", b"10")
    val = await cache.incr("counter")
    assert val == 11


async def test_decr(cache):
    await cache.set("counter2", b"5")
    val = await cache.decr("counter2")
    assert val == 4


# ---------------------------------------------------------------------------
# mget / mset
# ---------------------------------------------------------------------------

async def test_mset_mget(cache):
    await cache.mset({"mk1": b"v1", "mk2": b"v2"})
    results = await cache.mget("mk1", "mk2", "mk3")
    assert results[0] == b"v1"
    assert results[1] == b"v2"
    assert results[2] is None


# ---------------------------------------------------------------------------
# setex
# ---------------------------------------------------------------------------

async def test_setex(cache):
    await cache.setex("sk", b"hello", 60)
    assert await cache.get("sk") == b"hello"
    remaining = await cache.ttl("sk")
    assert 0 < remaining <= 60


# ---------------------------------------------------------------------------
# ping / health_check / flush
# ---------------------------------------------------------------------------

async def test_ping(cache):
    assert await cache.ping() is True


async def test_health_check(cache):
    assert await cache.health_check() is True


async def test_flush(cache):
    await cache.set("f1", b"a")
    await cache.set("f2", b"b")
    await cache.flush()
    assert await cache.get("f1") is None
    assert await cache.get("f2") is None


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def test_invalid_provider():
    with pytest.raises(ValueError, match="Unknown cache provider"):
        get_cache("gcp_memorystore", "from_url", url="redis://localhost")
