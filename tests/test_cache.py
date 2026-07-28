import inspect

import pytest
import fakeredis.aioredis

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError

from cloudrift.cache import get_cache, resilient_client_kwargs
from cloudrift.cache.redis_azure import AzureRedisCacheBackend
from cloudrift.cache.redis_elasticache import AWSElastiCacheBackend
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
# Set commands
# ---------------------------------------------------------------------------

async def test_sadd_returns_newly_added(cache):
    assert await cache.sadd("s", b"a", b"b") == 2
    # adding an existing member does not count as new
    assert await cache.sadd("s", b"a", b"c") == 1


async def test_smembers_and_scard(cache):
    await cache.sadd("s2", b"x", b"y", b"z")
    assert await cache.smembers("s2") == {b"x", b"y", b"z"}
    assert await cache.scard("s2") == 3


async def test_srem_and_sismember(cache):
    await cache.sadd("s3", b"a", b"b")
    assert await cache.sismember("s3", b"a")
    assert await cache.srem("s3", b"a") == 1
    assert not await cache.sismember("s3", b"a")


async def test_sinter(cache):
    await cache.sadd("set_a", b"1", b"2", b"3")
    await cache.sadd("set_b", b"2", b"3", b"4")
    await cache.sadd("set_c", b"3", b"4", b"5")
    assert await cache.sinter("set_a", "set_b") == {b"2", b"3"}
    assert await cache.sinter("set_a", "set_b", "set_c") == {b"3"}


async def test_sinter_single_key_equals_smembers(cache):
    await cache.sadd("only", b"a", b"b")
    assert await cache.sinter("only") == await cache.smembers("only")


async def test_sinter_missing_key_is_empty(cache):
    await cache.sadd("present", b"a", b"b")
    assert await cache.sinter("present", "absent") == set()


async def test_sinter_no_keys_raises(cache):
    with pytest.raises(ValueError, match="at least one key"):
        await cache.sinter()


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


# ---------------------------------------------------------------------------
# Connection resilience
# ---------------------------------------------------------------------------
#
# redis-py defaults to health_check_interval=0 and Retry(NoBackoff(), 0), so a
# managed Redis that reaps idle connections or fails over surfaces
# "Connection closed by server." to the caller. Every factory must opt out of
# those defaults.
#
# _FACTORIES covers the auth methods constructible without cloud credentials.
# test_every_factory_accepts_client_kwargs covers the rest by signature, so a
# newly added factory cannot silently skip the resilience settings.

_FACTORIES = [
    ("redis", "from_url", {"url": "redis://localhost:6379/0"}),
    ("redis", "from_credentials", {"host": "localhost"}),
    ("redis", "from_tls_cert", {"host": "localhost"}),
    ("elasticache", "from_auth_token", {"host": "ec.example.com", "auth_token": "t"}),
    ("elasticache", "from_tls_cert", {"host": "ec.example.com"}),
    ("azure_redis", "from_access_key", {"host": "az.example.com", "access_key": "k"}),
]


@pytest.mark.parametrize("provider,auth_method,kwargs", _FACTORIES)
def test_factory_sets_connection_resilience(provider, auth_method, kwargs):
    backend = get_cache(provider, auth_method, **kwargs)
    pool = backend._client.connection_pool
    ckw = pool.connection_kwargs

    assert ckw["health_check_interval"] == 30
    assert ckw["socket_keepalive"] is True
    assert ckw["socket_timeout"] == 5.0
    assert ckw["socket_connect_timeout"] == 5.0
    assert pool.max_connections == 100

    # A retry policy with real attempts, covering connection loss and timeouts.
    assert ckw["retry"].get_retries() == 3
    assert RedisConnectionError in ckw["retry_on_error"]
    assert RedisTimeoutError in ckw["retry_on_error"]
    # Azure failover answers writes with -READONLY from the demoted primary.
    # It is a ResponseError, not a ConnectionError, so it must be listed
    # explicitly or the caller sees the error instead of a reconnect.
    assert ReadOnlyError in ckw["retry_on_error"]


@pytest.mark.parametrize("provider,auth_method,kwargs", _FACTORIES)
def test_factory_resilience_is_overridable(provider, auth_method, kwargs):
    backend = get_cache(
        provider, auth_method, health_check_interval=7, max_connections=5, **kwargs
    )
    pool = backend._client.connection_pool
    assert pool.connection_kwargs["health_check_interval"] == 7
    assert pool.max_connections == 5


def test_resilient_client_kwargs_defaults_and_overrides():
    defaults = resilient_client_kwargs()
    assert defaults["health_check_interval"] == 30
    assert defaults["max_connections"] == 100

    overridden = resilient_client_kwargs(socket_timeout=0.5, decode_responses=True)
    assert overridden["socket_timeout"] == 0.5
    assert overridden["decode_responses"] is True
    # Untouched defaults survive an override.
    assert overridden["health_check_interval"] == 30


def test_connection_built_from_pool_carries_resilience():
    """The settings must reach the Connection object, not just the kwargs dict."""
    backend = get_cache("redis", "from_credentials", host="localhost")
    conn = backend._client.connection_pool.make_connection()
    assert conn.health_check_interval == 30
    assert conn.retry.get_retries() == 3


@pytest.mark.parametrize(
    "backend_cls",
    [StandaloneRedisBackend, AWSElastiCacheBackend, AzureRedisCacheBackend],
)
def test_every_factory_accepts_client_kwargs(backend_cls):
    """Guards the factories that need cloud credentials to actually construct.

    A factory without **client_kwargs cannot be forwarding the resilience
    defaults, so this catches a new auth method that forgets them.
    """
    factories = [
        name
        for name in dir(backend_cls)
        if name.startswith("from_")
        and isinstance(inspect.getattr_static(backend_cls, name), classmethod)
    ]
    assert factories, f"no factories found on {backend_cls.__name__}"

    for name in factories:
        params = inspect.signature(getattr(backend_cls, name)).parameters.values()
        assert any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params), (
            f"{backend_cls.__name__}.{name} does not accept **client_kwargs, so it "
            "cannot forward the connection-resilience defaults"
        )
