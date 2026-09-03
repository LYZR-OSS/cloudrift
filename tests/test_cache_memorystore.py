"""Tests for the GCP Memorystore cache backend.

Operations come from ``_RedisMixin`` and are already covered by
``test_cache.py``; what is specific to Memorystore is the *construction* — which
client kwargs each ``from_*`` selects — plus the IAM credential provider. A
fakeredis client is substituted for the operation smoke test so the shared mixin
is exercised through this class too.
"""

from unittest.mock import MagicMock, patch

import fakeredis.aioredis
import pytest

from cloudrift.cache import cache_broker_url, get_cache
from cloudrift.cache.base import DEFAULT_MAX_RETRIES
from cloudrift.cache.redis_memorystore import (
    GCPMemorystoreBackend,
    _GCPIAMCredentialProvider,
)
from cloudrift.core.exceptions import CacheConnectionError

HOST = "10.0.0.3"


@pytest.fixture
async def cache():
    """Memorystore backend wired to an in-process fakeredis instance."""
    backend = GCPMemorystoreBackend(fakeredis.aioredis.FakeRedis())
    yield backend
    await backend.flush()
    await backend.close()


def _redis_kwargs(mock_redis) -> dict:
    return mock_redis.call_args.kwargs


# ---------------------------------------------------------------------------
# Operations reach the shared mixin
# ---------------------------------------------------------------------------


async def test_operations_work_through_the_mixin(cache):
    await cache.set("k", b"v", ttl=60)
    assert await cache.get("k") == b"v"
    assert await cache.exists("k")
    assert await cache.delete("k") == 1


async def test_pipeline_is_the_real_redis_pipeline(cache):
    async with cache.pipeline() as pipe:
        pipe.sadd("s", "a")
        pipe.expire("s", 60)
    assert await cache.scard("s") == 1


# ---------------------------------------------------------------------------
# from_auth_string
# ---------------------------------------------------------------------------


def test_auth_string_defaults_to_plaintext():
    """Memorystore leaves in-transit encryption off by default, unlike
    ElastiCache and Azure Redis — the default here must match the product."""
    with patch("redis.asyncio.Redis") as redis:
        GCPMemorystoreBackend.from_auth_string(HOST, auth_string="secret")
    kwargs = _redis_kwargs(redis)
    assert kwargs["ssl"] is False
    assert kwargs["port"] == 6379
    assert kwargs["password"] == "secret"


def test_auth_string_applies_resilience_defaults():
    with patch("redis.asyncio.Redis") as redis:
        GCPMemorystoreBackend.from_auth_string(HOST)
    kwargs = _redis_kwargs(redis)
    assert kwargs["health_check_interval"] == 30
    assert kwargs["socket_keepalive"] is True
    assert kwargs["retry"].get_retries() == DEFAULT_MAX_RETRIES


def test_auth_string_allows_overriding_resilience_defaults():
    with patch("redis.asyncio.Redis") as redis:
        GCPMemorystoreBackend.from_auth_string(HOST, socket_timeout=99)
    assert _redis_kwargs(redis)["socket_timeout"] == 99


def test_auth_string_without_auth_connects_unauthenticated():
    with patch("redis.asyncio.Redis") as redis:
        GCPMemorystoreBackend.from_auth_string(HOST)
    assert _redis_kwargs(redis)["password"] is None


# ---------------------------------------------------------------------------
# from_server_ca_cert
# ---------------------------------------------------------------------------


def test_server_ca_cert_forces_tls_and_pins_the_ca():
    with patch("redis.asyncio.Redis") as redis:
        GCPMemorystoreBackend.from_server_ca_cert(HOST, ssl_ca_certs="/etc/ca.pem")
    kwargs = _redis_kwargs(redis)
    assert kwargs["ssl"] is True
    assert kwargs["ssl_ca_certs"] == "/etc/ca.pem"
    # 6378 is Memorystore's in-transit-encryption port, not the usual 6380.
    assert kwargs["port"] == 6378


def test_server_ca_cert_requires_the_ca_file():
    """The per-instance CA is not in the system trust store, so making it
    optional would hand callers a connection that always fails verification."""
    with pytest.raises(TypeError):
        GCPMemorystoreBackend.from_server_ca_cert(HOST)


# ---------------------------------------------------------------------------
# from_iam_auth
# ---------------------------------------------------------------------------


def test_iam_auth_uses_a_credential_provider_and_tls():
    with (
        patch("redis.asyncio.Redis") as redis,
        patch("cloudrift.core.gcp_credentials.build_credentials") as build,
    ):
        build.return_value = MagicMock(valid=True, token="ya29.token")
        GCPMemorystoreBackend.from_iam_auth(HOST)
    kwargs = _redis_kwargs(redis)
    assert kwargs["ssl"] is True
    assert isinstance(kwargs["credential_provider"], _GCPIAMCredentialProvider)
    # No static password may be set — the token comes from the provider.
    assert "password" not in kwargs


def test_iam_auth_forwards_prefer_metadata():
    with (
        patch("redis.asyncio.Redis"),
        patch("cloudrift.core.gcp_credentials.build_credentials") as build,
    ):
        build.return_value = MagicMock(valid=True, token="t")
        GCPMemorystoreBackend.from_iam_auth(HOST, prefer_metadata=True)
    assert build.call_args.kwargs["prefer_metadata"] is True


def test_iam_auth_connection_failure_is_translated():
    with (
        patch("redis.asyncio.Redis", side_effect=RuntimeError("boom")),
        patch("cloudrift.core.gcp_credentials.build_credentials") as build,
    ):
        build.return_value = MagicMock(valid=True, token="t")
        with pytest.raises(CacheConnectionError, match="Memorystore"):
            GCPMemorystoreBackend.from_iam_auth(HOST)


# ---------------------------------------------------------------------------
# The IAM credential provider itself
# ---------------------------------------------------------------------------


def test_provider_returns_username_and_token():
    credentials = MagicMock(valid=True, token="ya29.token")
    provider = _GCPIAMCredentialProvider(credentials, "default")
    assert provider.get_credentials() == ("default", "ya29.token")
    credentials.refresh.assert_not_called()


def test_provider_refreshes_an_expired_token():
    """redis-py calls get_credentials() on every reconnect; an expired token
    must be refreshed there or reconnects fail after ~1 hour."""
    credentials = MagicMock(valid=False, token="stale")

    def _refresh(_request):
        credentials.valid = True
        credentials.token = "fresh"

    credentials.refresh.side_effect = _refresh
    provider = _GCPIAMCredentialProvider(credentials, "default")
    assert provider.get_credentials() == ("default", "fresh")
    credentials.refresh.assert_called_once()


# ---------------------------------------------------------------------------
# Factory + broker URL
# ---------------------------------------------------------------------------


def test_get_cache_routes_to_memorystore():
    with patch("redis.asyncio.Redis"):
        backend = get_cache("memorystore", "from_auth_string", host=HOST)
    assert isinstance(backend, GCPMemorystoreBackend)


def test_get_cache_rejects_unknown_auth_method():
    with pytest.raises(ValueError, match="no auth method"):
        get_cache("memorystore", "from_nowhere", host=HOST)


def test_unknown_provider_error_lists_memorystore():
    with pytest.raises(ValueError, match="memorystore"):
        get_cache("nope", "from_auth_string")


def test_broker_url_for_memorystore_is_tls():
    url = cache_broker_url("memorystore", HOST, 6378, password="secret")
    assert url.startswith("rediss://default:secret@")
    assert "ssl_cert_reqs=CERT_NONE" in url
