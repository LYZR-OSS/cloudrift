"""Tests for the GCP Secret Manager backend.

Verified against a mocked ``SecretManagerServiceAsyncClient`` — there is no
in-process Secret Manager mock, so the same approach as
``test_messaging_azure.py`` applies: assert our wiring, and let the API contract
cover the rest.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.api_core.exceptions import (
    AlreadyExists,
    InvalidArgument,
    NotFound,
    PermissionDenied,
)

from cloudrift.core.exceptions import (
    SecretError,
    SecretNotFoundError,
    SecretPermissionError,
)
from cloudrift.secrets import get_secrets
from cloudrift.secrets.gcp_secret_manager import GCPSecretManagerBackend

PROJECT = "test-project"


def _payload(value: str):
    response = MagicMock()
    response.payload.data = value.encode("utf-8")
    return response


def _async_pager(items):
    """Stand-in for a GAPIC async pager: awaited, then iterated."""

    class Pager:
        def __aiter__(self):
            async def gen():
                for item in items:
                    yield item

            return gen()

    return Pager()


def _backend(client=None):
    backend = GCPSecretManagerBackend(PROJECT)
    backend._client = client if client is not None else MagicMock()
    return backend


def _secret(name: str):
    secret = MagicMock()
    secret.name = f"projects/{PROJECT}/secrets/{name}"
    return secret


# ---------------------------------------------------------------------------
# Resource paths — bare IDs in, fully-qualified names out
# ---------------------------------------------------------------------------


async def test_get_secret_builds_the_latest_version_path():
    client = MagicMock()
    client.access_secret_version = AsyncMock(return_value=_payload("s3cret"))
    backend = _backend(client)

    assert await backend.get_secret("db-password") == "s3cret"
    client.access_secret_version.assert_awaited_once_with(
        name=f"projects/{PROJECT}/secrets/db-password/versions/latest"
    )


async def test_get_secret_accepts_an_explicit_version():
    client = MagicMock()
    client.access_secret_version = AsyncMock(return_value=_payload("old"))
    backend = _backend(client)

    assert await backend.get_secret("db-password", version="3") == "old"
    assert client.access_secret_version.await_args.kwargs["name"].endswith("/versions/3")


async def test_get_secret_json_parses():
    client = MagicMock()
    client.access_secret_version = AsyncMock(return_value=_payload('{"user": "admin"}'))
    assert await _backend(client).get_secret_json("cfg") == {"user": "admin"}


async def test_get_secret_json_rejects_non_json():
    client = MagicMock()
    client.access_secret_version = AsyncMock(return_value=_payload("not json"))
    with pytest.raises(SecretError, match="not valid JSON"):
        await _backend(client).get_secret_json("cfg")


# ---------------------------------------------------------------------------
# set_secret — immutable versions
# ---------------------------------------------------------------------------


async def test_set_secret_adds_a_version_to_an_existing_secret():
    client = MagicMock()
    client.add_secret_version = AsyncMock()
    client.create_secret = AsyncMock()
    backend = _backend(client)

    await backend.set_secret("token", "abc")

    client.add_secret_version.assert_awaited_once_with(
        parent=f"projects/{PROJECT}/secrets/token",
        payload={"data": b"abc"},
    )
    client.create_secret.assert_not_awaited()


async def test_set_secret_creates_the_secret_when_missing_then_adds_a_version():
    client = MagicMock()
    client.add_secret_version = AsyncMock(side_effect=[NotFound("nope"), None])
    client.create_secret = AsyncMock()
    backend = _backend(client)

    await backend.set_secret("brand-new", "abc")

    client.create_secret.assert_awaited_once_with(
        parent=f"projects/{PROJECT}",
        secret_id="brand-new",
        secret={"replication": {"automatic": {}}},
    )
    assert client.add_secret_version.await_count == 2


async def test_set_secret_survives_losing_a_create_race():
    """Two writers creating the same secret concurrently: the loser gets
    AlreadyExists and must still add its version rather than blowing up."""
    client = MagicMock()
    client.add_secret_version = AsyncMock(side_effect=[NotFound("nope"), None])
    client.create_secret = AsyncMock(side_effect=AlreadyExists("raced"))
    backend = _backend(client)

    await backend.set_secret("contended", "abc")

    assert client.add_secret_version.await_count == 2


async def test_set_secret_honors_a_user_managed_replication_policy():
    client = MagicMock()
    client.add_secret_version = AsyncMock(side_effect=[NotFound("nope"), None])
    client.create_secret = AsyncMock()
    policy = {"user_managed": {"replicas": [{"location": "europe-west1"}]}}
    backend = GCPSecretManagerBackend(PROJECT, replication=policy)
    backend._client = client

    await backend.set_secret("eu-only", "abc")

    assert client.create_secret.await_args.kwargs["secret"] == {"replication": policy}


# ---------------------------------------------------------------------------
# delete / list
# ---------------------------------------------------------------------------


async def test_delete_secret_targets_the_secret_path():
    client = MagicMock()
    client.delete_secret = AsyncMock()
    await _backend(client).delete_secret("gone")
    client.delete_secret.assert_awaited_once_with(name=f"projects/{PROJECT}/secrets/gone")


async def test_list_secrets_returns_bare_ids():
    client = MagicMock()
    client.list_secrets = AsyncMock(return_value=_async_pager([_secret("alpha"), _secret("beta")]))
    assert await _backend(client).list_secrets() == ["alpha", "beta"]


async def test_list_secrets_prefix_is_anchored():
    """Filtering client-side is deliberate: Secret Manager's server-side filter
    is a substring match, which would also return 'my-prod-db'."""
    client = MagicMock()
    client.list_secrets = AsyncMock(
        return_value=_async_pager(
            [_secret("prod-db"), _secret("my-prod-db"), _secret("prod-cache")]
        )
    )
    assert await _backend(client).list_secrets(prefix="prod-") == [
        "prod-db",
        "prod-cache",
    ]


# ---------------------------------------------------------------------------
# Error translation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc,expected",
    [
        (NotFound("missing"), SecretNotFoundError),
        (PermissionDenied("denied"), SecretPermissionError),
        (InvalidArgument("bad"), SecretError),
    ],
)
async def test_native_errors_are_translated(exc, expected):
    client = MagicMock()
    client.access_secret_version = AsyncMock(side_effect=exc)
    with pytest.raises(expected):
        await _backend(client).get_secret("whatever")


async def test_health_check_touches_the_first_page():
    """The async pager is lazy, so awaiting list_secrets alone proves nothing."""
    client = MagicMock()
    client.list_secrets = AsyncMock(return_value=_async_pager([_secret("a")]))
    assert await _backend(client).health_check() is True


async def test_health_check_false_on_error():
    client = MagicMock()
    client.list_secrets = AsyncMock(side_effect=PermissionDenied("denied"))
    assert await _backend(client).health_check() is False


# ---------------------------------------------------------------------------
# Lifecycle + factory routing
# ---------------------------------------------------------------------------


async def test_close_closes_the_transport_and_is_idempotent():
    client = MagicMock()
    client.transport.close = AsyncMock()
    backend = _backend(client)

    await backend.close()
    await backend.close()

    client.transport.close.assert_awaited_once()


async def test_client_is_built_once_and_reused():
    backend = GCPSecretManagerBackend(PROJECT)
    with patch("cloudrift.secrets.gcp_secret_manager.SecretManagerServiceAsyncClient") as ctor:
        first = await backend._ensure()
        second = await backend._ensure()
    assert first is second
    ctor.assert_called_once()


def test_factory_routes_by_credential_keys():
    with patch.object(GCPSecretManagerBackend, "from_service_account_file") as target:
        get_secrets("gcp_secret_manager", project=PROJECT, service_account_file="/tmp/sa.json")
    target.assert_called_once()

    with patch.object(GCPSecretManagerBackend, "from_service_account_info") as target:
        get_secrets("gcp_secret_manager", project=PROJECT, service_account_info={})
    target.assert_called_once()

    with patch.object(GCPSecretManagerBackend, "from_application_default") as target:
        get_secrets("gcp_secret_manager", project=PROJECT)
    target.assert_called_once()


def test_unknown_provider_error_lists_gcp():
    with pytest.raises(ValueError, match="gcp_secret_manager"):
        get_secrets("nope")
