"""Tests for the GCP Pub/Sub fan-out backend.

Pub/Sub serves two cloudrift categories: this one (the SNS analog — publish to a
topic) and ``cloudrift.messaging.gcp_pubsub`` (the SQS analog — pull from a
subscription). These tests cover the publish-only surface.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.api_core.exceptions import InvalidArgument, NotFound, PermissionDenied

from cloudrift.core.exceptions import PubSubError, TopicNotFoundError
from cloudrift.pubsub import get_pubsub
from cloudrift.pubsub.gcp_pubsub import GCPPubSubBackend

PROJECT = "test-project"
TOPIC = "events"
TOPIC_PATH = f"projects/{PROJECT}/topics/{TOPIC}"


def _client(message_ids=("mid-1",)):
    client = MagicMock()
    client.publish = AsyncMock(return_value=MagicMock(message_ids=list(message_ids)))
    client.transport.close = AsyncMock()
    return client


def _backend(client=None):
    backend = GCPPubSubBackend(PROJECT)
    backend._client = client if client is not None else _client()
    return backend


def _published(client):
    return client.publish.await_args.kwargs


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------


async def test_publish_returns_the_message_id():
    client = _client(message_ids=("mid-42",))
    assert await _backend(client).publish(TOPIC, "hello") == "mid-42"


async def test_publish_encodes_the_body_to_bytes():
    client = _client()
    await _backend(client).publish(TOPIC, "hello world")
    assert _published(client)["messages"][0].data == b"hello world"


async def test_publish_resolves_a_bare_topic_id_against_the_project():
    client = _client()
    await _backend(client).publish(TOPIC, "x")
    assert _published(client)["topic"] == TOPIC_PATH


async def test_publish_passes_a_full_resource_name_through():
    client = _client()
    await _backend(client).publish(TOPIC_PATH, "x")
    assert _published(client)["topic"] == TOPIC_PATH


async def test_publish_maps_attributes_natively():
    """Pub/Sub attributes are already string→string — no SNS DataType wrapper."""
    client = _client()
    await _backend(client).publish(TOPIC, "x", attributes={"event_type": "created"})
    assert dict(_published(client)["messages"][0].attributes) == {"event_type": "created"}


async def test_publish_stringifies_non_string_attribute_values():
    client = _client()
    await _backend(client).publish(TOPIC, "x", attributes={"version": 2, "ok": True})
    attributes = dict(_published(client)["messages"][0].attributes)
    assert attributes == {"version": "2", "ok": "True"}


# ---------------------------------------------------------------------------
# publish_batch
# ---------------------------------------------------------------------------


async def test_publish_batch_returns_all_ids():
    client = _client(message_ids=("m1", "m2", "m3"))
    ids = await _backend(client).publish_batch(
        TOPIC,
        [
            {"message": "one", "attributes": {"seq": "1"}},
            {"message": "two"},
            {"message": "three"},
        ],
    )
    assert ids == ["m1", "m2", "m3"]


async def test_publish_batch_sends_one_request_for_more_than_ten():
    """SNS caps a batch at 10 and the AWS backend chunks; Pub/Sub does not, so
    chunking here would be wasted round trips."""
    client = _client(message_ids=[f"m{i}" for i in range(25)])
    ids = await _backend(client).publish_batch(TOPIC, [{"message": str(i)} for i in range(25)])
    assert len(ids) == 25
    client.publish.assert_awaited_once()
    assert len(_published(client)["messages"]) == 25


async def test_publish_batch_empty_is_a_no_op():
    client = _client()
    assert await _backend(client).publish_batch(TOPIC, []) == []
    client.publish.assert_not_awaited()


async def test_publish_batch_handles_a_message_without_a_body():
    client = _client()
    await _backend(client).publish_batch(TOPIC, [{"attributes": {"a": "b"}}])
    assert _published(client)["messages"][0].data == b""


# ---------------------------------------------------------------------------
# Error translation — must match the SNS backend's mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc,expected",
    [
        (NotFound("missing"), TopicNotFoundError),
        (PermissionDenied("denied"), PubSubError),
        (InvalidArgument("bad"), PubSubError),
    ],
)
async def test_native_errors_are_translated(exc, expected):
    client = MagicMock()
    client.publish = AsyncMock(side_effect=exc)
    with pytest.raises(expected):
        await _backend(client).publish(TOPIC, "x")


async def test_topic_not_found_names_the_topic():
    client = MagicMock()
    client.publish = AsyncMock(side_effect=NotFound("missing"))
    with pytest.raises(TopicNotFoundError, match=TOPIC):
        await _backend(client).publish(TOPIC, "x")


# ---------------------------------------------------------------------------
# Lifecycle + factory routing
# ---------------------------------------------------------------------------


async def test_close_closes_the_transport_and_is_idempotent():
    client = _client()
    backend = _backend(client)
    await backend.close()
    await backend.close()
    client.transport.close.assert_awaited_once()


async def test_context_manager_closes():
    client = _client()
    async with _backend(client):
        pass
    client.transport.close.assert_awaited_once()


async def test_client_is_built_once_and_reused():
    backend = GCPPubSubBackend(PROJECT)
    with patch("cloudrift.pubsub.gcp_pubsub.PublisherAsyncClient") as ctor:
        assert await backend._ensure() is await backend._ensure()
    ctor.assert_called_once()


def test_factory_routes_by_credential_keys():
    with patch.object(GCPPubSubBackend, "from_service_account_file") as target:
        get_pubsub("gcp_pubsub", project=PROJECT, service_account_file="/tmp/sa.json")
    target.assert_called_once()

    with patch.object(GCPPubSubBackend, "from_service_account_info") as target:
        get_pubsub("gcp_pubsub", project=PROJECT, service_account_info={})
    target.assert_called_once()

    with patch.object(GCPPubSubBackend, "from_application_default") as target:
        get_pubsub("gcp_pubsub", project=PROJECT)
    target.assert_called_once()


def test_unknown_provider_error_lists_gcp():
    with pytest.raises(ValueError, match="gcp_pubsub"):
        get_pubsub("nope")
