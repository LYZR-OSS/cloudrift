"""Tests for the GCP Pub/Sub messaging backend.

Verified against mocked publisher/subscriber clients — there is no in-process
Pub/Sub mock, so this follows ``test_messaging_azure.py``: assert our wiring and
the documented behavior at every point where Pub/Sub semantics differ from SQS.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.api_core.exceptions import (
    DeadlineExceeded,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    RetryError,
)

from cloudrift.core.exceptions import (
    FeatureNotSupportedError,
    MessageSendError,
    MessagingError,
    QueueNotFoundError,
)
from cloudrift.messaging import get_queue
from cloudrift.messaging.base import OutgoingMessage
from cloudrift.messaging.gcp_pubsub import GCPPubSubBackend

PROJECT = "test-project"
TOPIC = "jobs"
SUBSCRIPTION = "jobs-worker"
TOPIC_PATH = f"projects/{PROJECT}/topics/{TOPIC}"
SUB_PATH = f"projects/{PROJECT}/subscriptions/{SUBSCRIPTION}"


def _publisher(message_ids=("mid-1",)):
    publisher = MagicMock()
    publisher.publish = AsyncMock(return_value=MagicMock(message_ids=list(message_ids)))
    publisher.transport.close = AsyncMock()
    return publisher


def _subscriber():
    subscriber = MagicMock()
    subscriber.acknowledge = AsyncMock()
    subscriber.modify_ack_deadline = AsyncMock()
    subscriber.seek = AsyncMock()
    subscriber.get_subscription = AsyncMock()
    subscriber.transport.close = AsyncMock()
    return subscriber


def _received(ack_id="ack-1", data=b'{"a": 1}', attributes=None, ordering_key="", attempt=0):
    received = MagicMock()
    received.ack_id = ack_id
    received.delivery_attempt = attempt
    received.message.message_id = "mid-1"
    received.message.data = data
    received.message.attributes = attributes or {}
    received.message.ordering_key = ordering_key
    return received


def _pull_response(messages):
    return MagicMock(received_messages=messages)


def _backend(publisher=None, subscriber=None, **kwargs):
    kwargs.setdefault("topic", TOPIC)
    kwargs.setdefault("subscription", SUBSCRIPTION)
    backend = GCPPubSubBackend(PROJECT, **kwargs)
    backend._publisher = publisher
    backend._subscriber = subscriber
    return backend


# ---------------------------------------------------------------------------
# Construction: topic and subscription are independently optional
# ---------------------------------------------------------------------------


def test_construction_requires_at_least_one_resource():
    with pytest.raises(ValueError, match="topic .* subscription"):
        GCPPubSubBackend(PROJECT)


async def test_send_only_backend_needs_no_subscription():
    publisher = _publisher()
    backend = GCPPubSubBackend(PROJECT, topic=TOPIC)
    backend._publisher = publisher
    assert await backend.send({"a": 1}) == "mid-1"


async def test_receiving_without_a_subscription_fails_clearly():
    backend = GCPPubSubBackend(PROJECT, topic=TOPIC)
    backend._subscriber = _subscriber()
    with pytest.raises(MessagingError, match="no subscription configured"):
        await backend.receive()


async def test_sending_without_a_topic_fails_clearly():
    backend = GCPPubSubBackend(PROJECT, subscription=SUBSCRIPTION)
    backend._publisher = _publisher()
    with pytest.raises(MessagingError, match="no topic configured"):
        await backend.send({"a": 1})


@pytest.mark.parametrize(
    "topic,expected",
    [
        (TOPIC, TOPIC_PATH),
        (TOPIC_PATH, TOPIC_PATH),  # a full resource name passes through
    ],
)
async def test_topic_accepts_bare_id_or_resource_name(topic, expected):
    publisher = _publisher()
    backend = _backend(publisher=publisher, topic=topic)
    await backend.send({"a": 1})
    assert publisher.publish.await_args.kwargs["topic"] == expected


async def test_subscription_accepts_a_full_resource_name():
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(return_value=_pull_response([]))
    backend = _backend(subscriber=subscriber, subscription=SUB_PATH)
    await backend.receive()
    assert subscriber.pull.await_args.kwargs["subscription"] == SUB_PATH


# ---------------------------------------------------------------------------
# send: serialization stays in the base class
# ---------------------------------------------------------------------------


async def test_send_serializes_the_dict_to_json_bytes():
    """The base class produces a JSON str; Pub/Sub wants bytes, so this backend
    encodes. The payload must survive that round trip exactly."""
    import json

    publisher = _publisher()
    await _backend(publisher=publisher).send({"user": "alice", "n": 3})
    message = publisher.publish.await_args.kwargs["messages"][0]
    assert json.loads(message.data.decode("utf-8")) == {"user": "alice", "n": 3}


async def test_send_rejects_a_non_dict_payload():
    """Enforced by the ABC's to_json, so it must hold here too."""
    with pytest.raises(TypeError):
        await _backend(publisher=_publisher()).send("already a string")


async def test_send_maps_attributes_natively():
    publisher = _publisher()
    await _backend(publisher=publisher).send({"a": 1}, attributes={"trace": "abc"})
    message = publisher.publish.await_args.kwargs["messages"][0]
    assert dict(message.attributes) == {"trace": "abc"}


async def test_send_maps_group_id_to_ordering_key():
    publisher = _publisher()
    await _backend(publisher=publisher).send({"a": 1}, group_id="tenant-1")
    assert publisher.publish.await_args.kwargs["messages"][0].ordering_key == "tenant-1"


async def test_send_batch_goes_in_one_request():
    """Pub/Sub takes the whole batch at once — no 10-message chunking as on SNS/SQS."""
    publisher = _publisher(message_ids=[f"mid-{i}" for i in range(25)])
    ids = await _backend(publisher=publisher).send_batch(
        [OutgoingMessage(body={"i": i}) for i in range(25)]
    )
    assert len(ids) == 25
    publisher.publish.assert_awaited_once()
    assert len(publisher.publish.await_args.kwargs["messages"]) == 25


async def test_send_batch_empty_is_a_no_op():
    publisher = _publisher()
    assert await _backend(publisher=publisher).send_batch([]) == []
    publisher.publish.assert_not_awaited()


async def test_send_batch_rejects_mismatched_dedup_ids():
    with pytest.raises(MessageSendError, match="parallel"):
        await _backend(publisher=_publisher()).send_batch(
            [OutgoingMessage(body={"i": 1})], dedup_ids=["a", "b"]
        )


# ---------------------------------------------------------------------------
# Unsupported features fail loudly rather than silently differing
# ---------------------------------------------------------------------------


async def test_dedup_id_is_unsupported():
    with pytest.raises(FeatureNotSupportedError, match="deduplication"):
        await _backend(publisher=_publisher()).send({"a": 1}, dedup_id="dedup-1")


async def test_delay_is_unsupported():
    with pytest.raises(FeatureNotSupportedError, match="delay"):
        await _backend(publisher=_publisher()).send({"a": 1}, delay=30)


async def test_receive_by_group_is_unsupported():
    with pytest.raises(FeatureNotSupportedError, match="ordering key"):
        await _backend(subscriber=_subscriber()).receive(group_id="tenant-1")


async def test_get_queue_depth_is_not_implemented():
    """Backlog is a Cloud Monitoring metric, not a data-plane call. Raising
    beats returning a wrong number to an autoscaler."""
    with pytest.raises(NotImplementedError, match="num_undelivered_messages"):
        await _backend().get_queue_depth()


# ---------------------------------------------------------------------------
# receive
# ---------------------------------------------------------------------------


async def test_receive_maps_a_message():
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(
        return_value=_pull_response(
            [
                _received(
                    ack_id="ack-9",
                    data=b'{"job": 7}',
                    attributes={"trace": "xyz"},
                    ordering_key="tenant-1",
                    attempt=3,
                )
            ]
        )
    )
    messages = await _backend(subscriber=subscriber).receive(max_messages=5)

    assert len(messages) == 1
    message = messages[0]
    assert message.receipt_handle == "ack-9"
    assert message.body == b'{"job": 7}'
    assert message.data == {"job": 7}
    assert message.attributes == {"trace": "xyz"}
    assert message.group_id == "tenant-1"
    assert message.receive_count == 3
    assert message.dedup_id is None


async def test_receive_body_stays_raw_bytes_for_malformed_payloads():
    """Deliberately asymmetric with send: a non-JSON payload from a foreign
    producer must stay inspectable for dead-letter triage."""
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(return_value=_pull_response([_received(data=b"\xff not json")]))
    message = (await _backend(subscriber=subscriber).receive())[0]
    assert message.body == b"\xff not json"
    with pytest.raises(Exception):
        message.data


async def test_receive_count_is_none_when_untracked():
    """delivery_attempt is 0 unless the subscription has a dead-letter policy."""
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(return_value=_pull_response([_received(attempt=0)]))
    message = (await _backend(subscriber=subscriber).receive())[0]
    assert message.receive_count is None


async def test_wait_time_becomes_the_rpc_timeout():
    """Pub/Sub has no long-poll parameter, so wait_time bounds the call."""
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(return_value=_pull_response([]))
    await _backend(subscriber=subscriber).receive(wait_time=20)
    assert subscriber.pull.await_args.kwargs["timeout"] == 20.0


async def test_wait_time_zero_uses_a_bounded_deadline():
    """wait_time=0 must still bound the pull. Passing no timeout would let a bare
    receive() block on the gRPC client's default (~60s) against an idle queue."""
    from cloudrift.messaging.gcp_pubsub import _NO_WAIT_PULL_TIMEOUT

    subscriber = _subscriber()
    subscriber.pull = AsyncMock(return_value=_pull_response([]))
    await _backend(subscriber=subscriber).receive()
    assert subscriber.pull.await_args.kwargs["timeout"] == _NO_WAIT_PULL_TIMEOUT


@pytest.mark.parametrize(
    "exc_factory",
    [
        lambda: DeadlineExceeded("deadline"),
        lambda: RetryError("retry exhausted", cause=DeadlineExceeded("deadline")),
    ],
)
async def test_empty_pull_deadline_returns_empty_not_error(exc_factory):
    """Regression: a pull whose deadline elapses with no messages is the long-poll
    "nothing this round" outcome (SQS returns []), NOT an error. This raised
    MessagingError before the fix, so every poll of an idle Pub/Sub queue blew up."""
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(side_effect=exc_factory())
    result = await _backend(subscriber=subscriber).receive(wait_time=5)
    assert result == []


async def test_visibility_timeout_is_applied_after_the_pull():
    """The pull RPC cannot carry a deadline override, so it becomes a
    modifyAckDeadline over the returned ack IDs."""
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(
        return_value=_pull_response([_received(ack_id="a1"), _received(ack_id="a2")])
    )
    await _backend(subscriber=subscriber).receive(max_messages=2, visibility_timeout=90)
    subscriber.modify_ack_deadline.assert_awaited_once_with(
        subscription=SUB_PATH, ack_ids=["a1", "a2"], ack_deadline_seconds=90
    )


async def test_no_modack_when_the_pull_is_empty():
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(return_value=_pull_response([]))
    await _backend(subscriber=subscriber).receive(visibility_timeout=90)
    subscriber.modify_ack_deadline.assert_not_awaited()


# ---------------------------------------------------------------------------
# ack / nack
# ---------------------------------------------------------------------------


async def test_delete_acknowledges():
    subscriber = _subscriber()
    await _backend(subscriber=subscriber).delete("ack-1")
    subscriber.acknowledge.assert_awaited_once_with(subscription=SUB_PATH, ack_ids=["ack-1"])


async def test_nack_zeroes_the_ack_deadline():
    subscriber = _subscriber()
    await _backend(subscriber=subscriber).nack("ack-1")
    subscriber.modify_ack_deadline.assert_awaited_once_with(
        subscription=SUB_PATH, ack_ids=["ack-1"], ack_deadline_seconds=0
    )


async def test_delete_forgets_the_pending_body():
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(return_value=_pull_response([_received(ack_id="ack-1")]))
    backend = _backend(subscriber=subscriber)
    await backend.receive()
    assert "ack-1" in backend._pending
    await backend.delete("ack-1")
    assert "ack-1" not in backend._pending


async def test_pending_body_is_dropped_even_when_ack_fails():
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(return_value=_pull_response([_received(ack_id="ack-1")]))
    subscriber.acknowledge = AsyncMock(side_effect=InvalidArgument("expired"))
    backend = _backend(subscriber=subscriber)
    await backend.receive()
    with pytest.raises(MessagingError):
        await backend.delete("ack-1")
    assert "ack-1" not in backend._pending


# ---------------------------------------------------------------------------
# dead_letter (emulated)
# ---------------------------------------------------------------------------


async def test_dead_letter_publishes_then_acks():
    publisher = _publisher()
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(
        return_value=_pull_response([_received(ack_id="ack-1", data=b'{"job": 7}')])
    )
    backend = _backend(publisher=publisher, subscriber=subscriber, dead_letter_topic="jobs-dlq")
    await backend.receive()
    await backend.dead_letter("ack-1", "poison payload")

    published = publisher.publish.await_args.kwargs
    assert published["topic"] == f"projects/{PROJECT}/topics/jobs-dlq"
    # The original payload is forwarded verbatim, with the reason attached.
    assert published["messages"][0].data == b'{"job": 7}'
    assert dict(published["messages"][0].attributes) == {"DeadLetterReason": "poison payload"}
    subscriber.acknowledge.assert_awaited_once_with(subscription=SUB_PATH, ack_ids=["ack-1"])


async def test_dead_letter_without_a_received_message_raises():
    backend = _backend(
        publisher=_publisher(), subscriber=_subscriber(), dead_letter_topic="jobs-dlq"
    )
    with pytest.raises(MessagingError, match="No pending message"):
        await backend.dead_letter("never-seen", "reason")


async def test_dead_letter_without_a_configured_topic_raises():
    """Pub/Sub cannot report its dead-letter topic without the Admin API, so
    cloudrift refuses to guess rather than silently dropping the message."""
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(return_value=_pull_response([_received(ack_id="ack-1")]))
    backend = _backend(publisher=_publisher(), subscriber=subscriber)
    await backend.receive()
    with pytest.raises(MessagingError, match="No dead-letter topic"):
        await backend.dead_letter("ack-1", "reason")


# ---------------------------------------------------------------------------
# purge / health_check
# ---------------------------------------------------------------------------


async def test_purge_seeks_to_now():
    subscriber = _subscriber()
    backend = _backend(subscriber=subscriber)
    await backend.purge()
    request = subscriber.seek.await_args.kwargs["request"]
    assert request["subscription"] == SUB_PATH
    assert request["time"].seconds > 0


async def test_purge_clears_pending_bodies():
    subscriber = _subscriber()
    subscriber.pull = AsyncMock(return_value=_pull_response([_received(ack_id="ack-1")]))
    backend = _backend(subscriber=subscriber)
    await backend.receive()
    await backend.purge()
    assert backend._pending == {}


async def test_health_check_probes_the_subscription():
    subscriber = _subscriber()
    assert await _backend(subscriber=subscriber).health_check() is True
    subscriber.get_subscription.assert_awaited_once_with(subscription=SUB_PATH)


async def test_health_check_probes_the_topic_when_send_only():
    publisher = _publisher()
    publisher.get_topic = AsyncMock()
    backend = GCPPubSubBackend(PROJECT, topic=TOPIC)
    backend._publisher = publisher
    assert await backend.health_check() is True
    publisher.get_topic.assert_awaited_once_with(topic=TOPIC_PATH)


async def test_health_check_false_on_error():
    subscriber = _subscriber()
    subscriber.get_subscription = AsyncMock(side_effect=NotFound("gone"))
    assert await _backend(subscriber=subscriber).health_check() is False


# ---------------------------------------------------------------------------
# Error translation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc,expected",
    [
        (NotFound("missing"), QueueNotFoundError),
        (PermissionDenied("denied"), MessagingError),
        (InvalidArgument("bad"), MessagingError),
    ],
)
async def test_native_errors_are_translated(exc, expected):
    publisher = _publisher()
    publisher.publish = AsyncMock(side_effect=exc)
    with pytest.raises(expected):
        await _backend(publisher=publisher).send({"a": 1})


# ---------------------------------------------------------------------------
# Lifecycle + factory routing
# ---------------------------------------------------------------------------


async def test_close_closes_both_transports_and_is_idempotent():
    publisher = _publisher()
    subscriber = _subscriber()
    backend = _backend(publisher=publisher, subscriber=subscriber)

    await backend.close()
    await backend.close()

    publisher.transport.close.assert_awaited_once()
    subscriber.transport.close.assert_awaited_once()


async def test_send_only_service_never_opens_a_subscriber():
    backend = GCPPubSubBackend(PROJECT, topic=TOPIC)
    backend._publisher = _publisher()
    await backend.send({"a": 1})
    assert backend._subscriber is None


async def test_clients_are_built_once_and_reused():
    backend = GCPPubSubBackend(PROJECT, topic=TOPIC, subscription=SUBSCRIPTION)
    with (
        patch("cloudrift.messaging.gcp_pubsub.PublisherAsyncClient") as pub,
        patch("cloudrift.messaging.gcp_pubsub.SubscriberAsyncClient") as sub,
    ):
        assert await backend._ensure_publisher() is await backend._ensure_publisher()
        assert await backend._ensure_subscriber() is await backend._ensure_subscriber()
    pub.assert_called_once()
    sub.assert_called_once()


def test_factory_routes_by_credential_keys():
    with patch.object(GCPPubSubBackend, "from_service_account_file") as target:
        get_queue(
            "gcp_pubsub",
            project=PROJECT,
            topic=TOPIC,
            service_account_file="/tmp/sa.json",
        )
    target.assert_called_once()

    with patch.object(GCPPubSubBackend, "from_application_default") as target:
        get_queue("gcp_pubsub", project=PROJECT, topic=TOPIC)
    target.assert_called_once()


def test_unknown_provider_error_lists_gcp():
    with pytest.raises(ValueError, match="gcp_pubsub"):
        get_queue("nope")
