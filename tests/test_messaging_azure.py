"""Unit tests for AzureServiceBusBackend session/FIFO support.

These verify our wiring against a mocked ServiceBusClient — Azure session
behavior itself is validated by contract (no emulator in CI).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from azure.servicebus import NEXT_AVAILABLE_SESSION
from azure.servicebus.exceptions import (
    MessageSizeExceededError,
    MessagingEntityNotFoundError,
    OperationTimeoutError,
    ServiceBusConnectionError,
    ServiceBusError,
)

from cloudrift.core.exceptions import (
    FeatureNotSupportedError,
    MessageSendError,
    MessagingError,
    QueueNotFoundError,
)
from cloudrift.messaging.azure_bus import AzureServiceBusBackend
from cloudrift.messaging.base import OutgoingMessage

CONN_STR = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y"


def _make_backend(session_enabled=False):
    return AzureServiceBusBackend.from_connection_string(
        CONN_STR, "test-queue", session_enabled=session_enabled
    )


def _mock_sender():
    # The backend caches the sender and never enters it as a context manager —
    # `async with sender` would close the link on every send.
    return AsyncMock()


def _patch_client(backend, client):
    backend._client = client


def _sending_backend(session_enabled=False, sender=None):
    """Backend wired to a mock client whose get_queue_sender returns `sender`."""
    backend = _make_backend(session_enabled=session_enabled)
    client = MagicMock()
    client.close = AsyncMock()
    sender = sender or _mock_sender()
    client.get_queue_sender.return_value = sender
    _patch_client(backend, client)
    return backend, client, sender


class _FakeReceivedMessage:
    """Minimal stand-in for ServiceBusReceivedMessage.

    Mirrors the real type's payload access: ``.body`` yields the raw bytes in
    chunks and is single-use, while ``bytes(message)`` raises TypeError. An
    earlier version of this fake implemented ``__bytes__``, which let a
    ``bytes(m)`` conversion pass here and fail against a live broker.
    """

    def __init__(
        self, lock_token, session_id, message_id, delivery_count, body, application_properties
    ):
        self.lock_token = lock_token
        self.session_id = session_id
        self.message_id = message_id
        self.delivery_count = delivery_count
        self.sequence_number = 7
        self.enqueued_time_utc = "2026-01-01"
        self.application_properties = application_properties
        self._body = body
        self._body_consumed = False

    @property
    def body(self):
        if self._body_consumed:
            raise RuntimeError("ServiceBusReceivedMessage.body is single-use")
        self._body_consumed = True
        # The real SDK yields the payload in chunks.
        return iter([self._body[:1], self._body[1:]] if self._body else [])


def _make_received_message(
    lock_token="tok-1",
    session_id=None,
    message_id="m-1",
    delivery_count=0,
    body=b'{"n": 1}',
    application_properties=None,
):
    return _FakeReceivedMessage(
        lock_token, session_id, message_id, delivery_count, body, application_properties
    )


async def test_send_sets_session_and_message_id():
    backend, _, sender = _sending_backend(session_enabled=True)

    await backend.send({"n": 1}, group_id="owner-1", dedup_id="d-1")

    sent = sender.send_messages.call_args[0][0]
    assert sent.session_id == "owner-1"
    assert sent.message_id == "d-1"


async def test_send_serializes_dict_to_json_body():
    """The ABC hands the backend a JSON string; the SDK encodes it to the same bytes."""
    backend, _, sender = _sending_backend()

    await backend.send({"n": 1, "s": "café"})

    sent = sender.send_messages.call_args[0][0]
    assert b"".join(sent.body) == b'{"n": 1, "s": "caf\\u00e9"}'


async def test_send_rejects_non_dict_payload():
    backend, _, sender = _sending_backend()
    with pytest.raises(TypeError, match="got bytes"):
        await backend.send(b'{"n": 1}')
    sender.send_messages.assert_not_awaited()


async def test_sessionless_send_to_session_queue_raises():
    backend = _make_backend(session_enabled=True)
    _patch_client(backend, MagicMock())
    with pytest.raises(MessageSendError, match="group_id is required"):
        await backend.send({"n": 1})


async def test_send_without_session_on_plain_queue_ok():
    backend, _, sender = _sending_backend(session_enabled=False)

    await backend.send({"n": 1})
    sent = sender.send_messages.call_args[0][0]
    assert sent.session_id is None


async def test_send_sets_application_properties_from_attributes():
    backend, _, sender = _sending_backend(session_enabled=False)

    await backend.send({"raw": True}, attributes={"content_type": "text/plain"})
    sent = sender.send_messages.call_args[0][0]
    assert sent.application_properties == {"content_type": "text/plain"}


async def test_send_batch_sets_per_message_dedup_ids():
    sender = _mock_sender()
    batch = MagicMock()
    sender.create_message_batch = AsyncMock(return_value=batch)
    backend, _, _ = _sending_backend(session_enabled=True, sender=sender)

    ids = await backend.send_batch(
        [OutgoingMessage(body={"n": 1}), OutgoingMessage(body={"n": 2})],
        group_id="g",
        dedup_ids=["a", "b"],
    )
    assert ids == ["a", "b"]
    added = [c.args[0] for c in batch.add_message.call_args_list]
    assert [m.message_id for m in added] == ["a", "b"]
    assert all(m.session_id == "g" for m in added)
    assert [b"".join(m.body) for m in added] == [b'{"n": 1}', b'{"n": 2}']


async def test_send_batch_mismatched_dedup_ids():
    backend, _, _ = _sending_backend(session_enabled=True)
    with pytest.raises(MessageSendError, match="parallel"):
        await backend.send_batch(
            [OutgoingMessage(body={"n": 1})], group_id="g", dedup_ids=["a", "b"]
        )


# ---------------------------------------------------------------------------
# Sender caching — one AMQP send link for the life of the backend
# ---------------------------------------------------------------------------


async def test_sender_is_cached_across_sends():
    sender = _mock_sender()
    # add_message is sync on the real batch; MagicMock keeps it from returning a coroutine
    sender.create_message_batch = AsyncMock(return_value=MagicMock())
    backend, client, _ = _sending_backend(sender=sender)

    await backend.send({"n": 1})
    await backend.send({"n": 2})
    await backend.send_batch([OutgoingMessage(body={"n": 3})])

    client.get_queue_sender.assert_called_once_with("test-queue")
    assert sender.send_messages.await_count == 3
    # the cached link is never closed between sends
    sender.close.assert_not_awaited()


@pytest.mark.parametrize(
    "error",
    [
        ServiceBusConnectionError(message="link detached"),
        ValueError("The handler has already been shutdown. Please use ServiceBusClient ..."),
    ],
    ids=["connection_error", "shutdown_handler"],
)
async def test_dead_link_is_rebuilt_and_the_send_retried(error):
    sender = _mock_sender()
    sender.send_messages.side_effect = [error, None]
    backend, client, _ = _sending_backend(sender=sender)

    await backend.send({"n": 1})

    assert client.get_queue_sender.call_count == 2
    assert sender.send_messages.await_count == 2
    sender.close.assert_awaited_once()  # the dead link was discarded


async def test_dead_link_failing_twice_surfaces_as_cloudrift_error():
    sender = _mock_sender()
    sender.send_messages.side_effect = ServiceBusConnectionError(message="still down")
    backend, client, _ = _sending_backend(sender=sender)

    with pytest.raises(MessageSendError, match="still down"):
        await backend.send({"n": 1})
    assert client.get_queue_sender.call_count == 2  # rebuilt once, then gave up


async def test_non_link_error_does_not_rebuild_the_sender():
    sender = _mock_sender()
    sender.send_messages.side_effect = MessageSizeExceededError(message="too big")
    backend, client, _ = _sending_backend(sender=sender)

    with pytest.raises(MessageSendError, match="too big"):
        await backend.send({"n": 1})
    client.get_queue_sender.assert_called_once()
    assert sender.send_messages.await_count == 1


async def test_close_releases_the_cached_sender():
    backend, client, sender = _sending_backend()
    await backend.send({"n": 1})
    assert backend._sender is sender

    await backend.close()

    sender.close.assert_awaited_once()
    assert backend._sender is None
    client.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# Error translation — callers only ever see cloudrift exceptions
# ---------------------------------------------------------------------------


async def test_service_bus_error_translated_to_message_send_error():
    """ServiceBusError is an AzureError, not an HttpResponseError — it must still translate."""
    sender = _mock_sender()
    sender.send_messages.side_effect = ServiceBusError(message="amqp exploded")
    backend, _, _ = _sending_backend(sender=sender)

    with pytest.raises(MessageSendError, match="amqp exploded"):
        await backend.send({"n": 1})


async def test_entity_not_found_translated_to_queue_not_found():
    sender = _mock_sender()
    sender.send_messages.side_effect = MessagingEntityNotFoundError(message="gone")
    backend, _, _ = _sending_backend(sender=sender)

    with pytest.raises(QueueNotFoundError, match="test-queue"):
        await backend.send({"n": 1})


async def test_batch_overflow_translated_to_message_send_error():
    """batch.add_message raises MessageSizeExceededError once the batch is full."""
    sender = _mock_sender()
    batch = MagicMock()
    batch.add_message.side_effect = MessageSizeExceededError(message="batch full")
    sender.create_message_batch = AsyncMock(return_value=batch)
    backend, _, _ = _sending_backend(sender=sender)

    with pytest.raises(MessageSendError, match="batch full"):
        await backend.send_batch([OutgoingMessage(body={"n": 1})])


async def test_receive_uses_next_available_session():
    backend = _make_backend(session_enabled=True)
    client = MagicMock()
    receiver = AsyncMock()
    receiver.receive_messages.return_value = []
    client.get_queue_receiver.return_value = receiver
    _patch_client(backend, client)

    result = await backend.receive(max_messages=5, wait_time=10)
    assert result == []
    kwargs = client.get_queue_receiver.call_args.kwargs
    assert kwargs["session_id"] is NEXT_AVAILABLE_SESSION
    assert kwargs["max_wait_time"] == 10


async def test_receive_with_explicit_group_id():
    backend = _make_backend(session_enabled=True)
    client = MagicMock()
    receiver = AsyncMock()
    receiver.receive_messages.return_value = []
    client.get_queue_receiver.return_value = receiver
    _patch_client(backend, client)

    await backend.receive(group_id="owner-1")
    assert client.get_queue_receiver.call_args.kwargs["session_id"] == "owner-1"


async def test_receive_session_timeout_returns_empty():
    backend = _make_backend(session_enabled=True)
    client = MagicMock()
    receiver = AsyncMock()
    receiver.__aenter__.side_effect = OperationTimeoutError(message="no session")
    client.get_queue_receiver.return_value = receiver
    _patch_client(backend, client)

    assert await backend.receive(wait_time=5) == []


async def test_receive_group_id_on_plain_queue_raises():
    backend = _make_backend(session_enabled=False)
    _patch_client(backend, MagicMock())
    with pytest.raises(FeatureNotSupportedError):
        await backend.receive(group_id="g1")


async def test_receive_populates_fifo_fields():
    backend = _make_backend(session_enabled=True)
    client = MagicMock()
    receiver = AsyncMock()
    raw = _make_received_message(
        session_id="owner-1",
        message_id="d-1",
        delivery_count=1,
        application_properties={b"content_type": b"text/plain"},
    )
    receiver.receive_messages.return_value = [raw]
    client.get_queue_receiver.return_value = receiver
    _patch_client(backend, client)

    [m] = await backend.receive()
    assert m.group_id == "owner-1"
    assert m.dedup_id == "d-1"
    assert m.receive_count == 2  # delivery_count + 1
    assert m.body == b'{"n": 1}'
    assert m.data == {"n": 1}
    # application_properties (bytes keys/values) are stringified into attributes.
    assert m.attributes["content_type"] == "text/plain"


async def test_nack_abandons_and_releases_receiver():
    backend = _make_backend(session_enabled=True)
    client = MagicMock()
    receiver = AsyncMock()
    raw = _make_received_message(lock_token="tok-9")
    receiver.receive_messages.return_value = [raw]
    client.get_queue_receiver.return_value = receiver
    _patch_client(backend, client)

    [m] = await backend.receive()
    await backend.nack(m.receipt_handle)

    receiver.abandon_message.assert_awaited_once_with(raw)
    assert backend._pending == {}
    assert backend._receiver_tokens == {}
    receiver.__aexit__.assert_awaited()


async def test_delete_completes_and_releases_receiver():
    backend = _make_backend(session_enabled=True)
    client = MagicMock()
    receiver = AsyncMock()
    raw = _make_received_message(lock_token="tok-5")
    receiver.receive_messages.return_value = [raw]
    client.get_queue_receiver.return_value = receiver
    _patch_client(backend, client)

    [m] = await backend.receive()
    await backend.delete(m.receipt_handle)

    receiver.complete_message.assert_awaited_once_with(raw)
    assert backend._pending == {}
    assert backend._receiver_tokens == {}


async def test_nack_unknown_handle_raises():
    backend = _make_backend()
    with pytest.raises(MessagingError, match="No pending message"):
        await backend.nack("missing")


async def test_dead_letter_calls_dead_letter_message_and_releases():
    backend = _make_backend(session_enabled=True)
    client = MagicMock()
    receiver = AsyncMock()
    raw = _make_received_message(lock_token="tok-dl")
    receiver.receive_messages.return_value = [raw]
    client.get_queue_receiver.return_value = receiver
    _patch_client(backend, client)

    [m] = await backend.receive()
    await backend.dead_letter(m.receipt_handle, reason="schema mismatch")

    receiver.dead_letter_message.assert_awaited_once_with(
        raw, reason="schema mismatch", error_description="schema mismatch"
    )
    assert backend._pending == {}
    assert backend._receiver_tokens == {}
    receiver.__aexit__.assert_awaited()


async def test_dead_letter_unknown_handle_raises():
    backend = _make_backend()
    with pytest.raises(MessagingError, match="No pending message"):
        await backend.dead_letter("missing", reason="x")


async def test_get_queue_depth_uses_admin_client():
    backend = _make_backend()
    props = MagicMock()
    props.active_message_count = 5
    admin = AsyncMock()
    admin.get_queue_runtime_properties.return_value = props
    admin.__aenter__.return_value = admin
    with patch("azure.servicebus.aio.management.ServiceBusAdministrationClient") as admin_cls:
        admin_cls.from_connection_string.return_value = admin
        depth = await backend.get_queue_depth()
    assert depth == 5
    admin.get_queue_runtime_properties.assert_awaited_once_with("test-queue")


async def test_session_enabled_threads_through_factories():
    with patch("azure.identity.aio.DefaultAzureCredential"):
        b = AzureServiceBusBackend.from_managed_identity(
            "ns.servicebus.windows.net", "q", session_enabled=True
        )
        assert b.session_enabled is True
    with patch("azure.identity.aio.ClientSecretCredential"):
        b = AzureServiceBusBackend.from_service_principal(
            "ns.servicebus.windows.net", "t", "c", "s", "q", session_enabled=True
        )
        assert b.session_enabled is True
