"""Unit tests for AzureServiceBusBackend session/FIFO support.

These verify our wiring against a mocked ServiceBusClient — Azure session
behavior itself is validated by contract (no emulator in CI).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from azure.servicebus import NEXT_AVAILABLE_SESSION
from azure.servicebus.exceptions import OperationTimeoutError

from cloudrift.core.exceptions import FeatureNotSupportedError, MessageSendError, MessagingError
from cloudrift.messaging.azure_bus import AzureServiceBusBackend

CONN_STR = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y"


def _make_backend(session_enabled=False):
    return AzureServiceBusBackend.from_connection_string(
        CONN_STR, "test-queue", session_enabled=session_enabled
    )


def _mock_sender():
    sender = AsyncMock()
    sender.__aenter__.return_value = sender
    return sender


def _patch_client(backend, client):
    backend._client = client


def _make_received_message(lock_token="tok-1", session_id=None, message_id="m-1",
                           delivery_count=0, body='{"n": 1}'):
    m = MagicMock()
    m.lock_token = lock_token
    m.session_id = session_id
    m.message_id = message_id
    m.delivery_count = delivery_count
    m.sequence_number = 7
    m.enqueued_time_utc = "2026-01-01"
    m.__str__ = MagicMock(return_value=body)
    return m


async def test_send_sets_session_and_message_id():
    backend = _make_backend(session_enabled=True)
    client = MagicMock()
    sender = _mock_sender()
    client.get_queue_sender.return_value = sender
    _patch_client(backend, client)

    await backend.send({"n": 1}, group_id="owner-1", dedup_id="d-1")

    sent = sender.send_messages.call_args[0][0]
    assert sent.session_id == "owner-1"
    assert sent.message_id == "d-1"


async def test_sessionless_send_to_session_queue_raises():
    backend = _make_backend(session_enabled=True)
    _patch_client(backend, MagicMock())
    with pytest.raises(MessageSendError, match="group_id is required"):
        await backend.send({"n": 1})


async def test_send_without_session_on_plain_queue_ok():
    backend = _make_backend(session_enabled=False)
    client = MagicMock()
    sender = _mock_sender()
    client.get_queue_sender.return_value = sender
    _patch_client(backend, client)

    await backend.send({"n": 1})
    sent = sender.send_messages.call_args[0][0]
    assert sent.session_id is None


async def test_send_batch_sets_per_message_dedup_ids():
    backend = _make_backend(session_enabled=True)
    client = MagicMock()
    sender = _mock_sender()
    batch = MagicMock()
    sender.create_message_batch = AsyncMock(return_value=batch)
    client.get_queue_sender.return_value = sender
    _patch_client(backend, client)

    ids = await backend.send_batch(
        [{"n": 1}, {"n": 2}], group_id="g", dedup_ids=["a", "b"]
    )
    assert ids == ["a", "b"]
    added = [c.args[0] for c in batch.add_message.call_args_list]
    assert [m.message_id for m in added] == ["a", "b"]
    assert all(m.session_id == "g" for m in added)


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
    raw = _make_received_message(session_id="owner-1", message_id="d-1", delivery_count=1)
    receiver.receive_messages.return_value = [raw]
    client.get_queue_receiver.return_value = receiver
    _patch_client(backend, client)

    [m] = await backend.receive()
    assert m.group_id == "owner-1"
    assert m.dedup_id == "d-1"
    assert m.receive_count == 2  # delivery_count + 1
    assert m.body == {"n": 1}


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
    with patch(
        "azure.servicebus.aio.management.ServiceBusAdministrationClient"
    ) as admin_cls:
        admin_cls.from_connection_string.return_value = admin
        depth = await backend.get_queue_depth()
    assert depth == 5
    admin.get_queue_runtime_properties.assert_awaited_once_with("test-queue")


async def test_session_enabled_threads_through_factories():
    with patch("azure.identity.aio.ManagedIdentityCredential"):
        b = AzureServiceBusBackend.from_managed_identity(
            "ns.servicebus.windows.net", "q", session_enabled=True
        )
        assert b.session_enabled is True
    with patch("azure.identity.aio.ClientSecretCredential"):
        b = AzureServiceBusBackend.from_service_principal(
            "ns.servicebus.windows.net", "t", "c", "s", "q", session_enabled=True
        )
        assert b.session_enabled is True
