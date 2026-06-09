"""Unit tests for the Azure Service Bus backend.

Azure Service Bus has no local emulator (unlike SQS via moto), so these tests
mock the data-plane receiver and the management-plane admin client. They verify
the cloudrift glue logic — receipt-handle bookkeeping, dead-letter dispatch, and
queue-depth extraction — not the Azure SDK itself.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Azure is an optional dependency and has no local emulator; skip the whole
# module when the SDK isn't installed (e.g. the default `.[dev]` CI install).
pytest.importorskip("azure.servicebus")

from cloudrift.core.exceptions import MessagingError  # noqa: E402
from cloudrift.messaging.azure_bus import AzureServiceBusBackend  # noqa: E402


@pytest.fixture
def backend():
    return AzureServiceBusBackend.from_connection_string(
        connection_string="Endpoint=sb://fake.servicebus.windows.net/;"
        "SharedAccessKeyName=k;SharedAccessKey=secret",
        queue_name="test-queue",
    )


# ---------------------------------------------------------------------------
# dead_letter (native, data plane)
# ---------------------------------------------------------------------------

async def test_dead_letter_calls_native_api(backend):
    receiver = AsyncMock()
    message = MagicMock()
    handle = "lock-token-1"
    # Simulate the state receive() would have left behind.
    backend._pending[handle] = (receiver, message)
    backend._receiver_tokens[id(receiver)] = (receiver, {handle})

    await backend.dead_letter(handle, reason="poison message")

    receiver.dead_letter_message.assert_awaited_once_with(
        message, reason="poison message", error_description="poison message"
    )
    # Bookkeeping is cleared and the now-empty receiver is closed.
    assert handle not in backend._pending
    assert id(receiver) not in backend._receiver_tokens
    receiver.__aexit__.assert_awaited()


async def test_dead_letter_unknown_handle_raises(backend):
    with pytest.raises(MessagingError, match="No pending message"):
        await backend.dead_letter("nope", reason="x")


async def test_dead_letter_keeps_receiver_open_for_other_messages(backend):
    receiver = AsyncMock()
    msg_a, msg_b = MagicMock(), MagicMock()
    backend._pending["a"] = (receiver, msg_a)
    backend._pending["b"] = (receiver, msg_b)
    backend._receiver_tokens[id(receiver)] = (receiver, {"a", "b"})

    await backend.dead_letter("a", reason="bad")

    # "b" is still pending, so the shared receiver must stay open.
    assert "b" in backend._pending
    assert id(receiver) in backend._receiver_tokens
    receiver.__aexit__.assert_not_awaited()


# ---------------------------------------------------------------------------
# get_queue_depth (management plane)
# ---------------------------------------------------------------------------

async def test_get_queue_depth(backend):
    props = MagicMock(active_message_count=7)
    admin = AsyncMock()
    admin.get_queue_runtime_properties = AsyncMock(return_value=props)
    admin.__aenter__ = AsyncMock(return_value=admin)
    admin.__aexit__ = AsyncMock(return_value=None)

    with patch(
        "azure.servicebus.aio.management.ServiceBusAdministrationClient"
    ) as Admin:
        Admin.from_connection_string.return_value = admin
        depth = await backend.get_queue_depth()

    assert depth == 7
    admin.get_queue_runtime_properties.assert_awaited_once_with("test-queue")


async def test_get_queue_depth_uses_namespace_and_credential():
    credential = object()
    backend = AzureServiceBusBackend(
        "q", fully_qualified_namespace="ns.servicebus.windows.net", credential=credential
    )
    props = MagicMock(active_message_count=0)
    admin = AsyncMock()
    admin.get_queue_runtime_properties = AsyncMock(return_value=props)
    admin.__aenter__ = AsyncMock(return_value=admin)
    admin.__aexit__ = AsyncMock(return_value=None)

    with patch(
        "azure.servicebus.aio.management.ServiceBusAdministrationClient"
    ) as Admin:
        Admin.return_value = admin
        depth = await backend.get_queue_depth()

    assert depth == 0
    # Namespace path constructs the client directly with the async credential.
    Admin.assert_called_once_with("ns.servicebus.windows.net", credential=credential)
