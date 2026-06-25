import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class OutgoingMessage:
    """A message to send via :meth:`MessagingBackend.send_batch`.

    ``body`` is the raw payload bytes; ``attributes`` is an optional flat map of
    string metadata that maps to SQS ``MessageAttributes`` (String type) and
    Service Bus ``application_properties``.
    """

    body: bytes
    attributes: dict[str, str] | None = None


@dataclass
class Message:
    id: str
    body: bytes
    receipt_handle: str
    attributes: dict[str, str] = field(default_factory=dict)
    group_id: str | None = None
    dedup_id: str | None = None
    receive_count: int | None = None

    def json(self):
        """Decode the raw ``body`` bytes as JSON.

        Convenience for the common case where the payload was sent with
        :func:`send_json`. Raises ``json.JSONDecodeError`` if the body is not
        valid JSON.
        """
        return json.loads(self.body)


class MessagingBackend(ABC):
    """Abstract base class for cloud messaging/queue backends.

    The primitive payload is **raw bytes** plus an optional flat ``attributes``
    map (string → string). JSON users should use the :func:`send_json` helper
    and :meth:`Message.json` to (de)serialize without touching the byte layer.

    Backends hold long-lived async clients. Use ``await backend.close()`` (or
    ``async with backend:``) to release sockets cleanly.

    FIFO / ordered queues: ``group_id`` maps to SQS ``MessageGroupId`` and
    Service Bus ``session_id``; ``dedup_id`` maps to SQS
    ``MessageDeduplicationId`` and Service Bus ``message_id`` (effective only
    when the queue has duplicate detection enabled).
    """

    @abstractmethod
    async def send(
        self,
        body: bytes,
        attributes: dict[str, str] | None = None,
        delay: int = 0,
        *,
        group_id: str | None = None,
        dedup_id: str | None = None,
    ) -> str:
        """Send a raw-bytes message with optional attributes. Returns the message ID.

        ``attributes`` map to SQS ``MessageAttributes`` (String type) /
        Service Bus ``application_properties``. group_id/dedup_id apply to FIFO
        (SQS) or session-enabled (Service Bus) queues. SQS FIFO does not support
        per-message ``delay``.
        """

    @abstractmethod
    async def send_batch(
        self,
        messages: list[OutgoingMessage],
        *,
        group_id: str | None = None,
        dedup_ids: list[str] | None = None,
    ) -> list[str]:
        """Send multiple :class:`OutgoingMessage`. Returns list of message IDs.

        ``group_id`` applies to every message; ``dedup_ids``, if given, must be
        parallel to ``messages``.
        """

    @abstractmethod
    async def receive(
        self,
        max_messages: int = 1,
        wait_time: int = 0,
        *,
        group_id: str | None = None,
        visibility_timeout: int | None = None,
    ) -> list[Message]:
        """Receive messages. wait_time is long-poll duration in seconds.

        Each :class:`Message` carries the raw ``body`` bytes and an
        ``attributes`` map (string → string) populated from the provider's
        message attributes / application properties.

        ``group_id`` receives from a specific session (Service Bus only; SQS
        cannot filter by group). ``visibility_timeout`` overrides the queue's
        visibility timeout on SQS; ignored on Service Bus (lock duration is
        queue-level configuration).
        """

    @abstractmethod
    async def delete(self, receipt_handle: str) -> None:
        """Delete/acknowledge a message by its receipt handle."""

    async def nack(self, receipt_handle: str) -> None:
        """Return a message to the queue for immediate redelivery."""
        raise NotImplementedError(f"{type(self).__name__} does not support nack()")

    @abstractmethod
    async def dead_letter(self, receipt_handle: str, reason: str) -> None:
        """Move a received message to the dead-letter queue and acknowledge it.

        Args:
            receipt_handle: The receipt handle from a previously received message.
            reason: A human-readable reason recorded with the dead-lettered message.

        Azure Service Bus implements this natively via ``dead_letter_message``.
        SQS has no native per-message dead-letter API, so backends emulate it by
        sending the message body to a configured dead-letter queue and then
        deleting the original from the source queue.
        """

    @abstractmethod
    async def get_queue_depth(self) -> int:
        """Return the approximate number of messages waiting in the queue.

        This is an estimate: cloud queues report it asynchronously and it may
        lag in-flight (received-but-not-yet-deleted) messages.
        """

    @abstractmethod
    async def purge(self) -> None:
        """Delete all messages in the queue."""

    @abstractmethod
    async def health_check(self) -> bool:
        """Return True if the messaging backend is reachable."""

    async def close(self) -> None:
        """Close the underlying client and release sockets. Default is a no-op."""

    async def __aenter__(self) -> "MessagingBackend":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


async def send_json(
    backend: MessagingBackend,
    message: dict,
    attributes: dict[str, str] | None = None,
    delay: int = 0,
    *,
    group_id: str | None = None,
    dedup_id: str | None = None,
) -> str:
    """Serialize ``message`` to JSON bytes and send it via ``backend``.

    Backend-agnostic convenience wrapper around :meth:`MessagingBackend.send`
    for the common JSON-payload case. Decode the received body with
    :meth:`Message.json`.
    """
    return await backend.send(
        json.dumps(message).encode(),
        attributes,
        delay,
        group_id=group_id,
        dedup_id=dedup_id,
    )
