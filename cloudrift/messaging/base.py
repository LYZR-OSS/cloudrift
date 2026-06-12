from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class Message:
    id: str
    body: dict
    receipt_handle: str
    attributes: dict = field(default_factory=dict)
    group_id: str | None = None
    dedup_id: str | None = None
    receive_count: int | None = None


class MessagingBackend(ABC):
    """Abstract base class for cloud messaging/queue backends.

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
        message: dict,
        delay: int = 0,
        *,
        group_id: str | None = None,
        dedup_id: str | None = None,
    ) -> str:
        """Send a message. Returns the message ID.

        group_id/dedup_id apply to FIFO (SQS) or session-enabled (Service Bus)
        queues. SQS FIFO does not support per-message ``delay``.
        """

    @abstractmethod
    async def send_batch(
        self,
        messages: list[dict],
        *,
        group_id: str | None = None,
        dedup_ids: list[str] | None = None,
    ) -> list[str]:
        """Send multiple messages. Returns list of message IDs.

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
