from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class Message:
    id: str
    body: dict
    receipt_handle: str
    attributes: dict = field(default_factory=dict)


class MessagingBackend(ABC):
    """Abstract base class for cloud messaging/queue backends.

    Backends hold long-lived async clients. Use ``await backend.close()`` (or
    ``async with backend:``) to release sockets cleanly.
    """

    @abstractmethod
    async def send(self, message: dict, delay: int = 0) -> str:
        """Send a message. Returns the message ID."""

    @abstractmethod
    async def send_batch(self, messages: list[dict]) -> list[str]:
        """Send multiple messages. Returns list of message IDs."""

    @abstractmethod
    async def receive(self, max_messages: int = 1, wait_time: int = 0) -> list[Message]:
        """Receive messages. wait_time is long-poll duration in seconds."""

    @abstractmethod
    async def delete(self, receipt_handle: str) -> None:
        """Delete/acknowledge a message by its receipt handle."""

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
