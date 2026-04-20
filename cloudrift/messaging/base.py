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
