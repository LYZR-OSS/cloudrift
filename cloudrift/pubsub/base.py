from abc import ABC, abstractmethod


class PubSubBackend(ABC):
    """Abstract base class for cloud pub/sub (topic-based) backends.

    Unlike ``MessagingBackend`` (point-to-point queues), pub/sub backends
    fan out messages to multiple subscribers via topics.

    Backends hold long-lived async clients. Use ``await backend.close()`` (or
    ``async with backend:``) to release sockets cleanly.
    """

    @abstractmethod
    async def publish(
        self, topic: str, message: str, attributes: dict | None = None
    ) -> str:
        """Publish a message to a topic. Returns the message ID."""

    @abstractmethod
    async def publish_batch(
        self, topic: str, messages: list[dict]
    ) -> list[str]:
        """Publish multiple messages to a topic. Returns list of message IDs.

        Each dict in *messages* should have ``message`` (str) and optionally
        ``attributes`` (dict).
        """

    async def health_check(self) -> bool:
        """Return True if the pub/sub backend is reachable."""
        # Subclasses should override with a lightweight check.
        return True

    async def close(self) -> None:
        """Close the underlying client and release sockets. Default is a no-op."""

    async def __aenter__(self) -> "PubSubBackend":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()
