from abc import ABC, abstractmethod


class SecretBackend(ABC):
    """Abstract base class for cloud secret management backends.

    Backends hold long-lived async clients. Use ``await backend.close()`` (or
    ``async with backend:``) to release sockets cleanly.
    """

    @abstractmethod
    async def get_secret(self, name: str) -> str:
        """Retrieve the plaintext value of a secret by name."""

    @abstractmethod
    async def get_secret_json(self, name: str) -> dict:
        """Retrieve a secret and parse its value as JSON."""

    @abstractmethod
    async def set_secret(self, name: str, value: str) -> None:
        """Create or update a secret."""

    @abstractmethod
    async def delete_secret(self, name: str) -> None:
        """Delete a secret by name."""

    @abstractmethod
    async def list_secrets(self, prefix: str = "") -> list[str]:
        """List secret names, optionally filtered by prefix."""

    async def health_check(self) -> bool:
        """Return True if the secret store is reachable."""
        try:
            await self.list_secrets(prefix="__cloudrift_health__")
            return True
        except Exception:
            return False

    async def close(self) -> None:
        """Close the underlying client and release sockets. Default is a no-op."""

    async def __aenter__(self) -> "SecretBackend":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()
