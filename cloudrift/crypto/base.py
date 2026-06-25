import base64
from abc import ABC, abstractmethod


class CryptoBackend(ABC):
    """Abstract base class for cloud key-management crypto backends.

    Provides provider-agnostic encrypt/decrypt against a *managed key* — AWS KMS
    or an Azure Key Vault key. Subclasses implement the raw byte operations
    (:meth:`encrypt` / :meth:`decrypt`); this base adds base64 string helpers for
    the common "encrypt a token, store the ciphertext as text" case.

    Ciphertext is the provider's native format (it is NOT re-wrapped by
    cloudrift), so values encrypted by the equivalent native SDK call remain
    decryptable through this backend and vice-versa.

    Payload size limits are provider-specific:
      - AWS KMS symmetric keys accept up to 4 KB of plaintext.
      - Azure Key Vault RSA keys (RSA-OAEP-256) accept far less — roughly
        190 bytes for RSA-2048, ~446 bytes for RSA-4096.
    For larger payloads, use this backend to wrap a random data key and encrypt
    the payload yourself (envelope encryption).

    Backends hold long-lived async clients. Use ``await backend.close()`` (or
    ``async with backend:``) to release them cleanly.
    """

    @abstractmethod
    async def encrypt(self, plaintext: bytes) -> bytes:
        """Encrypt raw bytes with the managed key; return raw ciphertext bytes."""

    @abstractmethod
    async def decrypt(self, ciphertext: bytes) -> bytes:
        """Decrypt raw ciphertext bytes; return the original plaintext bytes."""

    async def encrypt_str(self, plaintext: str) -> str:
        """Encrypt a UTF-8 string, returning base64-encoded ciphertext.

        An empty/None input returns ``""`` (no crypto call), mirroring the common
        "encrypt this optional token" pattern.
        """
        if not plaintext:
            return ""
        return base64.b64encode(await self.encrypt(plaintext.encode("utf-8"))).decode("ascii")

    async def decrypt_str(self, ciphertext_b64: str) -> str:
        """Decrypt base64 ciphertext produced by :meth:`encrypt_str` to a string."""
        if not ciphertext_b64:
            return ""
        return (await self.decrypt(base64.b64decode(ciphertext_b64))).decode("utf-8")

    async def close(self) -> None:
        """Close the underlying client and release sockets. Default is a no-op."""

    async def __aenter__(self) -> "CryptoBackend":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()
