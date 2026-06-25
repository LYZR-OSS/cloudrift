import asyncio

from azure.core.exceptions import (
    ClientAuthenticationError,
    ResourceNotFoundError,
)
from azure.identity.aio import (
    ClientSecretCredential,
    DefaultAzureCredential,
    ManagedIdentityCredential,
)
from azure.keyvault.keys.crypto import EncryptionAlgorithm
from azure.keyvault.keys.crypto.aio import CryptographyClient

from cloudrift.core.exceptions import (
    CryptoError,
    CryptoKeyNotFoundError,
    CryptoPermissionError,
)
from cloudrift.crypto.base import CryptoBackend


class AzureKeyVaultKeysBackend(CryptoBackend):
    """Azure Key Vault *keys* crypto backend — the analog of AWS KMS.

    Encrypts/decrypts against a Key Vault key via ``CryptographyClient``.
    ``key_id`` is the full key identifier URL, e.g.
    ``https://myvault.vault.azure.net/keys/mykey`` (or pinned to a version
    ``.../keys/mykey/<version>``).

    The default algorithm is ``RSA-OAEP-256`` (RSA keys). RSA encryption has a
    small payload ceiling (~190 bytes for RSA-2048); pass ``algorithm=`` for a
    different key type, or wrap a data key for larger payloads.

    Construct via:
    - ``from_service_principal`` — tenant_id / client_id / client_secret
    - ``from_managed_identity``  — managed identity (optionally a client_id),
      else DefaultAzureCredential
    """

    def __init__(
        self,
        key_id: str,
        credential,
        *,
        algorithm: "EncryptionAlgorithm | None" = None,
    ) -> None:
        self._key_id = key_id
        self._credential = credential
        self._algorithm = algorithm or EncryptionAlgorithm.rsa_oaep_256
        self._client: CryptographyClient | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_service_principal(
        cls,
        key_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        **kwargs,
    ) -> "AzureKeyVaultKeysBackend":
        """Authenticate with an Azure AD service principal."""
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
        return cls(key_id, credential, **kwargs)

    @classmethod
    def from_managed_identity(
        cls,
        key_id: str,
        client_id: str | None = None,
        **kwargs,
    ) -> "AzureKeyVaultKeysBackend":
        """Authenticate via managed identity (or DefaultAzureCredential)."""
        credential = (
            ManagedIdentityCredential(client_id=client_id)
            if client_id
            else DefaultAzureCredential()
        )
        return cls(key_id, credential, **kwargs)

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _ensure(self) -> CryptographyClient:
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = CryptographyClient(self._key_id, self._credential)
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
        if self._credential is not None:
            await self._credential.close()

    # ------------------------------------------------------------------
    # CryptoBackend implementation
    # ------------------------------------------------------------------

    async def encrypt(self, plaintext: bytes) -> bytes:
        client = await self._ensure()
        try:
            result = await client.encrypt(self._algorithm, plaintext)
            return result.ciphertext
        except Exception as e:
            self._raise(e)

    async def decrypt(self, ciphertext: bytes) -> bytes:
        client = await self._ensure()
        try:
            result = await client.decrypt(self._algorithm, ciphertext)
            return result.plaintext
        except Exception as e:
            self._raise(e)

    def _raise(self, exc: Exception):
        if isinstance(exc, ResourceNotFoundError):
            raise CryptoKeyNotFoundError(str(exc)) from exc
        if isinstance(exc, ClientAuthenticationError):
            raise CryptoPermissionError(str(exc)) from exc
        raise CryptoError(str(exc)) from exc
