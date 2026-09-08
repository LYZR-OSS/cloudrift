import asyncio
import os
import struct

from azure.core.exceptions import (
    ClientAuthenticationError,
    ResourceNotFoundError,
)
from azure.identity.aio import ClientSecretCredential
from azure.keyvault.keys.crypto import EncryptionAlgorithm
from azure.keyvault.keys.crypto.aio import CryptographyClient
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from cloudrift.core.exceptions import (
    CryptoError,
    CryptoKeyNotFoundError,
    CryptoPermissionError,
)
from cloudrift.crypto.base import CryptoBackend

# Envelope framing. RSA can only encrypt a payload smaller than the key (~190
# bytes for RSA-2048 with OAEP-SHA256), which is far too small for real secrets
# like OAuth tokens. So we envelope-encrypt: a random AES-256 data key encrypts
# the payload (no size limit), and only that 32-byte key is RSA-wrapped by the
# Key Vault key. The magic prefix lets decrypt tell an enveloped blob from a
# legacy direct-RSA ciphertext written before this change.
_ENVELOPE_MAGIC = b"CRV1"
_DATA_KEY_BYTES = 32  # AES-256
_NONCE_BYTES = 12  # AES-GCM standard nonce length


class AzureKeyVaultKeysBackend(CryptoBackend):
    """Azure Key Vault *keys* crypto backend — the analog of AWS KMS.

    Encrypts/decrypts against a Key Vault key via ``CryptographyClient``.
    ``key_id`` is the full key identifier URL, e.g.
    ``https://myvault.vault.azure.net/keys/mykey`` (or pinned to a version
    ``.../keys/mykey/<version>``).

    The default algorithm is ``RSA-OAEP-256`` (RSA keys). RSA has a small payload
    ceiling (~190 bytes for RSA-2048), so ``encrypt`` uses **envelope
    encryption** — a random AES-256 data key encrypts the payload and only that
    key is RSA-wrapped by the Key Vault key — which removes the size limit.
    ``decrypt`` also transparently reads legacy direct-RSA ciphertexts written
    before envelope mode. ``algorithm=`` selects a different key/wrap algorithm.

    Construct via:
    - ``from_service_principal`` — tenant_id / client_id / client_secret
    - ``from_managed_identity``  — workload identity → managed identity → az CLI
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
        credential_options: dict | None = None,
        **kwargs,
    ) -> "AzureKeyVaultKeysBackend":
        """Authenticate via Azure AD: workload identity → managed identity → az CLI.

        ``client_id`` selects a user-assigned managed identity; omit it for the
        system-assigned one. ``credential_options`` is forwarded to
        ``DefaultAzureCredential`` — see :mod:`cloudrift.core.azure_credentials`.
        (A dict rather than ``**kwargs`` here because ``**kwargs`` already
        carries backend options such as ``algorithm``.)
        """
        from cloudrift.core.azure_credentials import build_async_credential

        credential = build_async_credential(client_id, **(credential_options or {}))
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
        """Envelope-encrypt ``plaintext`` so any size works despite RSA's ceiling.

        A fresh AES-256 key encrypts the payload with AES-GCM; only that 32-byte
        key is RSA-wrapped by the Key Vault key. The returned blob is
        ``magic | uint16 wrapped_len | wrapped_key | nonce | aes_gcm_body``.
        """
        client = await self._ensure()
        data_key = os.urandom(_DATA_KEY_BYTES)
        nonce = os.urandom(_NONCE_BYTES)
        try:
            body = AESGCM(data_key).encrypt(nonce, plaintext, None)
            wrapped = (await client.encrypt(self._algorithm, data_key)).ciphertext
        except Exception as e:
            self._raise(e)
        return b"".join((_ENVELOPE_MAGIC, struct.pack(">H", len(wrapped)), wrapped, nonce, body))

    async def decrypt(self, ciphertext: bytes) -> bytes:
        client = await self._ensure()
        if ciphertext[: len(_ENVELOPE_MAGIC)] == _ENVELOPE_MAGIC:
            return await self._decrypt_envelope(client, ciphertext)
        # Legacy: payloads written before envelope mode were RSA-encrypted whole.
        try:
            return (await client.decrypt(self._algorithm, ciphertext)).plaintext
        except Exception as e:
            self._raise(e)

    async def _decrypt_envelope(self, client: CryptographyClient, ciphertext: bytes) -> bytes:
        off = len(_ENVELOPE_MAGIC)
        (wrapped_len,) = struct.unpack_from(">H", ciphertext, off)
        off += 2
        wrapped = ciphertext[off : off + wrapped_len]
        off += wrapped_len
        nonce = ciphertext[off : off + _NONCE_BYTES]
        off += _NONCE_BYTES
        body = ciphertext[off:]
        try:
            data_key = (await client.decrypt(self._algorithm, wrapped)).plaintext
            return AESGCM(data_key).decrypt(nonce, body, None)
        except Exception as e:
            self._raise(e)

    def _raise(self, exc: Exception):
        if isinstance(exc, ResourceNotFoundError):
            raise CryptoKeyNotFoundError(str(exc)) from exc
        if isinstance(exc, ClientAuthenticationError):
            raise CryptoPermissionError(str(exc)) from exc
        raise CryptoError(str(exc)) from exc
