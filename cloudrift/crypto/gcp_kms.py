import asyncio

from google.api_core.exceptions import (
    FailedPrecondition,
    GoogleAPICallError,
    NotFound,
    PermissionDenied,
)
from google.cloud.kms import KeyManagementServiceAsyncClient

from cloudrift.core.exceptions import (
    CryptoError,
    CryptoKeyNotFoundError,
    CryptoPermissionError,
)
from cloudrift.crypto.base import CryptoBackend


class GCPKMSBackend(CryptoBackend):
    """Google Cloud KMS crypto backend (native async via the GAPIC client).

    A single async client is created lazily on first use and reused for the
    lifetime of the backend.

    Use one of the class methods to construct:
    - ``from_application_default``  — ADC: GKE Workload Identity / Cloud Run / gcloud
    - ``from_service_account_file`` — service-account JSON key file
    - ``from_service_account_info`` — service-account JSON held in memory

    ``key_id`` is a full CryptoKey resource name::

        projects/<p>/locations/<l>/keyRings/<r>/cryptoKeys/<k>

    Cloud KMS symmetric keys accept up to **64 KiB** of plaintext, considerably
    more headroom than AWS KMS's 4 KB or a Key Vault RSA key's ~190–446 bytes,
    but envelope encryption is still the right pattern above that.

    ``additional_authenticated_data`` is the analog of an AWS KMS encryption
    context: it is bound into the ciphertext and must match on decrypt.
    """

    def __init__(
        self,
        key_id: str | None = None,
        *,
        credentials=None,
        additional_authenticated_data: bytes | None = None,
        client_options: dict | None = None,
    ) -> None:
        self._key_id = key_id
        self._credentials = credentials
        self._aad = additional_authenticated_data
        self._client_options = client_options or {}
        self._client: KeyManagementServiceAsyncClient | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_application_default(
        cls,
        key_id: str | None = None,
        prefer_metadata: bool = False,
        **kwargs,
    ) -> "GCPKMSBackend":
        """Authenticate via Application Default Credentials.

        ``prefer_metadata=True`` reads the attached service account straight from
        the metadata server — see :mod:`cloudrift.core.gcp_credentials`.
        """
        from cloudrift.core.gcp_credentials import build_credentials

        return cls(
            key_id,
            credentials=build_credentials(prefer_metadata=prefer_metadata),
            **kwargs,
        )

    @classmethod
    def from_service_account_file(
        cls,
        service_account_file: str,
        key_id: str | None = None,
        **kwargs,
    ) -> "GCPKMSBackend":
        """Authenticate with a service-account JSON key file."""
        from cloudrift.core.gcp_credentials import build_credentials

        return cls(
            key_id,
            credentials=build_credentials(service_account_file=service_account_file),
            **kwargs,
        )

    @classmethod
    def from_service_account_info(
        cls,
        service_account_info: dict,
        key_id: str | None = None,
        **kwargs,
    ) -> "GCPKMSBackend":
        """Authenticate with parsed service-account JSON (never touches disk)."""
        from cloudrift.core.gcp_credentials import build_credentials

        return cls(
            key_id,
            credentials=build_credentials(service_account_info=service_account_info),
            **kwargs,
        )

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _ensure(self) -> KeyManagementServiceAsyncClient:
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client = KeyManagementServiceAsyncClient(
                    credentials=self._credentials,
                    **self._client_options,
                )
        return self._client

    async def close(self) -> None:
        from cloudrift.core.gcp_credentials import close_credentials

        client, self._client = self._client, None
        if client is not None:
            await client.transport.close()
        await close_credentials(self._credentials)

    # ------------------------------------------------------------------
    # CryptoBackend implementation
    # ------------------------------------------------------------------

    async def encrypt(self, plaintext: bytes) -> bytes:
        if not self._key_id:
            raise CryptoError("GCP KMS key_id is required to encrypt")
        client = await self._ensure()
        request: dict = {"name": self._key_id, "plaintext": plaintext}
        if self._aad:
            request["additional_authenticated_data"] = self._aad
        try:
            response = await client.encrypt(request=request)
            return response.ciphertext
        except GoogleAPICallError as e:
            self._raise(e)

    async def decrypt(self, ciphertext: bytes) -> bytes:
        """Decrypt with the configured CryptoKey.

        Unlike AWS KMS — whose ciphertext blob names its own key — Cloud KMS
        requires the key on decrypt too, so ``key_id`` is mandatory here.
        """
        if not self._key_id:
            raise CryptoError("GCP KMS key_id is required to decrypt")
        client = await self._ensure()
        request: dict = {"name": self._key_id, "ciphertext": ciphertext}
        if self._aad:
            request["additional_authenticated_data"] = self._aad
        try:
            response = await client.decrypt(request=request)
            return response.plaintext
        except GoogleAPICallError as e:
            self._raise(e)

    def _raise(self, exc: GoogleAPICallError):
        if isinstance(exc, NotFound):
            raise CryptoKeyNotFoundError(str(exc)) from exc
        if isinstance(exc, PermissionDenied):
            raise CryptoPermissionError(str(exc)) from exc
        if isinstance(exc, FailedPrecondition):
            # Raised when the key version is disabled/destroyed or the key's
            # purpose does not allow encrypt/decrypt — the key exists but is
            # unusable, which is what KeyUnavailableException means on AWS.
            raise CryptoKeyNotFoundError(str(exc)) from exc
        raise CryptoError(str(exc)) from exc
