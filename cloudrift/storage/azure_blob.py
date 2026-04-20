from datetime import datetime, timedelta, timezone

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.storage.blob import BlobSasPermissions, ContentSettings, generate_blob_sas
from azure.storage.blob.aio import BlobServiceClient

from cloudrift.core.exceptions import ObjectNotFoundError, StorageError, StoragePermissionError
from cloudrift.storage.base import StorageBackend


class AzureBlobBackend(StorageBackend):
    """Azure Blob Storage backend (native async via ``azure.storage.blob.aio``).

    A single ``BlobServiceClient`` is held for the lifetime of the backend and
    reused across all operations. Call ``await backend.close()`` (or use
    ``async with backend:``) to release the underlying connections.

    Use one of the class methods to construct:
    - ``from_connection_string`` — shared-access connection string
    - ``from_account_key``       — storage account URL + account key
    - ``from_sas_token``         — storage account URL + SAS token
    - ``from_managed_identity``  — Azure Managed Identity (system or user-assigned)
    - ``from_service_principal`` — Azure AD service principal (client secret)
    """

    def __init__(
        self,
        service_client: BlobServiceClient,
        container: str,
        *,
        account_key: str | None = None,
        credential=None,
    ) -> None:
        self.container = container
        self._service = service_client
        self._account_key = account_key
        self._credential = credential

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_connection_string(cls, connection_string: str, container: str) -> "AzureBlobBackend":
        """Authenticate with an Azure Storage connection string."""
        # Parse account key from the connection string so presigned_url works.
        account_key = _parse_conn_string_field(connection_string, "AccountKey")
        return cls(
            BlobServiceClient.from_connection_string(connection_string),
            container,
            account_key=account_key,
        )

    @classmethod
    def from_account_key(
        cls,
        account_url: str,
        account_key: str,
        container: str,
    ) -> "AzureBlobBackend":
        """Authenticate with a storage account URL and account key."""
        return cls(
            BlobServiceClient(account_url, credential=account_key),
            container,
            account_key=account_key,
        )

    @classmethod
    def from_sas_token(
        cls,
        account_url: str,
        sas_token: str,
        container: str,
    ) -> "AzureBlobBackend":
        """Authenticate with a Shared Access Signature (SAS) token."""
        return cls(BlobServiceClient(account_url, credential=sas_token), container)

    @classmethod
    def from_managed_identity(
        cls,
        account_url: str,
        container: str,
        client_id: str | None = None,
    ) -> "AzureBlobBackend":
        """Authenticate via Azure Managed Identity (system or user-assigned)."""
        from azure.identity.aio import ManagedIdentityCredential

        credential = (
            ManagedIdentityCredential(client_id=client_id)
            if client_id
            else ManagedIdentityCredential()
        )
        return cls(
            BlobServiceClient(account_url, credential=credential),
            container,
            credential=credential,
        )

    @classmethod
    def from_service_principal(
        cls,
        account_url: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        container: str,
    ) -> "AzureBlobBackend":
        """Authenticate via Azure AD service principal (client secret)."""
        from azure.identity.aio import ClientSecretCredential

        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )
        return cls(
            BlobServiceClient(account_url, credential=credential),
            container,
            credential=credential,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        await self._service.close()
        if self._credential is not None and hasattr(self._credential, "close"):
            await self._credential.close()

    # ------------------------------------------------------------------
    # StorageBackend implementation
    # ------------------------------------------------------------------

    async def upload(self, key: str, data: bytes, content_type: str | None = None) -> str:
        blob = self._service.get_blob_client(self.container, key)
        try:
            content_settings = ContentSettings(content_type=content_type) if content_type else None
            await blob.upload_blob(data, overwrite=True, content_settings=content_settings)
        except HttpResponseError as e:
            self._raise(e, key)
        return key

    async def download(self, key: str) -> bytes:
        blob = self._service.get_blob_client(self.container, key)
        try:
            stream = await blob.download_blob()
            return await stream.readall()
        except ResourceNotFoundError as e:
            raise ObjectNotFoundError(f"Object not found: {key}") from e
        except HttpResponseError as e:
            self._raise(e, key)

    async def delete(self, key: str) -> None:
        blob = self._service.get_blob_client(self.container, key)
        try:
            await blob.delete_blob()
        except ResourceNotFoundError as e:
            raise ObjectNotFoundError(f"Object not found: {key}") from e
        except HttpResponseError as e:
            self._raise(e, key)

    async def exists(self, key: str) -> bool:
        blob = self._service.get_blob_client(self.container, key)
        return await blob.exists()

    async def list(self, prefix: str = "") -> list[str]:
        container = self._service.get_container_client(self.container)
        try:
            return [
                blob.name async for blob in container.list_blobs(name_starts_with=prefix or None)
            ]
        except HttpResponseError as e:
            self._raise(e, prefix)

    async def presigned_url(self, key: str, expires_in: int = 3600) -> str:
        if not self._account_key:
            raise StorageError(
                "presigned_url requires account_key authentication. "
                "Use from_connection_string or from_account_key."
            )
        try:
            sas = generate_blob_sas(
                account_name=self._service.account_name,
                container_name=self.container,
                blob_name=key,
                account_key=self._account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.now(timezone.utc) + timedelta(seconds=expires_in),
            )
            return (
                f"https://{self._service.account_name}.blob.core.windows.net/"
                f"{self.container}/{key}?{sas}"
            )
        except HttpResponseError as e:
            self._raise(e, key)

    async def copy(self, src_key: str, dst_key: str) -> str:
        src_blob = self._service.get_blob_client(self.container, src_key)
        dst_blob = self._service.get_blob_client(self.container, dst_key)
        try:
            await dst_blob.start_copy_from_url(src_blob.url)
        except ResourceNotFoundError as e:
            raise ObjectNotFoundError(f"Object not found: {src_key}") from e
        except HttpResponseError as e:
            self._raise(e, src_key)
        return dst_key

    async def get_metadata(self, key: str) -> dict:
        blob = self._service.get_blob_client(self.container, key)
        try:
            props = await blob.get_blob_properties()
            return {
                "content_type": props.content_settings.content_type if props.content_settings else None,
                "size": props.size,
                "last_modified": props.last_modified,
                "etag": props.etag,
                "metadata": props.metadata or {},
            }
        except ResourceNotFoundError as e:
            raise ObjectNotFoundError(f"Object not found: {key}") from e
        except HttpResponseError as e:
            self._raise(e, key)

    async def upload_stream(self, key: str, stream, content_type: str | None = None) -> str:
        """Upload from an async byte stream (true streaming — no in-memory buffering)."""
        blob = self._service.get_blob_client(self.container, key)
        try:
            content_settings = ContentSettings(content_type=content_type) if content_type else None
            await blob.upload_blob(stream, overwrite=True, content_settings=content_settings)
        except HttpResponseError as e:
            self._raise(e, key)
        return key

    async def list_iter(self, prefix: str = ""):
        """Yield object keys lazily using Azure async iterator (true pagination)."""
        container = self._service.get_container_client(self.container)
        try:
            async for blob in container.list_blobs(name_starts_with=prefix or None):
                yield blob.name
        except HttpResponseError as e:
            self._raise(e, prefix)

    def _raise(self, exc: HttpResponseError, key: str):
        status = getattr(exc, "status_code", None)
        if status == 404:
            raise ObjectNotFoundError(f"Object not found: {key}") from exc
        if status == 403:
            raise StoragePermissionError(f"Access denied for key: {key}") from exc
        raise StorageError(str(exc)) from exc


def _parse_conn_string_field(conn_string: str, field: str) -> str | None:
    for part in conn_string.split(";"):
        if part.startswith(f"{field}="):
            return part[len(field) + 1 :]
    return None
