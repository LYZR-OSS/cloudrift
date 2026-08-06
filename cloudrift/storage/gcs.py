import asyncio
from datetime import datetime

from aiohttp import ClientResponseError
from gcloud.aio.storage import Storage

from cloudrift.core.exceptions import ObjectNotFoundError, StorageError, StoragePermissionError
from cloudrift.storage.base import StorageBackend

#: GCS caps signed-URL lifetime at 7 days.
_MAX_SIGNED_URL_EXPIRY = 604800


class GCSClient:
    """Project-scoped Google Cloud Storage client.

    Owns one ``gcloud-aio-storage`` ``Storage`` instance, and with it a single
    aiohttp connection pool that serves every bucket, so callers using multiple
    buckets share one pool.

    Use ``client.bucket(name)`` to get a per-bucket :class:`StorageBackend`
    handle. Call ``await client.close()`` (or ``async with client:``) once when
    you're done — that tears down the pool for every view issued from it.

    Use one of the class methods to construct:
    - ``from_application_default``  — ADC: GKE Workload Identity / Cloud Run / gcloud
    - ``from_service_account_file`` — service-account JSON key file
    - ``from_service_account_info`` — service-account JSON held in memory

    Note on ``prefer_metadata``: unlike the other GCP backends, the storage
    factories do not accept it. ``gcloud-aio-storage`` resolves the ADC chain
    internally with no injection point, so there is no way to skip the
    ``GOOGLE_APPLICATION_CREDENTIALS`` step — passing the flag raises
    ``TypeError`` rather than being silently ignored. Pin the identity by
    passing the service account explicitly instead. See
    :mod:`cloudrift.core.gcp_credentials`.
    """

    def __init__(
        self,
        storage: Storage,
        *,
        signer_service_account_email: str | None = None,
    ) -> None:
        self._storage = storage
        # Required to sign URLs when the credential has no local private key
        # (Workload Identity / metadata / impersonation), where signing goes
        # through the IAM signBlob API instead.
        self._signer_email = signer_service_account_email
        self._iam_client = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_application_default(
        cls,
        api_root: str | None = None,
        signer_service_account_email: str | None = None,
    ) -> "GCSClient":
        """Authenticate via Application Default Credentials.

        Args:
            api_root: Override the API host — set this to point at a local
                emulator (``fake-gcs-server``). The library also honors the
                ``STORAGE_EMULATOR_HOST`` environment variable, and skips
                authentication entirely when either is set.
            signer_service_account_email: Service account to sign as in
                :meth:`GCSBackend.presigned_url`. Required under Workload
                Identity, where there is no local private key to sign with.
        """
        return cls(
            Storage(api_root=api_root),
            signer_service_account_email=signer_service_account_email,
        )

    @classmethod
    def from_service_account_file(
        cls,
        service_account_file: str,
        api_root: str | None = None,
        signer_service_account_email: str | None = None,
    ) -> "GCSClient":
        """Authenticate with a service-account JSON key file.

        The key's own ``client_email`` signs URLs locally, so
        ``signer_service_account_email`` is not needed unless you want to sign
        as a *different* account via impersonation.
        """
        from cloudrift.core.gcp_credentials import build_storage_token_kwargs

        return cls(
            Storage(
                api_root=api_root,
                **build_storage_token_kwargs(service_account_file=service_account_file),
            ),
            signer_service_account_email=signer_service_account_email,
        )

    @classmethod
    def from_service_account_info(
        cls,
        service_account_info: dict,
        api_root: str | None = None,
        signer_service_account_email: str | None = None,
    ) -> "GCSClient":
        """Authenticate with parsed service-account JSON (never touches disk)."""
        from cloudrift.core.gcp_credentials import build_storage_token_kwargs

        return cls(
            Storage(
                api_root=api_root,
                **build_storage_token_kwargs(service_account_info=service_account_info),
            ),
            signer_service_account_email=signer_service_account_email,
        )

    # ------------------------------------------------------------------
    # Bucket view factory
    # ------------------------------------------------------------------

    def bucket(self, name: str) -> "GCSBackend":
        """Return a :class:`StorageBackend` view bound to ``name``.

        The view shares this client's connection pool. ``await view.close()``
        is a no-op — call ``await client.close()`` to release sockets.
        """
        return GCSBackend(name, self)

    # ------------------------------------------------------------------
    # Signing
    # ------------------------------------------------------------------

    async def _ensure_iam_client(self):
        """Return a lazily-built ``IamClient`` for signBlob-based URL signing.

        Built on first use rather than at construction time because it rejects
        user credentials (``gcloud auth application-default login``): eager
        construction would break every local-dev caller, including the ones that
        never sign a URL.
        """
        if self._iam_client is not None:
            return self._iam_client
        async with self._lock:
            if self._iam_client is None:
                from gcloud.aio.auth import IamClient

                try:
                    # Share the Storage session so signing does not open a
                    # second connection pool.
                    self._iam_client = IamClient(
                        token=self._storage.token,
                        session=self._storage.session.session,
                    )
                except TypeError as e:
                    raise StorageError(
                        "presigned_url requires a service-account or metadata-server "
                        "credential. User credentials from `gcloud auth "
                        "application-default login` cannot sign URLs — use a service "
                        "account key, or run where a service account is attached."
                    ) from e
        return self._iam_client

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        # The IamClient shares the Storage session, so closing Storage releases
        # both; drop the reference so a reopened client rebuilds it.
        self._iam_client = None
        await self._storage.close()

    async def __aenter__(self) -> "GCSClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class GCSBackend(StorageBackend):
    """Per-bucket :class:`StorageBackend` view over a :class:`GCSClient`.

    Holds only ``(client, bucket)`` — all I/O delegates to the shared client.
    ``close()`` is a no-op for views obtained from ``client.bucket(...)``; the
    project client owns the socket lifecycle.

    Views obtained from :func:`cloudrift.storage.get_storage` own their
    underlying client and *do* tear it down on ``close()`` (matching the S3 and
    Azure Blob backends).
    """

    def __init__(
        self,
        bucket: str,
        client: GCSClient,
        *,
        owns_client: bool = False,
    ) -> None:
        self.bucket = bucket
        self._client = client
        self._owns_client = owns_client

    # ------------------------------------------------------------------
    # Single-bucket factory constructors
    # ------------------------------------------------------------------
    # These build a one-shot client that the returned view owns. Prefer
    # ``GCSClient.from_*`` + ``client.bucket(...)`` when you want to share a
    # connection pool across buckets.

    @classmethod
    def from_application_default(
        cls,
        bucket: str,
        api_root: str | None = None,
        signer_service_account_email: str | None = None,
    ) -> "GCSBackend":
        client = GCSClient.from_application_default(
            api_root=api_root,
            signer_service_account_email=signer_service_account_email,
        )
        return cls(bucket, client, owns_client=True)

    @classmethod
    def from_service_account_file(
        cls,
        bucket: str,
        service_account_file: str,
        api_root: str | None = None,
        signer_service_account_email: str | None = None,
    ) -> "GCSBackend":
        client = GCSClient.from_service_account_file(
            service_account_file,
            api_root=api_root,
            signer_service_account_email=signer_service_account_email,
        )
        return cls(bucket, client, owns_client=True)

    @classmethod
    def from_service_account_info(
        cls,
        bucket: str,
        service_account_info: dict,
        api_root: str | None = None,
        signer_service_account_email: str | None = None,
    ) -> "GCSBackend":
        client = GCSClient.from_service_account_info(
            service_account_info,
            api_root=api_root,
            signer_service_account_email=signer_service_account_email,
        )
        return cls(bucket, client, owns_client=True)

    # ------------------------------------------------------------------
    # Convenience accessor
    # ------------------------------------------------------------------

    @property
    def _storage(self) -> Storage:
        return self._client._storage

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        if self._owns_client:
            await self._client.close()

    # ------------------------------------------------------------------
    # StorageBackend implementation
    # ------------------------------------------------------------------

    async def upload(self, key: str, data: bytes, content_type: str | None = None) -> str:
        try:
            await self._storage.upload(self.bucket, key, data, content_type=content_type)
        except ClientResponseError as e:
            self._raise(e, key)
        return key

    async def download(self, key: str) -> bytes:
        try:
            return await self._storage.download(self.bucket, key)
        except ClientResponseError as e:
            self._raise(e, key)

    async def delete(self, key: str) -> None:
        try:
            await self._storage.delete(self.bucket, key)
        except ClientResponseError as e:
            self._raise(e, key)

    async def exists(self, key: str) -> bool:
        try:
            await self._storage.download_metadata(self.bucket, key)
            return True
        except ClientResponseError as e:
            if e.status == 404:
                return False
            self._raise(e, key)

    async def list(self, prefix: str = "") -> list[str]:
        return [key async for key in self.list_iter(prefix)]

    async def presigned_url(self, key: str, expires_in: int = 3600) -> str:
        """Generate a V4 signed URL.

        Signing needs a private key. A service-account key file has one and
        signs locally; a Workload Identity / metadata credential does not, and
        signs through the IAM ``signBlob`` API — which needs to be told *which*
        service account to sign as, hence ``signer_service_account_email`` on
        the client. This is the GCS analog of the Azure Blob backend requiring
        ``account_key``, except GCS can sign without a key at all.
        """
        if expires_in > _MAX_SIGNED_URL_EXPIRY:
            raise StorageError(
                f"expires_in must be at most {_MAX_SIGNED_URL_EXPIRY} seconds (7 days), "
                f"got {expires_in}."
            )
        blob = self._storage.get_bucket(self.bucket).new_blob(key)
        # `service_data` is how gcloud-aio's own signer decides local-vs-IAM;
        # checking it here lets us fail with an actionable message instead of
        # signing with the literal string "None" as the credential.
        has_private_key = bool(self._storage.token.service_data.get("private_key"))
        if not has_private_key and not self._client._signer_email:
            raise StorageError(
                "presigned_url requires signer_service_account_email when the "
                "credential has no private key (Workload Identity / metadata / "
                "gcloud ADC). Pass it to get_storage()/GCSClient, and grant the "
                "caller roles/iam.serviceAccountTokenCreator on that account."
            )
        try:
            if has_private_key:
                return await blob.get_signed_url(expires_in)
            return await blob.get_signed_url(
                expires_in,
                iam_client=await self._client._ensure_iam_client(),
                service_account_email=self._client._signer_email,
            )
        except ClientResponseError as e:
            self._raise(e, key)

    async def copy(self, src_key: str, dst_key: str, *, dst_bucket: str | None = None) -> str:
        try:
            await self._storage.copy(
                self.bucket,
                src_key,
                dst_bucket or self.bucket,
                new_name=dst_key,
            )
        except ClientResponseError as e:
            self._raise(e, src_key)
        return dst_key

    async def get_metadata(self, key: str) -> dict:
        try:
            props = await self._storage.download_metadata(self.bucket, key)
        except ClientResponseError as e:
            self._raise(e, key)
        size = props.get("size")
        return {
            "content_type": props.get("contentType"),
            # GCS reports size as a string in the JSON API; the other backends
            # return an int.
            "size": int(size) if size is not None else None,
            "last_modified": _parse_timestamp(props.get("updated")),
            "etag": props.get("etag"),
            "metadata": props.get("metadata", {}),
        }

    async def upload_stream(self, key, stream, content_type=None):
        """Upload from an async byte stream by accumulating chunks.

        ``gcloud-aio-storage`` takes bytes or a synchronous file object, not an
        async iterator, so this buffers — same trade-off as the S3 backend.
        """
        chunks = []
        async for chunk in stream:
            chunks.append(chunk)
        return await self.upload(key, b"".join(chunks), content_type=content_type)

    async def list_iter(self, prefix: str = ""):
        """Yield object keys lazily, one GCS page at a time (true pagination)."""
        params = {"prefix": prefix} if prefix else {}
        while True:
            try:
                page = await self._storage.list_objects(self.bucket, params=dict(params))
            except ClientResponseError as e:
                self._raise(e, prefix)
            for item in page.get("items", []):
                yield item["name"]
            token = page.get("nextPageToken")
            if not token:
                return
            params["pageToken"] = token

    def _raise(self, exc: ClientResponseError, key: str):
        if exc.status == 404:
            raise ObjectNotFoundError(f"Object not found: {key}") from exc
        if exc.status in (401, 403):
            raise StoragePermissionError(f"Access denied for key: {key}") from exc
        raise StorageError(str(exc)) from exc


def _parse_timestamp(value: str | None) -> datetime | str | None:
    """Parse a GCS RFC 3339 timestamp to a ``datetime``.

    S3 and Azure hand back ``datetime`` objects, so this keeps ``get_metadata``
    uniform. Falls back to the raw string if GCS ever returns a form
    ``fromisoformat`` cannot read — a metadata read should not fail over a
    timestamp.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return value
