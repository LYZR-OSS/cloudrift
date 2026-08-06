import asyncio
import json

from google.api_core.exceptions import (
    AlreadyExists,
    GoogleAPICallError,
    NotFound,
    PermissionDenied,
)
from google.cloud.secretmanager import SecretManagerServiceAsyncClient

from cloudrift.core.exceptions import SecretError, SecretNotFoundError, SecretPermissionError
from cloudrift.secrets.base import SecretBackend


class GCPSecretManagerBackend(SecretBackend):
    """Google Cloud Secret Manager backend (native async via the GAPIC client).

    A single async client is created lazily on first use and reused for the
    lifetime of the backend.

    Use one of the class methods to construct:
    - ``from_application_default``  — ADC: GKE Workload Identity / Cloud Run / gcloud
    - ``from_service_account_file`` — service-account JSON key file
    - ``from_service_account_info`` — service-account JSON held in memory

    Secret *versions are immutable* on GCP, which changes the write semantics
    relative to AWS and Azure: :meth:`set_secret` adds a new version rather than
    overwriting, and reads resolve the ``latest`` alias. The names this backend
    takes are bare secret IDs — it builds the fully-qualified
    ``projects/<project>/secrets/<id>`` resource paths itself, so callers stay
    provider-neutral.
    """

    def __init__(
        self,
        project: str,
        *,
        credentials=None,
        replication: dict | None = None,
        client_options: dict | None = None,
    ) -> None:
        self.project = project
        self._credentials = credentials
        # Applied when set_secret() has to create the secret. Automatic
        # replication is the documented default; pass a user-managed policy to
        # pin secret material to specific regions for data-residency rules.
        self._replication = replication or {"automatic": {}}
        self._client_options = client_options or {}
        self._client: SecretManagerServiceAsyncClient | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_application_default(
        cls,
        project: str,
        prefer_metadata: bool = False,
        **kwargs,
    ) -> "GCPSecretManagerBackend":
        """Authenticate via Application Default Credentials.

        ``prefer_metadata=True`` reads the attached service account straight from
        the metadata server so an ambient ``GOOGLE_APPLICATION_CREDENTIALS``
        cannot shadow the workload's identity — see
        :mod:`cloudrift.core.gcp_credentials`.
        """
        from cloudrift.core.gcp_credentials import build_credentials

        return cls(
            project,
            credentials=build_credentials(prefer_metadata=prefer_metadata),
            **kwargs,
        )

    @classmethod
    def from_service_account_file(
        cls,
        project: str,
        service_account_file: str,
        **kwargs,
    ) -> "GCPSecretManagerBackend":
        """Authenticate with a service-account JSON key file."""
        from cloudrift.core.gcp_credentials import build_credentials

        return cls(
            project,
            credentials=build_credentials(service_account_file=service_account_file),
            **kwargs,
        )

    @classmethod
    def from_service_account_info(
        cls,
        project: str,
        service_account_info: dict,
        **kwargs,
    ) -> "GCPSecretManagerBackend":
        """Authenticate with parsed service-account JSON (never touches disk)."""
        from cloudrift.core.gcp_credentials import build_credentials

        return cls(
            project,
            credentials=build_credentials(service_account_info=service_account_info),
            **kwargs,
        )

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _ensure(self) -> SecretManagerServiceAsyncClient:
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client = SecretManagerServiceAsyncClient(
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
    # Resource paths
    # ------------------------------------------------------------------

    @property
    def _parent(self) -> str:
        return f"projects/{self.project}"

    def _secret_path(self, name: str) -> str:
        return f"{self._parent}/secrets/{name}"

    # ------------------------------------------------------------------
    # SecretBackend implementation
    # ------------------------------------------------------------------

    async def get_secret(self, name: str, version: str = "latest") -> str:
        """Retrieve the plaintext of a secret version (default ``latest``)."""
        client = await self._ensure()
        try:
            response = await client.access_secret_version(
                name=f"{self._secret_path(name)}/versions/{version}"
            )
            return response.payload.data.decode("utf-8")
        except GoogleAPICallError as e:
            self._raise(e, name)

    async def get_secret_json(self, name: str) -> dict:
        raw = await self.get_secret(name)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError) as e:
            raise SecretError(f"Secret '{name}' is not valid JSON") from e

    async def set_secret(self, name: str, value: str) -> None:
        """Add a new version holding ``value``, creating the secret if absent.

        GCP secret versions are immutable, so this never overwrites: the old
        version stays readable by explicit version number and ``latest`` moves
        forward. The create-on-``NotFound`` fallback mirrors the AWS backend's
        ``put_secret_value`` → ``create_secret`` path.
        """
        client = await self._ensure()
        payload = {"data": value.encode("utf-8")}
        try:
            await client.add_secret_version(parent=self._secret_path(name), payload=payload)
            return
        except NotFound:
            pass
        except GoogleAPICallError as e:
            self._raise(e, name)
        try:
            await client.create_secret(
                parent=self._parent,
                secret_id=name,
                secret={"replication": self._replication},
            )
        except AlreadyExists:
            # Lost a create race with another writer — the secret now exists, so
            # adding the version below still yields the caller's intended state.
            pass
        except GoogleAPICallError as e:
            self._raise(e, name)
        try:
            await client.add_secret_version(parent=self._secret_path(name), payload=payload)
        except GoogleAPICallError as e:
            self._raise(e, name)

    async def delete_secret(self, name: str) -> None:
        """Delete a secret and every version it holds."""
        client = await self._ensure()
        try:
            await client.delete_secret(name=self._secret_path(name))
        except GoogleAPICallError as e:
            self._raise(e, name)

    async def list_secrets(self, prefix: str = "") -> list[str]:
        """List secret IDs, optionally filtered by prefix.

        The prefix is applied client-side: Secret Manager's ``filter`` supports
        substring matching on ``name``, not anchored prefix matching, so
        filtering server-side would also return secrets that merely *contain*
        the prefix.
        """
        client = await self._ensure()
        try:
            names: list[str] = []
            pager = await client.list_secrets(parent=self._parent)
            async for secret in pager:
                secret_id = secret.name.rsplit("/", 1)[-1]
                if not prefix or secret_id.startswith(prefix):
                    names.append(secret_id)
            return names
        except GoogleAPICallError as e:
            self._raise(e, prefix)

    async def health_check(self) -> bool:
        try:
            client = await self._ensure()
            pager = await client.list_secrets(parent=self._parent)
            # Touch the first page only — the async pager is lazy, so awaiting
            # the call above alone would not prove the API is reachable.
            async for _ in pager:
                break
            return True
        except Exception:
            return False

    def _raise(self, exc: GoogleAPICallError, name: str):
        if isinstance(exc, NotFound):
            raise SecretNotFoundError(f"Secret not found: {name}") from exc
        if isinstance(exc, PermissionDenied):
            raise SecretPermissionError(f"Access denied for secret: {name}") from exc
        raise SecretError(str(exc)) from exc
