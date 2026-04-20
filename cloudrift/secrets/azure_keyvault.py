import json

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError

from cloudrift.core.exceptions import SecretError, SecretNotFoundError, SecretPermissionError
from cloudrift.secrets.base import SecretBackend


class AzureKeyVaultBackend(SecretBackend):
    """Azure Key Vault secrets backend (native async via ``azure.keyvault.secrets.aio``).

    Use one of the class methods to construct:
    - ``from_managed_identity``  — Azure Managed Identity (system or user-assigned)
    - ``from_service_principal`` — Azure AD service principal (client secret)
    """

    def __init__(self, client, *, credential=None) -> None:
        self._client = client
        self._credential = credential

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_managed_identity(
        cls,
        vault_url: str,
        client_id: str | None = None,
    ) -> "AzureKeyVaultBackend":
        """Authenticate via Azure Managed Identity (system or user-assigned)."""
        from azure.identity.aio import ManagedIdentityCredential
        from azure.keyvault.secrets.aio import SecretClient

        credential = (
            ManagedIdentityCredential(client_id=client_id)
            if client_id
            else ManagedIdentityCredential()
        )
        return cls(SecretClient(vault_url=vault_url, credential=credential), credential=credential)

    @classmethod
    def from_service_principal(
        cls,
        vault_url: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ) -> "AzureKeyVaultBackend":
        """Authenticate via Azure AD service principal (client secret)."""
        from azure.identity.aio import ClientSecretCredential
        from azure.keyvault.secrets.aio import SecretClient

        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )
        return cls(SecretClient(vault_url=vault_url, credential=credential), credential=credential)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        await self._client.close()
        if self._credential is not None and hasattr(self._credential, "close"):
            await self._credential.close()

    # ------------------------------------------------------------------
    # SecretBackend implementation
    # ------------------------------------------------------------------

    async def get_secret(self, name: str) -> str:
        try:
            secret = await self._client.get_secret(name)
            return secret.value
        except ResourceNotFoundError as e:
            raise SecretNotFoundError(f"Secret not found: {name}") from e
        except HttpResponseError as e:
            self._raise(e, name)

    async def get_secret_json(self, name: str) -> dict:
        raw = await self.get_secret(name)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError) as e:
            raise SecretError(f"Secret '{name}' is not valid JSON") from e

    async def set_secret(self, name: str, value: str) -> None:
        try:
            await self._client.set_secret(name, value)
        except HttpResponseError as e:
            self._raise(e, name)

    async def delete_secret(self, name: str) -> None:
        try:
            poller = await self._client.begin_delete_secret(name)
            await poller.wait()
        except ResourceNotFoundError as e:
            raise SecretNotFoundError(f"Secret not found: {name}") from e
        except HttpResponseError as e:
            self._raise(e, name)

    async def list_secrets(self, prefix: str = "") -> list[str]:
        try:
            names: list[str] = []
            async for props in self._client.list_properties_of_secrets():
                if not prefix or (props.name and props.name.startswith(prefix)):
                    names.append(props.name)
            return names
        except HttpResponseError as e:
            self._raise(e, prefix)

    def _raise(self, exc: HttpResponseError, name: str):
        status = getattr(exc, "status_code", None)
        if status == 404:
            raise SecretNotFoundError(f"Secret not found: {name}") from exc
        if status == 403:
            raise SecretPermissionError(f"Access denied for secret: {name}") from exc
        raise SecretError(str(exc)) from exc
