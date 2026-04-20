import redis.asyncio as aioredis
from redis.credentials import CredentialProvider

from cloudrift.cache.base import CacheBackend, _RedisMixin
from cloudrift.core.exceptions import CacheConnectionError


class AzureRedisCacheBackend(_RedisMixin, CacheBackend):
    """Azure Cache for Redis backend.

    Use one of the class methods to construct:
    - ``from_access_key``        — primary or secondary access key (standard auth)
    - ``from_managed_identity``  — Azure Managed Identity via Entra ID token auth
    - ``from_service_principal`` — Azure AD service principal via Entra ID token auth
    """

    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_access_key(
        cls,
        host: str,
        access_key: str,
        port: int = 6380,
        db: int = 0,
        ssl: bool = True,
    ) -> "AzureRedisCacheBackend":
        """Authenticate with an Azure Cache for Redis access key.

        Args:
            host: e.g. ``<name>.redis.cache.windows.net``
            access_key: Primary or secondary access key from the Azure portal.
            port: Redis SSL port (default 6380; non-TLS is 6379).
            db: Database index (default 0).
            ssl: Enable TLS (default ``True``; required for Azure Cache for Redis).
        """
        try:
            client = aioredis.Redis(
                host=host,
                port=port,
                password=access_key,
                db=db,
                ssl=ssl,
            )
            return cls(client)
        except Exception as e:
            raise CacheConnectionError(f"Failed to connect to Azure Cache for Redis: {e}") from e

    @classmethod
    def from_managed_identity(
        cls,
        host: str,
        username: str,
        port: int = 6380,
        db: int = 0,
        ssl: bool = True,
        client_id: str | None = None,
    ) -> "AzureRedisCacheBackend":
        """Authenticate via Azure Managed Identity (Entra ID token auth).

        Requires the cache to have *Microsoft Entra Authentication* enabled and the
        managed identity to have a Redis data-access role assigned.

        Args:
            host: e.g. ``<name>.redis.cache.windows.net``
            username: The object ID (or configured Redis username) of the managed identity.
            port: Redis SSL port (default 6380).
            db: Database index (default 0).
            ssl: Enable TLS (default ``True``).
            client_id: Optional client ID for a user-assigned managed identity.
                       Omit to use the system-assigned identity.
        """
        try:
            from azure.identity import ManagedIdentityCredential
            credential = ManagedIdentityCredential(client_id=client_id) if client_id else ManagedIdentityCredential()
            provider = _AzureEntraCredentialProvider(credential, username)
            client = aioredis.Redis(
                host=host,
                port=port,
                db=db,
                ssl=ssl,
                credential_provider=provider,
            )
            return cls(client)
        except Exception as e:
            raise CacheConnectionError(
                f"Failed to connect to Azure Cache for Redis (Managed Identity): {e}"
            ) from e

    @classmethod
    def from_service_principal(
        cls,
        host: str,
        username: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        port: int = 6380,
        db: int = 0,
        ssl: bool = True,
    ) -> "AzureRedisCacheBackend":
        """Authenticate via Azure AD service principal (Entra ID token auth).

        Requires the cache to have *Microsoft Entra Authentication* enabled and the
        service principal to have a Redis data-access role assigned.

        Args:
            host: e.g. ``<name>.redis.cache.windows.net``
            username: The object ID (or configured Redis username) of the service principal.
            tenant_id: Azure AD tenant ID.
            client_id: Service principal application (client) ID.
            client_secret: Service principal client secret.
            port: Redis SSL port (default 6380).
            db: Database index (default 0).
            ssl: Enable TLS (default ``True``).
        """
        try:
            from azure.identity import ClientSecretCredential
            credential = ClientSecretCredential(
                tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
            )
            provider = _AzureEntraCredentialProvider(credential, username)
            client = aioredis.Redis(
                host=host,
                port=port,
                db=db,
                ssl=ssl,
                credential_provider=provider,
            )
            return cls(client)
        except Exception as e:
            raise CacheConnectionError(
                f"Failed to connect to Azure Cache for Redis (Service Principal): {e}"
            ) from e


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

class _AzureEntraCredentialProvider(CredentialProvider):
    """Fetches and refreshes an Entra ID access token for Azure Cache for Redis.

    Uses the *sync* ``azure.identity`` credential so that ``get_credentials()``
    (called by redis-py on each connection) stays synchronous.
    Tokens are valid for ~1 hour; redis-py re-calls this on reconnect.
    """

    _REDIS_RESOURCE = "https://redis.azure.com/.default"

    def __init__(self, credential, username: str) -> None:
        self._credential = credential
        self._username = username

    def get_credentials(self) -> tuple[str, str]:
        token = self._credential.get_token(self._REDIS_RESOURCE)
        return self._username, token.token
