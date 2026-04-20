import json
import uuid

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError

from cloudrift.core.exceptions import PubSubError, PublishError, TopicNotFoundError
from cloudrift.pubsub.base import PubSubBackend


class AzureEventGridBackend(PubSubBackend):
    """Azure Event Grid pub/sub backend (native async via ``azure.eventgrid.aio``).

    Publishes ``CloudEvent`` messages to an Event Grid topic endpoint.

    Use one of the class methods to construct:
    - ``from_access_key``        — topic endpoint + access key
    - ``from_managed_identity``  — Azure Managed Identity
    - ``from_service_principal`` — Azure AD service principal
    """

    def __init__(self, client, *, credential=None) -> None:
        self._client = client
        self._credential = credential

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_access_key(
        cls,
        endpoint: str,
        access_key: str,
    ) -> "AzureEventGridBackend":
        """Authenticate with a topic endpoint URL and access key."""
        from azure.core.credentials import AzureKeyCredential
        from azure.eventgrid.aio import EventGridPublisherClient

        credential = AzureKeyCredential(access_key)
        return cls(EventGridPublisherClient(endpoint, credential), credential=None)

    @classmethod
    def from_managed_identity(
        cls,
        endpoint: str,
        client_id: str | None = None,
    ) -> "AzureEventGridBackend":
        """Authenticate via Azure Managed Identity."""
        from azure.identity.aio import ManagedIdentityCredential
        from azure.eventgrid.aio import EventGridPublisherClient

        credential = (
            ManagedIdentityCredential(client_id=client_id)
            if client_id
            else ManagedIdentityCredential()
        )
        return cls(
            EventGridPublisherClient(endpoint, credential), credential=credential
        )

    @classmethod
    def from_service_principal(
        cls,
        endpoint: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ) -> "AzureEventGridBackend":
        """Authenticate via Azure AD service principal."""
        from azure.identity.aio import ClientSecretCredential
        from azure.eventgrid.aio import EventGridPublisherClient

        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )
        return cls(
            EventGridPublisherClient(endpoint, credential), credential=credential
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        await self._client.close()
        if self._credential is not None and hasattr(self._credential, "close"):
            await self._credential.close()

    # ------------------------------------------------------------------
    # PubSubBackend implementation
    # ------------------------------------------------------------------

    async def publish(
        self, topic: str, message: str, attributes: dict | None = None
    ) -> str:
        from azure.core.messaging import CloudEvent

        event_id = str(uuid.uuid4())
        event = CloudEvent(
            source=topic,
            type="cloudrift.event",
            data=message,
            id=event_id,
            extensions=attributes or {},
        )
        try:
            await self._client.send(events=[event])
            return event_id
        except ResourceNotFoundError as e:
            raise TopicNotFoundError(f"Topic not found: {topic}") from e
        except HttpResponseError as e:
            self._raise(e, topic)

    async def publish_batch(
        self, topic: str, messages: list[dict]
    ) -> list[str]:
        from azure.core.messaging import CloudEvent

        events = []
        ids = []
        for msg in messages:
            event_id = str(uuid.uuid4())
            ids.append(event_id)
            events.append(
                CloudEvent(
                    source=topic,
                    type="cloudrift.event",
                    data=msg.get("message", json.dumps(msg)),
                    id=event_id,
                    extensions=msg.get("attributes", {}),
                )
            )
        try:
            await self._client.send(events=events)
            return ids
        except ResourceNotFoundError as e:
            raise TopicNotFoundError(f"Topic not found: {topic}") from e
        except HttpResponseError as e:
            self._raise(e, topic)

    async def health_check(self) -> bool:
        # Event Grid doesn't have a lightweight ping; best-effort check
        return True

    def _raise(self, exc: HttpResponseError, topic: str):
        status = getattr(exc, "status_code", None)
        if status == 404:
            raise TopicNotFoundError(f"Topic not found: {topic}") from exc
        if status == 403:
            raise PubSubError(f"Access denied for topic: {topic}") from exc
        raise PublishError(str(exc)) from exc
