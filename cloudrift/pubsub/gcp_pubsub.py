import asyncio

from google.api_core.exceptions import GoogleAPICallError, NotFound, PermissionDenied
from google.pubsub_v1 import PubsubMessage
from google.pubsub_v1.services.publisher import PublisherAsyncClient

from cloudrift.core.exceptions import PubSubError, TopicNotFoundError
from cloudrift.pubsub.base import PubSubBackend


class GCPPubSubBackend(PubSubBackend):
    """Google Cloud Pub/Sub fan-out backend (native async via the GAPIC client).

    A single async publisher is created lazily on first use and reused for the
    lifetime of the backend.

    Use one of the class methods to construct:
    - ``from_application_default``  — ADC: GKE Workload Identity / Cloud Run / gcloud
    - ``from_service_account_file`` — service-account JSON key file
    - ``from_service_account_info`` — service-account JSON held in memory

    ``topic`` accepts either a bare topic ID (resolved against ``project``) or a
    full ``projects/<p>/topics/<t>`` resource name, so callers can stay
    provider-neutral and pass the same short name they pass to SNS/Event Grid.

    One product, two categories: Pub/Sub is both the SNS analog (topic fan-out,
    this module) and the SQS analog (queue semantics via a pull subscription, see
    :mod:`cloudrift.messaging.gcp_pubsub`). Use this one when you only publish.
    """

    def __init__(
        self,
        project: str,
        *,
        credentials=None,
        client_options: dict | None = None,
        publisher_options: dict | None = None,
    ) -> None:
        self.project = project
        self._credentials = credentials
        self._client_options = client_options or {}
        self._publisher_options = publisher_options or {}
        self._client: PublisherAsyncClient | None = None
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
    ) -> "GCPPubSubBackend":
        """Authenticate via Application Default Credentials.

        ``prefer_metadata=True`` reads the attached service account straight from
        the metadata server — see :mod:`cloudrift.core.gcp_credentials`.
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
    ) -> "GCPPubSubBackend":
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
    ) -> "GCPPubSubBackend":
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

    async def _ensure(self) -> PublisherAsyncClient:
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client = PublisherAsyncClient(
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

    def _topic_path(self, topic: str) -> str:
        if topic.startswith("projects/"):
            return topic
        return f"projects/{self.project}/topics/{topic}"

    # ------------------------------------------------------------------
    # PubSubBackend implementation
    # ------------------------------------------------------------------

    async def publish(self, topic: str, message: str, attributes: dict | None = None) -> str:
        """Publish a message to a topic. Returns the server-assigned message ID."""
        client = await self._ensure()
        # Pub/Sub attributes are natively string→string, so unlike SNS there is
        # no DataType wrapper — but non-string values still have to be coerced.
        attrs = {k: str(v) for k, v in (attributes or {}).items()}
        pubsub_message = PubsubMessage(data=message.encode("utf-8"), attributes=attrs)
        try:
            response = await client.publish(
                topic=self._topic_path(topic), messages=[pubsub_message]
            )
            return response.message_ids[0]
        except GoogleAPICallError as e:
            self._raise(e, topic)

    async def publish_batch(self, topic: str, messages: list[dict]) -> list[str]:
        """Publish multiple messages in a single request.

        Pub/Sub takes the whole batch in one ``publish`` call — there is no
        10-message cap to chunk around as there is on SNS. The 10 MB request
        limit still applies; split the list if you approach it.
        """
        if not messages:
            return []
        client = await self._ensure()
        batch = []
        for msg in messages:
            attrs = {k: str(v) for k, v in (msg.get("attributes") or {}).items()}
            batch.append(
                PubsubMessage(data=msg.get("message", "").encode("utf-8"), attributes=attrs)
            )
        try:
            response = await client.publish(topic=self._topic_path(topic), messages=batch)
            return list(response.message_ids)
        except GoogleAPICallError as e:
            self._raise(e, topic)

    async def health_check(self) -> bool:
        try:
            client = await self._ensure()
            pager = await client.list_topics(project=f"projects/{self.project}")
            # The async pager is lazy — touch the first page so this actually
            # proves the API is reachable.
            async for _ in pager:
                break
            return True
        except Exception:
            return False

    def _raise(self, exc: GoogleAPICallError, topic: str):
        # Mirrors the SNS backend's mapping, including the PubSubError fallback:
        # a caller catching PublishError must see the same behavior on either
        # provider.
        if isinstance(exc, NotFound):
            raise TopicNotFoundError(f"Topic not found: {topic}") from exc
        if isinstance(exc, PermissionDenied):
            raise PubSubError(f"Access denied for topic: {topic}") from exc
        raise PubSubError(str(exc)) from exc
