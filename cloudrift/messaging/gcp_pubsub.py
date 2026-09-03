import asyncio

from google.api_core.exceptions import (
    DeadlineExceeded,
    GoogleAPICallError,
    NotFound,
    PermissionDenied,
    RetryError,
)
from google.protobuf.timestamp_pb2 import Timestamp
from google.pubsub_v1 import PubsubMessage
from google.pubsub_v1.services.publisher import PublisherAsyncClient
from google.pubsub_v1.services.subscriber import SubscriberAsyncClient

from cloudrift.core.exceptions import (
    FeatureNotSupportedError,
    MessageSendError,
    MessagingError,
    QueueNotFoundError,
)
from cloudrift.messaging.base import Message, MessagingBackend

# Pull deadline for a non-long-poll receive (wait_time=0). Bounded so a bare
# receive() returns promptly on an empty subscription instead of blocking on the
# gRPC client's much longer default timeout.
_NO_WAIT_PULL_TIMEOUT = 3.0


class GCPPubSubBackend(MessagingBackend):
    """Google Cloud Pub/Sub messaging backend (native async via the GAPIC clients).

    Pub/Sub splits what SQS and Service Bus call a queue into two resources: you
    **publish to a topic** and **receive from a subscription**. This backend
    therefore takes both, and each is required only by the direction that uses
    it — a send-only producer needs just ``topic``, a consumer just
    ``subscription``. Calling the other half without it raises
    :class:`MessagingError` rather than failing deeper in the SDK.

    Both accept a bare ID (resolved against ``project``) or a full
    ``projects/<p>/topics/<t>`` / ``projects/<p>/subscriptions/<s>`` resource
    name.

    Use one of the class methods to construct:
    - ``from_application_default``  — ADC: GKE Workload Identity / Cloud Run / gcloud
    - ``from_service_account_file`` — service-account JSON key file
    - ``from_service_account_info`` — service-account JSON held in memory

    Publisher and subscriber are separate clients, each built lazily on first
    use, so a send-only service never opens a subscriber channel.

    Ordered delivery: ``group_id`` maps to Pub/Sub's ``ordering_key``. Both the
    topic's publishes and the subscription must have message ordering enabled for
    it to take effect. ``dedup_id`` has no Pub/Sub equivalent and raises
    :class:`FeatureNotSupportedError` — Pub/Sub offers exactly-once delivery as a
    subscription setting instead of a caller-supplied deduplication token.
    """

    def __init__(
        self,
        project: str,
        *,
        topic: str | None = None,
        subscription: str | None = None,
        dead_letter_topic: str | None = None,
        credentials=None,
        client_options: dict | None = None,
    ) -> None:
        if topic is None and subscription is None:
            raise ValueError(
                "GCPPubSubBackend needs a topic (to send), a subscription (to receive), or both."
            )
        self.project = project
        self._topic = topic
        self._subscription = subscription
        self._dead_letter_topic = dead_letter_topic
        self._credentials = credentials
        self._client_options = client_options or {}
        self._publisher: PublisherAsyncClient | None = None
        self._subscriber: SubscriberAsyncClient | None = None
        self._lock = asyncio.Lock()
        # ack_id → raw message body, retained between receive() and
        # delete()/dead_letter() so emulated dead-lettering can re-publish the
        # original payload. Same approach as the SQS backend.
        self._pending: dict[str, bytes] = {}

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

    async def _ensure_publisher(self) -> PublisherAsyncClient:
        if self._publisher is not None:
            return self._publisher
        async with self._lock:
            if self._publisher is None:
                self._publisher = PublisherAsyncClient(
                    credentials=self._credentials, **self._client_options
                )
        return self._publisher

    async def _ensure_subscriber(self) -> SubscriberAsyncClient:
        if self._subscriber is not None:
            return self._subscriber
        async with self._lock:
            if self._subscriber is None:
                self._subscriber = SubscriberAsyncClient(
                    credentials=self._credentials, **self._client_options
                )
        return self._subscriber

    async def close(self) -> None:
        from cloudrift.core.gcp_credentials import close_credentials

        publisher, self._publisher = self._publisher, None
        subscriber, self._subscriber = self._subscriber, None
        self._pending.clear()
        if publisher is not None:
            await publisher.transport.close()
        if subscriber is not None:
            await subscriber.transport.close()
        # One shared credential backs both clients — close it once, not per-client.
        await close_credentials(self._credentials)

    # ------------------------------------------------------------------
    # Resource paths
    # ------------------------------------------------------------------

    def _topic_path(self, topic: str | None = None) -> str:
        name = topic or self._topic
        if name is None:
            raise MessagingError(
                "This backend has no topic configured — pass topic= to send messages."
            )
        if name.startswith("projects/"):
            return name
        return f"projects/{self.project}/topics/{name}"

    @property
    def _subscription_path(self) -> str:
        if self._subscription is None:
            raise MessagingError(
                "This backend has no subscription configured — pass subscription= "
                "to receive messages."
            )
        if self._subscription.startswith("projects/"):
            return self._subscription
        return f"projects/{self.project}/subscriptions/{self._subscription}"

    # ------------------------------------------------------------------
    # MessagingBackend implementation
    # ------------------------------------------------------------------

    def _build_message(
        self,
        body: str,
        attributes: dict[str, str] | None,
        group_id: str | None,
        dedup_id: str | None,
    ) -> PubsubMessage:
        if dedup_id:
            raise FeatureNotSupportedError(
                "Pub/Sub has no per-message deduplication ID; enable exactly-once "
                "delivery on the subscription instead"
            )
        # Pub/Sub message data is bytes, so the JSON string from the base class
        # is encoded here. The str-not-bytes hook signature is still the right
        # one: SQS's MessageBody is typed string and Service Bus is indifferent,
        # so a single encode on this backend beats a decode on the other two.
        return PubsubMessage(
            data=body.encode("utf-8"),
            attributes=dict(attributes or {}),
            ordering_key=group_id or "",
        )

    async def _send_json(
        self,
        body: str,
        attributes: dict[str, str] | None = None,
        delay: int = 0,
        *,
        group_id: str | None = None,
        dedup_id: str | None = None,
    ) -> str:
        if delay:
            raise FeatureNotSupportedError(
                "Pub/Sub does not support per-message delivery delay; schedule the "
                "publish yourself, or use Cloud Tasks for delayed delivery"
            )
        client = await self._ensure_publisher()
        message = self._build_message(body, attributes, group_id, dedup_id)
        try:
            response = await client.publish(topic=self._topic_path(), messages=[message])
            return response.message_ids[0]
        except GoogleAPICallError as e:
            self._raise(e)

    async def _send_json_batch(
        self,
        items: list[tuple[str, dict[str, str] | None]],
        *,
        group_id: str | None = None,
        dedup_ids: list[str] | None = None,
    ) -> list[str]:
        if dedup_ids is not None and len(dedup_ids) != len(items):
            raise MessageSendError("dedup_ids must be parallel to messages")
        if not items:
            return []
        client = await self._ensure_publisher()
        messages = [
            self._build_message(body, attributes, group_id, dedup_ids[i] if dedup_ids else None)
            for i, (body, attributes) in enumerate(items)
        ]
        try:
            response = await client.publish(topic=self._topic_path(), messages=messages)
            return list(response.message_ids)
        except GoogleAPICallError as e:
            self._raise(e)

    async def receive(
        self,
        max_messages: int = 1,
        wait_time: int = 0,
        *,
        group_id: str | None = None,
        visibility_timeout: int | None = None,
    ) -> list[Message]:
        """Pull messages from the subscription.

        Two mappings are imperfect and deliberately explicit:

        - ``wait_time`` becomes the pull RPC's timeout. Pub/Sub has no long-poll
          parameter: the server holds a pull briefly and returns fewer messages
          (or none) rather than waiting out a caller-specified window, so this
          bounds the call rather than guaranteeing the wait.
        - ``visibility_timeout`` is applied *after* the pull, as a
          ``modifyAckDeadline`` on the returned messages — the pull request
          itself cannot carry a deadline override. That costs one extra RPC and
          leaves a sub-second window at the subscription's default deadline.

        ``group_id`` cannot filter a pull (Pub/Sub ordering keys affect delivery
        order, not selection), so it raises
        :class:`FeatureNotSupportedError` as it does on SQS.
        """
        if group_id is not None:
            raise FeatureNotSupportedError("Pub/Sub cannot receive from a specific ordering key")
        client = await self._ensure_subscriber()
        subscription = self._subscription_path
        # wait_time is the long-poll window (the SQS WaitTimeSeconds analog). Pub/Sub
        # has no long-poll parameter, so it maps to the pull RPC deadline: the server
        # holds the request open until a message is available or the deadline fires.
        # wait_time=0 still needs a bounded deadline, or a non-polling receive() would
        # block on the client's default timeout instead of returning promptly.
        pull_timeout = float(wait_time) if wait_time else _NO_WAIT_PULL_TIMEOUT
        try:
            response = await client.pull(
                subscription=subscription,
                max_messages=max_messages,
                timeout=pull_timeout,
            )
        except (DeadlineExceeded, RetryError):
            # The pull deadline elapsed with nothing to return. That is the long-poll
            # "no messages this round" outcome — SQS returns an empty list here — not
            # an error. Surfacing it as MessagingError would make every poll of an
            # idle queue raise.
            return []
        except GoogleAPICallError as e:
            self._raise(e)

        messages = []
        ack_ids = []
        for received in response.received_messages:
            body = bytes(received.message.data)
            self._pending[received.ack_id] = body
            ack_ids.append(received.ack_id)
            messages.append(
                Message(
                    id=received.message.message_id,
                    body=body,
                    receipt_handle=received.ack_id,
                    attributes=dict(received.message.attributes),
                    group_id=received.message.ordering_key or None,
                    dedup_id=None,
                    # delivery_attempt is populated only when the subscription
                    # has a dead-letter policy; 0 means "not tracked".
                    receive_count=received.delivery_attempt or None,
                )
            )

        if visibility_timeout is not None and ack_ids:
            try:
                await client.modify_ack_deadline(
                    subscription=subscription,
                    ack_ids=ack_ids,
                    ack_deadline_seconds=visibility_timeout,
                )
            except GoogleAPICallError as e:
                self._raise(e)
        return messages

    async def nack(self, receipt_handle: str) -> None:
        """Return a message for immediate redelivery by zeroing its ack deadline."""
        client = await self._ensure_subscriber()
        try:
            await client.modify_ack_deadline(
                subscription=self._subscription_path,
                ack_ids=[receipt_handle],
                ack_deadline_seconds=0,
            )
        except GoogleAPICallError as e:
            self._raise(e)
        finally:
            # The ack_id goes stale on redelivery; redelivery issues a new one.
            self._pending.pop(receipt_handle, None)

    async def delete(self, receipt_handle: str) -> None:
        """Acknowledge a message, removing it from the subscription."""
        client = await self._ensure_subscriber()
        try:
            await client.acknowledge(subscription=self._subscription_path, ack_ids=[receipt_handle])
        except GoogleAPICallError as e:
            self._raise(e)
        finally:
            self._pending.pop(receipt_handle, None)

    async def dead_letter(self, receipt_handle: str, reason: str) -> None:
        """Emulated dead-letter: publishes to the dead-letter topic, then acks.

        Pub/Sub's native dead-letter policy is delivery-attempt-based and offers
        no per-message API, so this mirrors the SQS backend's emulation and
        inherits the same caveat: two calls with no cross-resource transaction.
        If the process dies between them, the message can end up in both places
        or neither. For strict dead-lettering, configure a dead-letter policy on
        the subscription and let Pub/Sub move the message after
        ``max_delivery_attempts``.

        Requires ``dead_letter_topic=`` at construction — unlike SQS, Pub/Sub
        exposes no way to read the configured dead-letter topic back off a
        subscription without the Admin API, so cloudrift does not guess.
        """
        body = self._pending.get(receipt_handle)
        if body is None:
            raise MessagingError(
                f"No pending message for ack ID: {receipt_handle!r}. "
                "Call receive() first and use the returned receipt_handle."
            )
        if self._dead_letter_topic is None:
            raise MessagingError(
                "No dead-letter topic configured. Pass dead_letter_topic= when "
                "constructing the backend, or configure a dead-letter policy on "
                "the subscription and let Pub/Sub handle redelivery."
            )
        publisher = await self._ensure_publisher()
        subscriber = await self._ensure_subscriber()
        try:
            await publisher.publish(
                topic=self._topic_path(self._dead_letter_topic),
                messages=[PubsubMessage(data=body, attributes={"DeadLetterReason": reason})],
            )
            await subscriber.acknowledge(
                subscription=self._subscription_path, ack_ids=[receipt_handle]
            )
        except GoogleAPICallError as e:
            self._raise(e)
        finally:
            self._pending.pop(receipt_handle, None)

    async def get_queue_depth(self) -> int:
        """Not available on Pub/Sub.

        Backlog size is not exposed on the data plane — it is the Cloud
        Monitoring metric
        ``pubsub.googleapis.com/subscription/num_undelivered_messages``. Reading
        it needs the Monitoring API, a separate dependency, and
        ``roles/monitoring.viewer``, so cloudrift does not silently pull it in.
        Query Monitoring directly if you are autoscaling on backlog.
        """
        raise NotImplementedError(
            "Pub/Sub does not expose queue depth on the data plane. Read the "
            "Cloud Monitoring metric subscription/num_undelivered_messages instead."
        )

    async def purge(self) -> None:
        """Discard the backlog by seeking the subscription to the current time.

        Pub/Sub has no purge call; seeking to *now* acknowledges everything
        published before this moment, which is the same observable outcome.
        Requires the subscription to retain acknowledged messages or be within
        its retention window — seek is a retention-based operation.
        """
        client = await self._ensure_subscriber()
        now = Timestamp()
        now.GetCurrentTime()
        try:
            await client.seek(request={"subscription": self._subscription_path, "time": now})
            self._pending.clear()
        except GoogleAPICallError as e:
            self._raise(e)

    async def health_check(self) -> bool:
        try:
            if self._subscription is not None:
                client = await self._ensure_subscriber()
                await client.get_subscription(subscription=self._subscription_path)
            else:
                publisher = await self._ensure_publisher()
                await publisher.get_topic(topic=self._topic_path())
            return True
        except Exception:
            return False

    def _raise(self, exc: GoogleAPICallError):
        if isinstance(exc, NotFound):
            raise QueueNotFoundError(str(exc)) from exc
        if isinstance(exc, PermissionDenied):
            raise MessagingError(f"Access denied: {exc}") from exc
        raise MessagingError(str(exc)) from exc
