import asyncio
import json

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.servicebus import NEXT_AVAILABLE_SESSION, ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus.exceptions import OperationTimeoutError

from cloudrift.core.exceptions import (
    FeatureNotSupportedError,
    MessageSendError,
    MessagingError,
    QueueNotFoundError,
)
from cloudrift.messaging.base import Message, MessagingBackend


class AzureServiceBusBackend(MessagingBackend):
    """Azure Service Bus messaging backend (native async via ``azure.servicebus.aio``).

    A single ``ServiceBusClient`` (one AMQP connection) is opened lazily and
    reused for the lifetime of the backend. Call ``await backend.close()``
    (or use ``async with backend:``) to release the connection.

    Use one of the class methods to construct:
    - ``from_connection_string``  — shared-access connection string
    - ``from_managed_identity``   — Azure Managed Identity (system or user-assigned)
    - ``from_service_principal``  — Azure AD service principal (client secret)

    FIFO-style queues: pass ``session_enabled=True`` for queues created with
    sessions. ``group_id`` maps to the message ``session_id`` and ``dedup_id``
    to ``message_id`` (deduplication requires duplicate detection enabled on
    the queue at creation time).

    Receipt handles (lock tokens) are only valid on the same backend instance
    and within the message lock duration — unlike SQS receipt handles, they
    cannot be passed to another client.
    """

    def __init__(
        self,
        queue_name: str,
        *,
        connection_string: str | None = None,
        fully_qualified_namespace: str | None = None,
        credential=None,
        session_enabled: bool = False,
    ) -> None:
        if not connection_string and not fully_qualified_namespace:
            raise ValueError(
                "Provide either connection_string or fully_qualified_namespace + credential"
            )
        self.queue_name = queue_name
        self.session_enabled = session_enabled
        self._connection_string = connection_string
        self._namespace = fully_qualified_namespace
        self._credential = credential
        self._client: ServiceBusClient | None = None
        self._lock = asyncio.Lock()
        # lock_token → (receiver, ServiceBusReceivedMessage)
        self._pending: dict[str, tuple] = {}
        # id(receiver) → (receiver, set_of_lock_tokens)
        self._receiver_tokens: dict[int, tuple] = {}

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_connection_string(
        cls, connection_string: str, queue_name: str, *, session_enabled: bool = False
    ) -> "AzureServiceBusBackend":
        """Authenticate with a Service Bus connection string."""
        return cls(
            queue_name,
            connection_string=connection_string,
            session_enabled=session_enabled,
        )

    @classmethod
    def from_managed_identity(
        cls,
        fully_qualified_namespace: str,
        queue_name: str,
        client_id: str | None = None,
        *,
        session_enabled: bool = False,
    ) -> "AzureServiceBusBackend":
        """Authenticate via Azure Managed Identity (system or user-assigned)."""
        from azure.identity.aio import ManagedIdentityCredential

        credential = (
            ManagedIdentityCredential(client_id=client_id)
            if client_id
            else ManagedIdentityCredential()
        )
        return cls(
            queue_name,
            fully_qualified_namespace=fully_qualified_namespace,
            credential=credential,
            session_enabled=session_enabled,
        )

    @classmethod
    def from_service_principal(
        cls,
        fully_qualified_namespace: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        queue_name: str,
        *,
        session_enabled: bool = False,
    ) -> "AzureServiceBusBackend":
        """Authenticate via Azure AD service principal (client secret)."""
        from azure.identity.aio import ClientSecretCredential

        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )
        return cls(
            queue_name,
            fully_qualified_namespace=fully_qualified_namespace,
            credential=credential,
            session_enabled=session_enabled,
        )

    # ------------------------------------------------------------------
    # Lifecycle (single client / single AMQP connection reused)
    # ------------------------------------------------------------------

    async def _ensure(self) -> ServiceBusClient:
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                if self._connection_string:
                    self._client = ServiceBusClient.from_connection_string(
                        self._connection_string
                    )
                else:
                    self._client = ServiceBusClient(
                        self._namespace, credential=self._credential
                    )
        return self._client

    async def close(self) -> None:
        for receiver, _ in list(self._receiver_tokens.values()):
            try:
                await receiver.__aexit__(None, None, None)
            except Exception:
                pass
        self._receiver_tokens.clear()
        self._pending.clear()
        if self._client is not None:
            await self._client.close()
            self._client = None
        if self._credential is not None and hasattr(self._credential, "close"):
            await self._credential.close()

    # ------------------------------------------------------------------
    # MessagingBackend implementation
    # ------------------------------------------------------------------

    def _build_message(
        self, message: dict, group_id: str | None, dedup_id: str | None
    ) -> ServiceBusMessage:
        if self.session_enabled and not group_id:
            raise MessageSendError(
                f"group_id is required when sending to session-enabled queue "
                f"{self.queue_name!r}"
            )
        sb_message = ServiceBusMessage(json.dumps(message))
        if group_id:
            sb_message.session_id = group_id
        if dedup_id:
            sb_message.message_id = dedup_id
        return sb_message

    async def send(
        self,
        message: dict,
        delay: int = 0,
        *,
        group_id: str | None = None,
        dedup_id: str | None = None,
    ) -> str:
        client = await self._ensure()
        sb_message = self._build_message(message, group_id, dedup_id)
        try:
            async with client.get_queue_sender(self.queue_name) as sender:
                if delay:
                    from datetime import datetime, timedelta, timezone

                    sb_message.scheduled_enqueue_time_utc = datetime.now(timezone.utc) + timedelta(
                        seconds=delay
                    )
                await sender.send_messages(sb_message)
                return sb_message.message_id or ""
        except ResourceNotFoundError as e:
            raise QueueNotFoundError(f"Queue not found: {self.queue_name}") from e
        except HttpResponseError as e:
            raise MessageSendError(str(e)) from e

    async def send_batch(
        self,
        messages: list[dict],
        *,
        group_id: str | None = None,
        dedup_ids: list[str] | None = None,
    ) -> list[str]:
        client = await self._ensure()
        if dedup_ids is not None and len(dedup_ids) != len(messages):
            raise MessageSendError("dedup_ids must be parallel to messages")
        sb_messages = [
            self._build_message(m, group_id, dedup_ids[i] if dedup_ids else None)
            for i, m in enumerate(messages)
        ]
        try:
            async with client.get_queue_sender(self.queue_name) as sender:
                batch = await sender.create_message_batch()
                for msg in sb_messages:
                    batch.add_message(msg)
                await sender.send_messages(batch)
                return [msg.message_id or "" for msg in sb_messages]
        except ResourceNotFoundError as e:
            raise QueueNotFoundError(f"Queue not found: {self.queue_name}") from e
        except HttpResponseError as e:
            raise MessageSendError(str(e)) from e

    async def receive(
        self,
        max_messages: int = 1,
        wait_time: int = 0,
        *,
        group_id: str | None = None,
        visibility_timeout: int | None = None,
    ) -> list[Message]:
        # visibility_timeout is intentionally ignored: Service Bus lock
        # duration is fixed at the queue level.
        client = await self._ensure()
        if self.session_enabled:
            try:
                receiver = client.get_queue_receiver(
                    self.queue_name,
                    session_id=group_id or NEXT_AVAILABLE_SESSION,
                    max_wait_time=wait_time or None,
                )
                await receiver.__aenter__()
            except OperationTimeoutError:
                # No session currently has messages — normal in polling loops.
                return []
        else:
            if group_id is not None:
                raise FeatureNotSupportedError(
                    "group_id receive requires a session-enabled queue "
                    "(construct the backend with session_enabled=True)"
                )
            receiver = client.get_queue_receiver(self.queue_name)
            await receiver.__aenter__()
        try:
            raw_messages = await receiver.receive_messages(
                max_message_count=max_messages,
                max_wait_time=wait_time or None,
            )
            if not raw_messages:
                await receiver.__aexit__(None, None, None)
                return []
            tokens: set[str] = set()
            messages: list[Message] = []
            for m in raw_messages:
                token = str(m.lock_token)
                self._pending[token] = (receiver, m)
                tokens.add(token)
                messages.append(
                    Message(
                        id=str(m.message_id or ""),
                        body=json.loads(str(m)),
                        receipt_handle=token,
                        attributes={
                            "sequence_number": m.sequence_number,
                            "enqueued_time": str(m.enqueued_time_utc),
                        },
                        group_id=m.session_id,
                        dedup_id=str(m.message_id) if m.message_id else None,
                        receive_count=(m.delivery_count or 0) + 1,
                    )
                )
            self._receiver_tokens[id(receiver)] = (receiver, tokens)
            return messages
        except OperationTimeoutError:
            await receiver.__aexit__(None, None, None)
            return []
        except ResourceNotFoundError as e:
            await receiver.__aexit__(None, None, None)
            raise QueueNotFoundError(f"Queue not found: {self.queue_name}") from e
        except HttpResponseError as e:
            await receiver.__aexit__(None, None, None)
            raise MessagingError(str(e)) from e
        except Exception:
            await receiver.__aexit__(None, None, None)
            raise

    def _take_pending(self, receipt_handle: str) -> tuple:
        entry = self._pending.pop(receipt_handle, None)
        if entry is None:
            raise MessagingError(
                f"No pending message for receipt handle: {receipt_handle!r}. "
                "Call receive() first and use the returned receipt_handle."
            )
        return entry

    async def _release_token(self, receipt_handle: str, receiver) -> None:
        """Drop the token from the receiver's set; close the receiver when empty."""
        rid = id(receiver)
        if rid in self._receiver_tokens:
            _, token_set = self._receiver_tokens[rid]
            token_set.discard(receipt_handle)
            if not token_set:
                try:
                    await receiver.__aexit__(None, None, None)
                except Exception:
                    pass
                del self._receiver_tokens[rid]

    async def delete(self, receipt_handle: str) -> None:
        receiver, message = self._take_pending(receipt_handle)
        try:
            await receiver.complete_message(message)
        except ResourceNotFoundError as e:
            raise QueueNotFoundError(str(e)) from e
        except HttpResponseError as e:
            raise MessagingError(str(e)) from e
        finally:
            await self._release_token(receipt_handle, receiver)

    async def nack(self, receipt_handle: str) -> None:
        """Abandon the message so Service Bus redelivers it immediately."""
        receiver, message = self._take_pending(receipt_handle)
        try:
            await receiver.abandon_message(message)
        except ResourceNotFoundError as e:
            raise QueueNotFoundError(str(e)) from e
        except HttpResponseError as e:
            raise MessagingError(str(e)) from e
        finally:
            await self._release_token(receipt_handle, receiver)

    async def health_check(self) -> bool:
        try:
            client = await self._ensure()
            # Validate queue connectivity by opening and closing a sender
            async with client.get_queue_sender(self.queue_name):
                pass
            return True
        except Exception:
            return False

    async def _purge_receiver(self, receiver) -> None:
        async with receiver:
            while True:
                messages = await receiver.receive_messages(
                    max_message_count=100, max_wait_time=5
                )
                if not messages:
                    break
                for msg in messages:
                    await receiver.complete_message(msg)

    async def purge(self) -> None:
        client = await self._ensure()
        try:
            if self.session_enabled:
                # Drain one session at a time until no session is available.
                while True:
                    try:
                        receiver = client.get_queue_receiver(
                            self.queue_name,
                            session_id=NEXT_AVAILABLE_SESSION,
                            max_wait_time=5,
                        )
                        await self._purge_receiver(receiver)
                    except OperationTimeoutError:
                        break
            else:
                await self._purge_receiver(client.get_queue_receiver(self.queue_name))
        except ResourceNotFoundError as e:
            raise QueueNotFoundError(f"Queue not found: {self.queue_name}") from e
        except HttpResponseError as e:
            raise MessagingError(str(e)) from e
