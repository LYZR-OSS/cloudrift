import asyncio
import json

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient

from cloudrift.core.exceptions import MessageSendError, MessagingError, QueueNotFoundError
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
    """

    def __init__(
        self,
        queue_name: str,
        *,
        connection_string: str | None = None,
        fully_qualified_namespace: str | None = None,
        credential=None,
    ) -> None:
        if not connection_string and not fully_qualified_namespace:
            raise ValueError(
                "Provide either connection_string or fully_qualified_namespace + credential"
            )
        self.queue_name = queue_name
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
        cls, connection_string: str, queue_name: str
    ) -> "AzureServiceBusBackend":
        """Authenticate with a Service Bus connection string."""
        return cls(queue_name, connection_string=connection_string)

    @classmethod
    def from_managed_identity(
        cls,
        fully_qualified_namespace: str,
        queue_name: str,
        client_id: str | None = None,
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
        )

    @classmethod
    def from_service_principal(
        cls,
        fully_qualified_namespace: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        queue_name: str,
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

    async def send(self, message: dict, delay: int = 0) -> str:
        client = await self._ensure()
        try:
            async with client.get_queue_sender(self.queue_name) as sender:
                sb_message = ServiceBusMessage(json.dumps(message))
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

    async def send_batch(self, messages: list[dict]) -> list[str]:
        client = await self._ensure()
        try:
            async with client.get_queue_sender(self.queue_name) as sender:
                batch = await sender.create_message_batch()
                sb_messages = [ServiceBusMessage(json.dumps(m)) for m in messages]
                for msg in sb_messages:
                    batch.add_message(msg)
                await sender.send_messages(batch)
                return [msg.message_id or "" for msg in sb_messages]
        except ResourceNotFoundError as e:
            raise QueueNotFoundError(f"Queue not found: {self.queue_name}") from e
        except HttpResponseError as e:
            raise MessageSendError(str(e)) from e

    async def receive(self, max_messages: int = 1, wait_time: int = 0) -> list[Message]:
        client = await self._ensure()
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
                    )
                )
            self._receiver_tokens[id(receiver)] = (receiver, tokens)
            return messages
        except ResourceNotFoundError as e:
            await receiver.__aexit__(None, None, None)
            raise QueueNotFoundError(f"Queue not found: {self.queue_name}") from e
        except HttpResponseError as e:
            await receiver.__aexit__(None, None, None)
            raise MessagingError(str(e)) from e
        except Exception:
            await receiver.__aexit__(None, None, None)
            raise

    async def delete(self, receipt_handle: str) -> None:
        entry = self._pending.pop(receipt_handle, None)
        if entry is None:
            raise MessagingError(
                f"No pending message for receipt handle: {receipt_handle!r}. "
                "Call receive() first and use the returned receipt_handle."
            )
        receiver, message = entry
        try:
            await receiver.complete_message(message)
        except ResourceNotFoundError as e:
            raise QueueNotFoundError(str(e)) from e
        except HttpResponseError as e:
            raise MessagingError(str(e)) from e
        finally:
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

    async def health_check(self) -> bool:
        try:
            client = await self._ensure()
            # Validate queue connectivity by opening and closing a sender
            async with client.get_queue_sender(self.queue_name):
                pass
            return True
        except Exception:
            return False

    async def purge(self) -> None:
        client = await self._ensure()
        try:
            async with client.get_queue_receiver(self.queue_name) as receiver:
                while True:
                    messages = await receiver.receive_messages(
                        max_message_count=100, max_wait_time=5
                    )
                    if not messages:
                        break
                    for msg in messages:
                        await receiver.complete_message(msg)
        except ResourceNotFoundError as e:
            raise QueueNotFoundError(f"Queue not found: {self.queue_name}") from e
        except HttpResponseError as e:
            raise MessagingError(str(e)) from e
