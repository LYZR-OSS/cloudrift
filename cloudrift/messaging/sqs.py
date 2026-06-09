import asyncio
import json

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from cloudrift.core.exceptions import MessageSendError, MessagingError, QueueNotFoundError
from cloudrift.messaging.base import Message, MessagingBackend


class AWSSQSBackend(MessagingBackend):
    """AWS SQS messaging backend (native async via ``aioboto3``).

    A single async SQS client is created lazily and reused across operations.
    Call ``await backend.close()`` (or use ``async with backend:``) to release
    the underlying connections.

    Use one of the class methods to construct:
    - ``from_access_key`` — static credentials (+ optional session token for assumed roles)
    - ``from_iam_role``   — instance profile / environment / ECS task role
    - ``from_profile``    — named profile from ``~/.aws/credentials``
    """

    def __init__(
        self,
        queue_url: str,
        session: aioboto3.Session,
        *,
        endpoint_url: str | None = None,
        dlq_url: str | None = None,
        max_pool_connections: int = 50,
        connect_timeout: float = 10.0,
        read_timeout: float = 60.0,
        client_kwargs: dict | None = None,
    ) -> None:
        self.queue_url = queue_url
        self._session = session
        self._endpoint_url = endpoint_url
        # Explicit DLQ URL; if None it is resolved lazily from the source queue's
        # RedrivePolicy the first time dead_letter() is called.
        self._dlq_url = dlq_url
        self._config = Config(
            max_pool_connections=max_pool_connections,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
        self._client_kwargs = client_kwargs or {}
        self._client_cm = None
        self._client = None
        self._lock = asyncio.Lock()
        # receipt_handle → raw message body (JSON string), retained between
        # receive() and delete()/dead_letter() so emulated dead-lettering can
        # re-send the original payload to the DLQ.
        self._pending: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_access_key(
        cls,
        queue_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region: str = "us-east-1",
        aws_session_token: str | None = None,
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSSQSBackend":
        """Authenticate with explicit access key / secret (+ optional STS session token)."""
        session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region,
        )
        return cls(queue_url, session, endpoint_url=endpoint_url, **kwargs)

    @classmethod
    def from_iam_role(
        cls,
        queue_url: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSSQSBackend":
        """Authenticate via IAM role / instance profile / environment variables."""
        session = aioboto3.Session(region_name=region)
        return cls(queue_url, session, endpoint_url=endpoint_url, **kwargs)

    @classmethod
    def from_profile(
        cls,
        queue_url: str,
        profile_name: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSSQSBackend":
        """Authenticate using a named profile from ``~/.aws/credentials``."""
        session = aioboto3.Session(profile_name=profile_name, region_name=region)
        return cls(queue_url, session, endpoint_url=endpoint_url, **kwargs)

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _ensure(self):
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client_cm = self._session.client(
                    "sqs",
                    endpoint_url=self._endpoint_url,
                    config=self._config,
                    **self._client_kwargs,
                )
                self._client = await self._client_cm.__aenter__()
        return self._client

    async def close(self) -> None:
        if self._client_cm is not None:
            await self._client_cm.__aexit__(None, None, None)
            self._client = None
            self._client_cm = None

    # ------------------------------------------------------------------
    # MessagingBackend implementation
    # ------------------------------------------------------------------

    async def send(self, message: dict, delay: int = 0) -> str:
        client = await self._ensure()
        try:
            response = await client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(message),
                DelaySeconds=delay,
            )
            return response["MessageId"]
        except ClientError as e:
            self._raise(e)

    async def send_batch(self, messages: list[dict]) -> list[str]:
        client = await self._ensure()
        entries = [
            {"Id": str(i), "MessageBody": json.dumps(msg)} for i, msg in enumerate(messages)
        ]
        try:
            response = await client.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
            if response.get("Failed"):
                failed = [f["Id"] for f in response["Failed"]]
                raise MessageSendError(f"Failed to send messages with IDs: {failed}")
            return [s["MessageId"] for s in response.get("Successful", [])]
        except ClientError as e:
            self._raise(e)

    async def receive(self, max_messages: int = 1, wait_time: int = 0) -> list[Message]:
        client = await self._ensure()
        try:
            response = await client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=min(max_messages, 10),
                WaitTimeSeconds=wait_time,
                AttributeNames=["All"],
            )
            messages = []
            for m in response.get("Messages", []):
                self._pending[m["ReceiptHandle"]] = m["Body"]
                messages.append(
                    Message(
                        id=m["MessageId"],
                        body=json.loads(m["Body"]),
                        receipt_handle=m["ReceiptHandle"],
                        attributes=m.get("Attributes", {}),
                    )
                )
            return messages
        except ClientError as e:
            self._raise(e)

    async def delete(self, receipt_handle: str) -> None:
        client = await self._ensure()
        try:
            await client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        except ClientError as e:
            self._raise(e)
        finally:
            self._pending.pop(receipt_handle, None)

    async def dead_letter(self, receipt_handle: str, reason: str) -> None:
        client = await self._ensure()
        body = self._pending.get(receipt_handle)
        if body is None:
            raise MessagingError(
                f"No pending message for receipt handle: {receipt_handle!r}. "
                "Call receive() first and use the returned receipt_handle."
            )
        dlq_url = await self._resolve_dlq_url(client)
        try:
            await client.send_message(
                QueueUrl=dlq_url,
                MessageBody=body,
                MessageAttributes={
                    "DeadLetterReason": {"DataType": "String", "StringValue": reason}
                },
            )
            await client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        except ClientError as e:
            self._raise(e)
        else:
            self._pending.pop(receipt_handle, None)

    async def get_queue_depth(self) -> int:
        client = await self._ensure()
        try:
            response = await client.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            return int(response["Attributes"]["ApproximateNumberOfMessages"])
        except ClientError as e:
            self._raise(e)

    async def _resolve_dlq_url(self, client) -> str:
        """Return the configured DLQ URL, deriving it from RedrivePolicy if needed."""
        if self._dlq_url is not None:
            return self._dlq_url
        try:
            response = await client.get_queue_attributes(
                QueueUrl=self.queue_url, AttributeNames=["RedrivePolicy"]
            )
        except ClientError as e:
            self._raise(e)
        redrive = response.get("Attributes", {}).get("RedrivePolicy")
        if not redrive:
            raise MessagingError(
                f"No dead-letter queue configured for {self.queue_url}. Pass dlq_url= "
                "when constructing the backend, or set a RedrivePolicy on the queue."
            )
        target_arn = json.loads(redrive)["deadLetterTargetArn"]
        dlq_name = target_arn.rsplit(":", 1)[-1]
        try:
            self._dlq_url = (await client.get_queue_url(QueueName=dlq_name))["QueueUrl"]
        except ClientError as e:
            self._raise(e)
        return self._dlq_url

    async def purge(self) -> None:
        client = await self._ensure()
        try:
            await client.purge_queue(QueueUrl=self.queue_url)
        except ClientError as e:
            self._raise(e)

    async def health_check(self) -> bool:
        try:
            client = await self._ensure()
            await client.get_queue_attributes(
                QueueUrl=self.queue_url, AttributeNames=["QueueArn"]
            )
            return True
        except Exception:
            return False

    def _raise(self, exc: ClientError):
        code = exc.response["Error"]["Code"]
        if code == "AWS.SimpleQueueService.NonExistentQueue":
            raise QueueNotFoundError(f"Queue not found: {self.queue_url}") from exc
        if code in (
            "SendMessageBatchRequestEntry.SendMessageBatchRequestEntryId",
            "InvalidMessageContents",
        ):
            raise MessageSendError(str(exc)) from exc
        raise MessagingError(str(exc)) from exc
