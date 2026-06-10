import asyncio
import json

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from cloudrift.core.exceptions import (
    FeatureNotSupportedError,
    MessageSendError,
    MessagingError,
    QueueNotFoundError,
)
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
        max_pool_connections: int = 50,
        connect_timeout: float = 10.0,
        read_timeout: float = 60.0,
        client_kwargs: dict | None = None,
    ) -> None:
        self.queue_url = queue_url
        self._is_fifo = queue_url.endswith(".fifo")
        self._session = session
        self._endpoint_url = endpoint_url
        self._config = Config(
            max_pool_connections=max_pool_connections,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
        self._client_kwargs = client_kwargs or {}
        self._client_cm = None
        self._client = None
        self._lock = asyncio.Lock()

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
                try:
                    self._client = await self._client_cm.__aenter__()
                except Exception:
                    self._client_cm = None
                    raise
        return self._client

    async def close(self) -> None:
        client_cm, self._client_cm = self._client_cm, None
        self._client = None
        if client_cm is not None:
            await client_cm.__aexit__(None, None, None)

    # ------------------------------------------------------------------
    # MessagingBackend implementation
    # ------------------------------------------------------------------

    def _fifo_params(
        self, group_id: str | None, dedup_id: str | None, delay: int = 0
    ) -> dict:
        """Validate FIFO/standard constraints and return per-message kwargs."""
        if self._is_fifo:
            if delay:
                raise FeatureNotSupportedError(
                    "SQS FIFO queues do not support per-message delay; "
                    "use a queue-level delivery delay instead"
                )
            if not group_id:
                raise MessageSendError(
                    "group_id is required when sending to an SQS FIFO queue"
                )
            params: dict = {"MessageGroupId": group_id}
            if dedup_id:
                params["MessageDeduplicationId"] = dedup_id
            return params
        if group_id or dedup_id:
            raise FeatureNotSupportedError(
                "group_id/dedup_id are only supported on SQS FIFO queues "
                f"(queue: {self.queue_url})"
            )
        return {"DelaySeconds": delay} if delay else {}

    async def send(
        self,
        message: dict,
        delay: int = 0,
        *,
        group_id: str | None = None,
        dedup_id: str | None = None,
    ) -> str:
        client = await self._ensure()
        params = self._fifo_params(group_id, dedup_id, delay)
        try:
            response = await client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(message),
                **params,
            )
            return response["MessageId"]
        except ClientError as e:
            self._raise(e)

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
        entries = []
        for i, msg in enumerate(messages):
            params = self._fifo_params(group_id, dedup_ids[i] if dedup_ids else None)
            entries.append({"Id": str(i), "MessageBody": json.dumps(msg), **params})
        try:
            response = await client.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
            if response.get("Failed"):
                failed = [f["Id"] for f in response["Failed"]]
                raise MessageSendError(f"Failed to send messages with IDs: {failed}")
            return [s["MessageId"] for s in response.get("Successful", [])]
        except ClientError as e:
            self._raise(e)

    async def receive(
        self,
        max_messages: int = 1,
        wait_time: int = 0,
        *,
        group_id: str | None = None,
        visibility_timeout: int | None = None,
    ) -> list[Message]:
        if group_id is not None:
            raise FeatureNotSupportedError(
                "SQS cannot receive from a specific message group"
            )
        client = await self._ensure()
        kwargs: dict = {}
        if visibility_timeout is not None:
            kwargs["VisibilityTimeout"] = visibility_timeout
        try:
            response = await client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=min(max_messages, 10),
                WaitTimeSeconds=wait_time,
                AttributeNames=["All"],
                **kwargs,
            )
            messages = []
            for m in response.get("Messages", []):
                attrs = m.get("Attributes", {})
                receive_count = attrs.get("ApproximateReceiveCount")
                messages.append(
                    Message(
                        id=m["MessageId"],
                        body=json.loads(m["Body"]),
                        receipt_handle=m["ReceiptHandle"],
                        attributes=attrs,
                        group_id=attrs.get("MessageGroupId"),
                        dedup_id=attrs.get("MessageDeduplicationId"),
                        receive_count=int(receive_count) if receive_count else None,
                    )
                )
            return messages
        except ClientError as e:
            self._raise(e)

    async def nack(self, receipt_handle: str) -> None:
        """Make the message immediately visible again for redelivery."""
        client = await self._ensure()
        try:
            await client.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=0,
            )
        except ClientError as e:
            self._raise(e)

    async def delete(self, receipt_handle: str) -> None:
        client = await self._ensure()
        try:
            await client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        except ClientError as e:
            self._raise(e)

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
