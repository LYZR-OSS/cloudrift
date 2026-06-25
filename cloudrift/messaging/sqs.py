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
from cloudrift.messaging.base import Message, MessagingBackend, OutgoingMessage


class AWSSQSBackend(MessagingBackend):
    """AWS SQS messaging backend (native async via ``aioboto3``).

    A single async SQS client is created lazily and reused across operations.
    Call ``await backend.close()`` (or use ``async with backend:``) to release
    the underlying connections.

    Use one of the class methods to construct:
    - ``from_access_key``  — static credentials (+ optional session token for assumed roles)
    - ``from_iam_role``    — instance profile / environment / ECS task role
    - ``from_profile``     — named profile from ``~/.aws/credentials``
    - ``from_assume_role`` — STS AssumeRole into a target role (cross-account)
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
        self._is_fifo = queue_url.endswith(".fifo")
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
        # receipt_handle → raw message body (str as returned by SQS), retained
        # between receive() and delete()/dead_letter() so emulated dead-lettering
        # can re-send the original payload to the DLQ.
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
        exclude_env_credentials: bool = False,
        **kwargs,
    ) -> "AWSSQSBackend":
        """Authenticate via IAM role / instance profile / environment variables.

        Set ``exclude_env_credentials=True`` to drop the environment-variable
        credential provider from the resolver, so that any ``AWS_ACCESS_KEY_ID`` /
        ``AWS_SECRET_ACCESS_KEY`` set elsewhere in the process (e.g. per-request
        credentials assumed for another service) cannot shadow the long-lived
        instance / ECS task role this client should use.
        """
        session = cls._build_iam_session(region, exclude_env_credentials)
        return cls(queue_url, session, endpoint_url=endpoint_url, **kwargs)

    @staticmethod
    def _build_iam_session(region: str, exclude_env_credentials: bool) -> aioboto3.Session:
        if not exclude_env_credentials:
            return aioboto3.Session(region_name=region)
        import aiobotocore.session

        botocore_session = aiobotocore.session.AioSession()
        # The container/instance-role provider auto-refreshes; dropping "env"
        # ensures stray process env credentials can't take precedence over it.
        botocore_session.get_component("credential_provider").remove("env")
        return aioboto3.Session(botocore_session=botocore_session, region_name=region)

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

    @classmethod
    def from_assume_role(
        cls,
        queue_url: str,
        role_arn: str,
        external_id: str | None = None,
        region: str = "us-east-1",
        session_name: str = "cloudrift-sqs",
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSSQSBackend":
        """Authenticate by assuming an IAM role via STS (cross-account access).

        Calls ``sts:AssumeRole`` (optionally with ``ExternalId``) using the
        ambient credential chain, then builds the SQS session from the returned
        temporary credentials. Note: the temporary credentials are not
        auto-refreshed; construct a new backend if the session expires.
        """
        import boto3

        sts = boto3.client("sts", region_name=region)
        params: dict = {"RoleArn": role_arn, "RoleSessionName": session_name}
        if external_id:
            params["ExternalId"] = external_id
        creds = sts.assume_role(**params)["Credentials"]
        session = aioboto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=region,
        )
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
        self._pending.clear()
        if client_cm is not None:
            await client_cm.__aexit__(None, None, None)

    # ------------------------------------------------------------------
    # MessagingBackend implementation
    # ------------------------------------------------------------------

    def _fifo_params(self, group_id: str | None, dedup_id: str | None, delay: int = 0) -> dict:
        """Validate FIFO/standard constraints and return per-message kwargs."""
        if self._is_fifo:
            if delay:
                raise FeatureNotSupportedError(
                    "SQS FIFO queues do not support per-message delay; "
                    "use a queue-level delivery delay instead"
                )
            if not group_id:
                raise MessageSendError("group_id is required when sending to an SQS FIFO queue")
            params: dict = {"MessageGroupId": group_id}
            if dedup_id:
                params["MessageDeduplicationId"] = dedup_id
            return params
        if group_id or dedup_id:
            raise FeatureNotSupportedError(
                f"group_id/dedup_id are only supported on SQS FIFO queues (queue: {self.queue_url})"
            )
        return {"DelaySeconds": delay} if delay else {}

    @staticmethod
    def _message_attributes(attributes: dict[str, str] | None) -> dict:
        """Map a flat str→str attribute dict to SQS MessageAttributes (String type)."""
        if not attributes:
            return {}
        return {
            "MessageAttributes": {
                key: {"DataType": "String", "StringValue": value}
                for key, value in attributes.items()
            }
        }

    async def send(
        self,
        body: bytes,
        attributes: dict[str, str] | None = None,
        delay: int = 0,
        *,
        group_id: str | None = None,
        dedup_id: str | None = None,
    ) -> str:
        client = await self._ensure()
        params = self._fifo_params(group_id, dedup_id, delay)
        params.update(self._message_attributes(attributes))
        try:
            response = await client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=body.decode(),
                **params,
            )
            return response["MessageId"]
        except ClientError as e:
            self._raise(e)

    async def send_batch(
        self,
        messages: list[OutgoingMessage],
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
            params.update(self._message_attributes(msg.attributes))
            entries.append({"Id": str(i), "MessageBody": msg.body.decode(), **params})
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
            raise FeatureNotSupportedError("SQS cannot receive from a specific message group")
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
                MessageAttributeNames=["All"],
                **kwargs,
            )
            messages = []
            for m in response.get("Messages", []):
                system_attrs = m.get("Attributes", {})
                receive_count = system_attrs.get("ApproximateReceiveCount")
                # Surface user-defined MessageAttributes as a flat str→str map.
                attrs = {
                    key: spec.get("StringValue", "")
                    for key, spec in m.get("MessageAttributes", {}).items()
                }
                self._pending[m["ReceiptHandle"]] = m["Body"]
                messages.append(
                    Message(
                        id=m["MessageId"],
                        body=m["Body"].encode(),
                        receipt_handle=m["ReceiptHandle"],
                        attributes=attrs,
                        group_id=system_attrs.get("MessageGroupId"),
                        dedup_id=system_attrs.get("MessageDeduplicationId"),
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
        finally:
            # the handle goes stale on redelivery; redelivery stores a new one
            self._pending.pop(receipt_handle, None)

    async def delete(self, receipt_handle: str) -> None:
        client = await self._ensure()
        try:
            await client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        except ClientError as e:
            self._raise(e)
        finally:
            self._pending.pop(receipt_handle, None)

    async def dead_letter(self, receipt_handle: str, reason: str) -> None:
        """Emulated dead-letter for SQS: sends to DLQ then deletes from source.

        Warning: these are two separate API calls with no cross-queue
        transaction. If the process dies between them (or the DLQ send
        succeeds but the delete fails), the message may appear in both
        queues (double-processed) or in neither (lost). For strict
        dead-lettering, prefer the native SQS redrive policy and let the
        service move the message after maxReceiveCount is reached.
        """
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
        finally:
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
            self._pending.clear()
        except ClientError as e:
            self._raise(e)

    async def health_check(self) -> bool:
        try:
            client = await self._ensure()
            await client.get_queue_attributes(QueueUrl=self.queue_url, AttributeNames=["QueueArn"])
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
