import json

import boto3
import pytest
from moto.server import ThreadedMotoServer

from cloudrift.core.exceptions import FeatureNotSupportedError, MessageSendError
from cloudrift.messaging import get_queue

REGION = "us-east-1"
QUEUE_NAME = "test-queue"
FIFO_QUEUE_NAME = "test-queue.fifo"


@pytest.fixture(scope="module")
def moto_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server._server.server_address
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def sqs_client(moto_server):
    return boto3.client(
        "sqs",
        region_name=REGION,
        endpoint_url=moto_server,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture
async def sqs_backend(moto_server, sqs_client):
    # Create a DLQ and wire the source queue to it via RedrivePolicy so
    # dead_letter() can resolve the target from the queue itself.
    dlq_url = sqs_client.create_queue(QueueName=DLQ_NAME)["QueueUrl"]
    dlq_arn = sqs_client.get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    queue_url = sqs_client.create_queue(
        QueueName=QUEUE_NAME,
        Attributes={
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"}
            )
        },
    )["QueueUrl"]
    backend = get_queue(
        "sqs",
        queue_url=queue_url,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region=REGION,
        endpoint_url=moto_server,
    )
    backend._dlq_test_url = dlq_url  # expose for assertions
    yield backend
    await backend.close()
    sqs_client.delete_queue(QueueUrl=queue_url)
    sqs_client.delete_queue(QueueUrl=dlq_url)


async def test_send_and_receive(sqs_backend):
    msg_id = await sqs_backend.send({"action": "greet", "name": "cloudrift"})
    assert isinstance(msg_id, str)
    messages = await sqs_backend.receive(max_messages=1)
    assert len(messages) == 1
    assert messages[0].body == {"action": "greet", "name": "cloudrift"}


async def test_send_batch(sqs_backend):
    ids = await sqs_backend.send_batch([{"n": 1}, {"n": 2}, {"n": 3}])
    assert len(ids) == 3


async def test_delete_message(sqs_backend):
    await sqs_backend.send({"x": 1})
    messages = await sqs_backend.receive(max_messages=1)
    assert messages
    await sqs_backend.delete(messages[0].receipt_handle)


async def test_purge(sqs_backend):
    await sqs_backend.send({"a": 1})
    await sqs_backend.send({"b": 2})
    await sqs_backend.purge()
    messages = await sqs_backend.receive(max_messages=10)
    assert messages == []


def test_invalid_provider():
    with pytest.raises(ValueError, match="Unknown messaging provider"):
        get_queue("rabbitmq", queue_url="x")


# --- New tests for P0 features ---


async def test_health_check(sqs_backend):
    result = await sqs_backend.health_check()
    assert result is True


async def test_standard_queue_zero_delay_omits_delay_seconds(sqs_backend):
    # Regression: DelaySeconds must be omitted when delay == 0 so the same
    # code path works on FIFO queues (which reject the parameter).
    msg_id = await sqs_backend.send({"ping": True})
    assert isinstance(msg_id, str)


async def test_group_id_on_standard_queue_raises(sqs_backend):
    with pytest.raises(FeatureNotSupportedError):
        await sqs_backend.send({"x": 1}, group_id="g1")


# --- FIFO tests ---


@pytest.fixture
async def fifo_backend(moto_server):
    sqs = boto3.client(
        "sqs",
        region_name=REGION,
        endpoint_url=moto_server,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    queue_url = sqs.create_queue(
        QueueName=FIFO_QUEUE_NAME,
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )["QueueUrl"]
    backend = get_queue(
        "sqs",
        queue_url=queue_url,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region=REGION,
        endpoint_url=moto_server,
    )
    yield backend
    await backend.close()
    sqs.delete_queue(QueueUrl=queue_url)


async def test_fifo_send_requires_group_id(fifo_backend):
    with pytest.raises(MessageSendError, match="group_id is required"):
        await fifo_backend.send({"x": 1})


async def test_fifo_send_with_delay_raises(fifo_backend):
    with pytest.raises(FeatureNotSupportedError):
        await fifo_backend.send({"x": 1}, delay=5, group_id="g1")


async def test_fifo_send_and_receive_exposes_fifo_fields(fifo_backend):
    await fifo_backend.send({"n": 1}, group_id="owner-1", dedup_id="d-1")
    messages = await fifo_backend.receive(max_messages=1)
    assert len(messages) == 1
    m = messages[0]
    assert m.body == {"n": 1}
    assert m.group_id == "owner-1"
    assert m.dedup_id == "d-1"
    assert m.receive_count == 1
    await fifo_backend.delete(m.receipt_handle)


async def test_fifo_dedup_id_suppresses_duplicate(fifo_backend):
    await fifo_backend.send({"n": 1}, group_id="g-dedup", dedup_id="same-id")
    await fifo_backend.send({"n": 2}, group_id="g-dedup", dedup_id="same-id")
    messages = await fifo_backend.receive(max_messages=10)
    assert len(messages) == 1
    await fifo_backend.delete(messages[0].receipt_handle)


async def test_fifo_ordering_within_group(fifo_backend):
    for i in range(3):
        await fifo_backend.send({"seq": i}, group_id="g-order", dedup_id=f"ord-{i}")
    received = []
    while len(received) < 3:
        messages = await fifo_backend.receive(max_messages=10)
        if not messages:
            break
        for m in messages:
            received.append(m.body["seq"])
            await fifo_backend.delete(m.receipt_handle)
    assert received == [0, 1, 2]


async def test_fifo_send_batch_with_group_and_dedup_ids(fifo_backend):
    ids = await fifo_backend.send_batch(
        [{"n": 1}, {"n": 2}], group_id="g-batch", dedup_ids=["b-1", "b-2"]
    )
    assert len(ids) == 2
    messages = await fifo_backend.receive(max_messages=10)
    assert {m.body["n"] for m in messages} == {1, 2}
    for m in messages:
        await fifo_backend.delete(m.receipt_handle)


async def test_fifo_send_batch_mismatched_dedup_ids(fifo_backend):
    with pytest.raises(MessageSendError, match="parallel"):
        await fifo_backend.send_batch([{"n": 1}], group_id="g", dedup_ids=["a", "b"])


async def test_nack_redelivers_immediately(fifo_backend):
    await fifo_backend.send({"task": "retry-me"}, group_id="g-nack", dedup_id="nack-1")
    first = await fifo_backend.receive(max_messages=1, visibility_timeout=300)
    assert len(first) == 1
    await fifo_backend.nack(first[0].receipt_handle)
    second = await fifo_backend.receive(max_messages=1)
    assert len(second) == 1
    assert second[0].body == {"task": "retry-me"}
    assert second[0].receive_count == 2
    await fifo_backend.delete(second[0].receipt_handle)


async def test_receive_with_group_id_raises_on_sqs(fifo_backend):
    with pytest.raises(FeatureNotSupportedError):
        await fifo_backend.receive(group_id="g1")


async def test_dlq_redrive_flow(moto_server):
    """Mirror lyzr-memory's DLQ redrive: receive from DLQ, re-send to the main
    FIFO queue with a fresh dedup ID, delete from DLQ."""
    sqs = boto3.client(
        "sqs",
        region_name=REGION,
        endpoint_url=moto_server,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    fifo_attrs = {"FifoQueue": "true", "ContentBasedDeduplication": "true"}
    main_url = sqs.create_queue(QueueName="main-redrive.fifo", Attributes=fifo_attrs)["QueueUrl"]
    dlq_url = sqs.create_queue(QueueName="dlq-redrive.fifo", Attributes=fifo_attrs)["QueueUrl"]
    creds = {
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region": REGION,
        "endpoint_url": moto_server,
    }
    main_q = get_queue("sqs", queue_url=main_url, **creds)
    dlq = get_queue("sqs", queue_url=dlq_url, **creds)
    try:
        await dlq.send({"owner_id": "u1", "messages": ["hi"]}, group_id="u1", dedup_id="orig-1")
        failed = await dlq.receive(max_messages=10)
        assert len(failed) == 1
        m = failed[0]
        await main_q.send(m.body, group_id=m.group_id, dedup_id="redrive-abc-123")
        await dlq.delete(m.receipt_handle)
        redriven = await main_q.receive(max_messages=1)
        assert redriven[0].body == {"owner_id": "u1", "messages": ["hi"]}
        assert redriven[0].group_id == "u1"
    finally:
        await main_q.close()
        await dlq.close()
        sqs.delete_queue(QueueUrl=main_url)
        sqs.delete_queue(QueueUrl=dlq_url)
