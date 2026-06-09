import json

import boto3
import pytest
from moto.server import ThreadedMotoServer

from cloudrift.messaging import get_queue

REGION = "us-east-1"
QUEUE_NAME = "test-queue"
DLQ_NAME = "test-queue-dlq"


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


async def test_get_queue_depth(sqs_backend):
    assert await sqs_backend.get_queue_depth() == 0
    await sqs_backend.send({"a": 1})
    await sqs_backend.send({"b": 2})
    assert await sqs_backend.get_queue_depth() == 2


async def test_dead_letter(sqs_backend, sqs_client):
    await sqs_backend.send({"task": "boom"})
    messages = await sqs_backend.receive(max_messages=1)
    assert messages

    await sqs_backend.dead_letter(messages[0].receipt_handle, reason="poison message")

    # Removed from the source queue...
    assert await sqs_backend.receive(max_messages=1) == []
    # ...and delivered to the DLQ with the reason recorded.
    dlq = sqs_client.receive_message(
        QueueUrl=sqs_backend._dlq_test_url,
        MaxNumberOfMessages=1,
        MessageAttributeNames=["All"],
    )
    received = dlq["Messages"][0]
    assert json.loads(received["Body"]) == {"task": "boom"}
    assert (
        received["MessageAttributes"]["DeadLetterReason"]["StringValue"]
        == "poison message"
    )


async def test_dead_letter_unknown_handle(sqs_backend):
    with pytest.raises(Exception, match="No pending message"):
        await sqs_backend.dead_letter("bogus-handle", reason="x")
