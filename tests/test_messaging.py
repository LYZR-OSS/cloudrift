import boto3
import pytest
from moto.server import ThreadedMotoServer

from cloudrift.messaging import get_queue

REGION = "us-east-1"
QUEUE_NAME = "test-queue"


@pytest.fixture(scope="module")
def moto_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server._server.server_address
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
async def sqs_backend(moto_server):
    sqs = boto3.client(
        "sqs",
        region_name=REGION,
        endpoint_url=moto_server,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    queue_url = sqs.create_queue(QueueName=QUEUE_NAME)["QueueUrl"]
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
