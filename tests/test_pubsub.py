import boto3
import pytest
from moto.server import ThreadedMotoServer

from cloudrift.pubsub import get_pubsub

REGION = "us-east-1"


@pytest.fixture(scope="module")
def moto_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server._server.server_address
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def topic_arn(moto_server):
    """Create a real SNS topic via boto3 against the moto server."""
    sns = boto3.client(
        "sns",
        region_name=REGION,
        endpoint_url=moto_server,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    response = sns.create_topic(Name="test-topic")
    return response["TopicArn"]


@pytest.fixture
async def pubsub_backend(moto_server):
    backend = get_pubsub(
        "sns",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region=REGION,
        endpoint_url=moto_server,
    )
    yield backend
    await backend.close()


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------

async def test_publish_returns_message_id(pubsub_backend, topic_arn):
    msg_id = await pubsub_backend.publish(topic_arn, "hello world")
    assert isinstance(msg_id, str)
    assert len(msg_id) > 0


async def test_publish_with_attributes(pubsub_backend, topic_arn):
    msg_id = await pubsub_backend.publish(
        topic_arn,
        "event payload",
        attributes={"event_type": "test", "version": "1"},
    )
    assert isinstance(msg_id, str)


# ---------------------------------------------------------------------------
# publish_batch
# ---------------------------------------------------------------------------

async def test_publish_batch(pubsub_backend, topic_arn):
    messages = [
        {"message": "msg1", "attributes": {"seq": "1"}},
        {"message": "msg2", "attributes": {"seq": "2"}},
        {"message": "msg3"},
    ]
    ids = await pubsub_backend.publish_batch(topic_arn, messages)
    assert len(ids) == 3
    assert all(isinstance(mid, str) and mid for mid in ids)


async def test_publish_batch_more_than_10(pubsub_backend, topic_arn):
    """SNS batches are chunked at 10; verify chunking works end-to-end."""
    messages = [{"message": f"m{i}"} for i in range(15)]
    ids = await pubsub_backend.publish_batch(topic_arn, messages)
    assert len(ids) == 15


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

async def test_health_check(pubsub_backend):
    assert await pubsub_backend.health_check() is True


# ---------------------------------------------------------------------------
# factory
# ---------------------------------------------------------------------------

def test_invalid_provider():
    with pytest.raises(ValueError, match="Unknown pubsub provider"):
        get_pubsub("gcp_pubsub", project="my-proj")
