import boto3
import pytest
from moto.server import ThreadedMotoServer

from cloudrift.core.exceptions import ObjectNotFoundError
from cloudrift.storage import get_storage

BUCKET = "test-bucket"
REGION = "us-east-1"


@pytest.fixture(scope="module")
def moto_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server._server.server_address
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
async def s3_backend(moto_server, request):
    bucket = f"{BUCKET}-{request.node.name}".lower().replace("_", "-")
    sync = boto3.client(
        "s3",
        region_name=REGION,
        endpoint_url=moto_server,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    sync.create_bucket(Bucket=bucket)
    backend = get_storage(
        "s3",
        bucket=bucket,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region=REGION,
        endpoint_url=moto_server,
    )
    yield backend
    await backend.close()
    for obj in sync.list_objects_v2(Bucket=bucket).get("Contents", []):
        sync.delete_object(Bucket=bucket, Key=obj["Key"])
    sync.delete_bucket(Bucket=bucket)


async def test_upload_and_download(s3_backend):
    key = await s3_backend.upload("hello.txt", b"hello world", content_type="text/plain")
    assert key == "hello.txt"
    data = await s3_backend.download("hello.txt")
    assert data == b"hello world"


async def test_exists(s3_backend):
    assert not await s3_backend.exists("missing.txt")
    await s3_backend.upload("present.txt", b"data")
    assert await s3_backend.exists("present.txt")


async def test_delete(s3_backend):
    await s3_backend.upload("to_delete.txt", b"bye")
    await s3_backend.delete("to_delete.txt")
    assert not await s3_backend.exists("to_delete.txt")


async def test_list(s3_backend):
    await s3_backend.upload("logs/a.txt", b"a")
    await s3_backend.upload("logs/b.txt", b"b")
    await s3_backend.upload("data/c.txt", b"c")
    keys = await s3_backend.list(prefix="logs/")
    assert set(keys) == {"logs/a.txt", "logs/b.txt"}


async def test_download_missing_raises(s3_backend):
    with pytest.raises(ObjectNotFoundError):
        await s3_backend.download("nonexistent.txt")


async def test_close_is_idempotent(s3_backend):
    await s3_backend.close()
    await s3_backend.close()


def test_invalid_provider():
    with pytest.raises(ValueError, match="Unknown storage provider"):
        get_storage("gcs", bucket="x")


# --- New tests for P0/P1 features ---


async def test_list_iter(s3_backend):
    await s3_backend.upload("iter/a.txt", b"a")
    await s3_backend.upload("iter/b.txt", b"b")
    await s3_backend.upload("other/c.txt", b"c")
    keys = []
    async for key in s3_backend.list_iter(prefix="iter/"):
        keys.append(key)
    assert set(keys) == {"iter/a.txt", "iter/b.txt"}


async def test_health_check(s3_backend):
    result = await s3_backend.health_check()
    assert result is True


async def test_copy(s3_backend):
    await s3_backend.upload("src.txt", b"source data")
    dst = await s3_backend.copy("src.txt", "dst.txt")
    assert dst == "dst.txt"
    data = await s3_backend.download("dst.txt")
    assert data == b"source data"
    # Original still exists
    assert await s3_backend.exists("src.txt")


async def test_move(s3_backend):
    await s3_backend.upload("move_src.txt", b"move data")
    dst = await s3_backend.move("move_src.txt", "move_dst.txt")
    assert dst == "move_dst.txt"
    data = await s3_backend.download("move_dst.txt")
    assert data == b"move data"
    assert not await s3_backend.exists("move_src.txt")


async def test_get_metadata(s3_backend):
    await s3_backend.upload("meta.txt", b"metadata test", content_type="text/plain")
    meta = await s3_backend.get_metadata("meta.txt")
    assert meta["content_type"] == "text/plain"
    assert meta["size"] == 13
    assert "etag" in meta


async def test_upload_stream(s3_backend):

    async def _stream():
        yield b"chunk1"
        yield b"chunk2"
        yield b"chunk3"

    key = await s3_backend.upload_stream("streamed.txt", _stream(), content_type="text/plain")
    assert key == "streamed.txt"
    data = await s3_backend.download("streamed.txt")
    assert data == b"chunk1chunk2chunk3"
