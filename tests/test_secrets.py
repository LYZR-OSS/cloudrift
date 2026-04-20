import json

import boto3
import pytest
from moto.server import ThreadedMotoServer

from cloudrift.core.exceptions import SecretNotFoundError
from cloudrift.secrets import get_secrets

REGION = "us-east-1"


@pytest.fixture(scope="module")
def moto_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server._server.server_address
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
async def secrets_backend(moto_server):
    backend = get_secrets(
        "aws_secrets_manager",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region=REGION,
        endpoint_url=moto_server,
    )
    yield backend
    await backend.close()


# ---------------------------------------------------------------------------
# set / get
# ---------------------------------------------------------------------------

async def test_set_and_get_secret(secrets_backend):
    await secrets_backend.set_secret("my/secret", "s3cr3t-value")
    value = await secrets_backend.get_secret("my/secret")
    assert value == "s3cr3t-value"


async def test_set_overwrites_existing(secrets_backend):
    await secrets_backend.set_secret("overwrite/me", "first")
    await secrets_backend.set_secret("overwrite/me", "second")
    assert await secrets_backend.get_secret("overwrite/me") == "second"


async def test_get_secret_not_found(secrets_backend):
    with pytest.raises(SecretNotFoundError):
        await secrets_backend.get_secret("does/not/exist")


# ---------------------------------------------------------------------------
# get_secret_json
# ---------------------------------------------------------------------------

async def test_get_secret_json(secrets_backend):
    payload = {"db_host": "localhost", "db_port": 5432}
    await secrets_backend.set_secret("json/secret", json.dumps(payload))
    result = await secrets_backend.get_secret_json("json/secret")
    assert result == payload


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------

async def test_delete_secret(secrets_backend):
    await secrets_backend.set_secret("to/delete", "bye")
    await secrets_backend.delete_secret("to/delete")
    with pytest.raises(SecretNotFoundError):
        await secrets_backend.get_secret("to/delete")


# ---------------------------------------------------------------------------
# list_secrets
# ---------------------------------------------------------------------------

async def test_list_secrets(secrets_backend):
    await secrets_backend.set_secret("svc/alpha", "a")
    await secrets_backend.set_secret("svc/beta", "b")
    names = await secrets_backend.list_secrets()
    assert "svc/alpha" in names
    assert "svc/beta" in names


async def test_list_secrets_prefix(secrets_backend):
    await secrets_backend.set_secret("prefix/one", "1")
    await secrets_backend.set_secret("prefix/two", "2")
    names = await secrets_backend.list_secrets(prefix="prefix/")
    assert all(n.startswith("prefix/") for n in names)


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

async def test_health_check(secrets_backend):
    assert await secrets_backend.health_check() is True


# ---------------------------------------------------------------------------
# factory
# ---------------------------------------------------------------------------

def test_invalid_provider():
    with pytest.raises(ValueError, match="Unknown secrets provider"):
        get_secrets("gcp_secret_manager", project="my-proj")
