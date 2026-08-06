import boto3
import pytest
from moto.server import ThreadedMotoServer

from cloudrift.core.exceptions import CryptoError
from cloudrift.crypto import get_crypto

REGION = "us-east-1"


@pytest.fixture(scope="module")
def moto_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server._server.server_address
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def kms_key_id(moto_server):
    kms = boto3.client(
        "kms",
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        endpoint_url=moto_server,
    )
    return kms.create_key(Description="cloudrift-test")["KeyMetadata"]["KeyId"]


@pytest.fixture
async def crypto_backend(moto_server, kms_key_id):
    backend = get_crypto(
        "aws_kms",
        key_id=kms_key_id,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region=REGION,
        endpoint_url=moto_server,
    )
    yield backend
    await backend.close()


# ---------------------------------------------------------------------------
# round-trip
# ---------------------------------------------------------------------------


async def test_encrypt_decrypt_bytes(crypto_backend):
    ciphertext = await crypto_backend.encrypt(b"top-secret-token")
    assert ciphertext != b"top-secret-token"
    assert await crypto_backend.decrypt(ciphertext) == b"top-secret-token"


async def test_encrypt_decrypt_str_roundtrip(crypto_backend):
    token = "ya29.a0Af-OAuth-Access-Token-Value"
    blob = await crypto_backend.encrypt_str(token)
    assert isinstance(blob, str)
    assert blob != token
    assert await crypto_backend.decrypt_str(blob) == token


async def test_ciphertext_is_nondeterministic(crypto_backend):
    a = await crypto_backend.encrypt_str("same-input")
    b = await crypto_backend.encrypt_str("same-input")
    assert a != b  # KMS adds randomness
    assert await crypto_backend.decrypt_str(a) == "same-input"
    assert await crypto_backend.decrypt_str(b) == "same-input"


async def test_empty_string_short_circuits(crypto_backend):
    assert await crypto_backend.encrypt_str("") == ""
    assert await crypto_backend.decrypt_str("") == ""


async def test_unicode_payload(crypto_backend):
    payload = "héllo — 世界 🔐"
    assert await crypto_backend.decrypt_str(await crypto_backend.encrypt_str(payload)) == payload


# ---------------------------------------------------------------------------
# context manager + decrypt without key_id
# ---------------------------------------------------------------------------


async def test_async_context_manager(moto_server, kms_key_id):
    async with get_crypto(
        "aws_kms",
        key_id=kms_key_id,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region=REGION,
        endpoint_url=moto_server,
    ) as backend:
        blob = await backend.encrypt_str("data")
        assert await backend.decrypt_str(blob) == "data"


async def test_encrypt_without_key_id_raises(moto_server):
    backend = get_crypto(
        "aws_kms",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region=REGION,
        endpoint_url=moto_server,
    )
    try:
        with pytest.raises(CryptoError):
            await backend.encrypt(b"data")
    finally:
        await backend.close()


# ---------------------------------------------------------------------------
# factory routing
# ---------------------------------------------------------------------------


def test_get_crypto_unknown_provider():
    # Was "gcp_kms" until GCP support landed; use a name that is genuinely
    # unsupported so this keeps testing the fallthrough.
    with pytest.raises(ValueError, match="Unknown crypto provider"):
        get_crypto("not_a_provider", key_id="x")


def test_get_crypto_returns_aws_backend(moto_server):
    from cloudrift.crypto.aws_kms import AWSKMSBackend

    backend = get_crypto("aws_kms", key_id="alias/x", region=REGION)
    assert isinstance(backend, AWSKMSBackend)
