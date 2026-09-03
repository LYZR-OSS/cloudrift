"""Tests for the GCP Cloud KMS crypto backend.

Verified against a mocked ``KeyManagementServiceAsyncClient`` — moto covers AWS
KMS but has no Cloud KMS equivalent.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.api_core.exceptions import (
    FailedPrecondition,
    InvalidArgument,
    NotFound,
    PermissionDenied,
)

from cloudrift.core.exceptions import (
    CryptoError,
    CryptoKeyNotFoundError,
    CryptoPermissionError,
)
from cloudrift.crypto import get_crypto
from cloudrift.crypto.gcp_kms import GCPKMSBackend

KEY = "projects/p/locations/us/keyRings/r/cryptoKeys/k"


def _backend(client=None, **kwargs):
    backend = GCPKMSBackend(KEY, **kwargs)
    backend._client = client if client is not None else MagicMock()
    return backend


def _encrypt_client(ciphertext=b"cipher"):
    client = MagicMock()
    client.encrypt = AsyncMock(return_value=MagicMock(ciphertext=ciphertext))
    return client


def _decrypt_client(plaintext=b"plain"):
    client = MagicMock()
    client.decrypt = AsyncMock(return_value=MagicMock(plaintext=plaintext))
    return client


# ---------------------------------------------------------------------------
# encrypt / decrypt
# ---------------------------------------------------------------------------


async def test_encrypt_sends_the_key_and_plaintext():
    client = _encrypt_client()
    assert await _backend(client).encrypt(b"secret") == b"cipher"
    client.encrypt.assert_awaited_once_with(request={"name": KEY, "plaintext": b"secret"})


async def test_decrypt_sends_the_key_and_ciphertext():
    """Unlike AWS KMS, Cloud KMS needs the key name on decrypt too."""
    client = _decrypt_client()
    assert await _backend(client).decrypt(b"cipher") == b"plain"
    client.decrypt.assert_awaited_once_with(request={"name": KEY, "ciphertext": b"cipher"})


async def test_encrypt_without_a_key_raises():
    backend = GCPKMSBackend()
    backend._client = MagicMock()
    with pytest.raises(CryptoError, match="key_id is required"):
        await backend.encrypt(b"x")


async def test_decrypt_without_a_key_raises():
    backend = GCPKMSBackend()
    backend._client = MagicMock()
    with pytest.raises(CryptoError, match="key_id is required"):
        await backend.decrypt(b"x")


# ---------------------------------------------------------------------------
# Additional authenticated data
# ---------------------------------------------------------------------------


async def test_aad_is_bound_into_both_directions():
    """AAD must be sent on encrypt *and* decrypt or decryption fails — the
    analog of an AWS KMS encryption context."""
    client = _encrypt_client()
    client.decrypt = AsyncMock(return_value=MagicMock(plaintext=b"plain"))
    backend = _backend(client, additional_authenticated_data=b"tenant-42")

    await backend.encrypt(b"secret")
    await backend.decrypt(b"cipher")

    assert client.encrypt.await_args.kwargs["request"]["additional_authenticated_data"] == (
        b"tenant-42"
    )
    assert client.decrypt.await_args.kwargs["request"]["additional_authenticated_data"] == (
        b"tenant-42"
    )


async def test_no_aad_key_when_unset():
    client = _encrypt_client()
    await _backend(client).encrypt(b"secret")
    assert "additional_authenticated_data" not in client.encrypt.await_args.kwargs["request"]


# ---------------------------------------------------------------------------
# The base-class string helpers work over this backend
# ---------------------------------------------------------------------------


async def test_encrypt_str_round_trip_through_base64():
    import base64

    client = _encrypt_client(ciphertext=b"\x00\x01\xfe raw bytes")
    backend = _backend(client)
    encoded = await backend.encrypt_str("hello")
    assert base64.b64decode(encoded) == b"\x00\x01\xfe raw bytes"


async def test_encrypt_str_short_circuits_on_empty_input():
    client = _encrypt_client()
    assert await _backend(client).encrypt_str("") == ""
    client.encrypt.assert_not_awaited()


# ---------------------------------------------------------------------------
# Error translation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc,expected",
    [
        (NotFound("missing"), CryptoKeyNotFoundError),
        (PermissionDenied("denied"), CryptoPermissionError),
        # A disabled/destroyed key version exists but is unusable — the Cloud KMS
        # equivalent of AWS KeyUnavailableException.
        (FailedPrecondition("disabled"), CryptoKeyNotFoundError),
        (InvalidArgument("too big"), CryptoError),
    ],
)
async def test_native_errors_are_translated(exc, expected):
    client = MagicMock()
    client.encrypt = AsyncMock(side_effect=exc)
    with pytest.raises(expected):
        await _backend(client).encrypt(b"x")


# ---------------------------------------------------------------------------
# Lifecycle + factory routing
# ---------------------------------------------------------------------------


async def test_close_closes_the_transport_and_is_idempotent():
    client = MagicMock()
    client.transport.close = AsyncMock()
    backend = _backend(client)

    await backend.close()
    await backend.close()

    client.transport.close.assert_awaited_once()


async def test_context_manager_closes():
    client = MagicMock()
    client.transport.close = AsyncMock()
    backend = _backend(client)
    async with backend:
        pass
    client.transport.close.assert_awaited_once()


async def test_client_is_built_once_and_reused():
    backend = GCPKMSBackend(KEY)
    with patch("cloudrift.crypto.gcp_kms.KeyManagementServiceAsyncClient") as ctor:
        first = await backend._ensure()
        second = await backend._ensure()
    assert first is second
    ctor.assert_called_once()


def test_factory_routes_by_credential_keys():
    with patch.object(GCPKMSBackend, "from_service_account_file") as target:
        get_crypto("gcp_kms", key_id=KEY, service_account_file="/tmp/sa.json")
    target.assert_called_once()

    with patch.object(GCPKMSBackend, "from_service_account_info") as target:
        get_crypto("gcp_kms", key_id=KEY, service_account_info={})
    target.assert_called_once()

    with patch.object(GCPKMSBackend, "from_application_default") as target:
        get_crypto("gcp_kms", key_id=KEY)
    target.assert_called_once()


def test_cloud_kms_alias_is_accepted():
    with patch.object(GCPKMSBackend, "from_application_default") as target:
        get_crypto("cloud_kms", key_id=KEY)
    target.assert_called_once()


def test_unknown_provider_error_lists_gcp():
    with pytest.raises(ValueError, match="gcp_kms"):
        get_crypto("nope")
