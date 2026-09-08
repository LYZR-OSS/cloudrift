"""Tests for the Azure Key Vault keys crypto backend (envelope encryption).

Verified against a mocked ``CryptographyClient`` — moto covers AWS KMS but there
is no local Key Vault. The fake RSA wrap/unwrap enforces the real ~190-byte
RSA-2048 input ceiling, so any test that encrypts a larger payload proves the
envelope keeps the payload away from RSA.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from cloudrift.core.exceptions import CryptoError
from cloudrift.crypto.azure_keyvault_keys import _ENVELOPE_MAGIC, AzureKeyVaultKeysBackend

# RSA-2048 with OAEP-SHA256 can encrypt at most ~190 bytes.
_RSA_INPUT_CEILING = 190


def _backend():
    """Backend wired to a fake Key Vault client that simulates RSA wrap/unwrap of
    the (small) data key and refuses inputs over the real RSA ceiling."""
    backend = AzureKeyVaultKeysBackend("https://v.vault.azure.net/keys/k", credential=MagicMock())

    async def _rsa_encrypt(algorithm, plaintext):
        assert len(plaintext) <= _RSA_INPUT_CEILING, "payload must never reach RSA directly"
        return MagicMock(ciphertext=b"RSA:" + plaintext)

    async def _rsa_decrypt(algorithm, ciphertext):
        assert ciphertext.startswith(b"RSA:")
        return MagicMock(plaintext=ciphertext[4:])

    client = MagicMock()
    client.encrypt = AsyncMock(side_effect=_rsa_encrypt)
    client.decrypt = AsyncMock(side_effect=_rsa_decrypt)
    backend._client = client  # bypass _ensure() / real Azure auth
    return backend


@pytest.mark.parametrize("size", [1, _RSA_INPUT_CEILING, _RSA_INPUT_CEILING + 1, 2048, 8192])
async def test_envelope_roundtrip_any_size(size):
    backend = _backend()
    payload = b"T" * size
    blob = await backend.encrypt(payload)
    assert blob[: len(_ENVELOPE_MAGIC)] == _ENVELOPE_MAGIC
    assert await backend.decrypt(blob) == payload


async def test_payload_never_reaches_rsa():
    # An 8 KB payload would blow the RSA ceiling if it were encrypted directly;
    # the fake client asserts len <= 190, so this passing proves envelope mode.
    backend = _backend()
    await backend.encrypt(b"x" * 8192)


async def test_str_helpers_roundtrip_oauth_sized_token():
    backend = _backend()
    token = "ya29." + "A" * 1500  # OAuth-token-sized, far over the RSA ceiling
    blob = await backend.encrypt_str(token)
    assert isinstance(blob, str)
    assert await backend.decrypt_str(blob) == token


async def test_legacy_direct_rsa_decrypt():
    # Ciphertext written before envelope mode: raw RSA, no magic prefix.
    backend = _backend()
    assert await backend.decrypt(b"RSA:legacy-secret") == b"legacy-secret"


async def test_truncated_envelope_raises_cryptoerror():
    # Magic present but no room for the length header: must surface a cloudrift
    # CryptoError, not a raw struct.error (backend-boundary rule).
    backend = _backend()
    with pytest.raises(CryptoError):
        await backend.decrypt(_ENVELOPE_MAGIC)  # 4 bytes; header needs 6
    with pytest.raises(CryptoError):
        await backend.decrypt(_ENVELOPE_MAGIC + b"\x01")


async def test_tampered_body_raises_cryptoerror():
    backend = _backend()
    blob = bytearray(await backend.encrypt(b"secret"))
    blob[-1] ^= 0x01  # flip a GCM tag byte
    with pytest.raises(CryptoError):
        await backend.decrypt(bytes(blob))
