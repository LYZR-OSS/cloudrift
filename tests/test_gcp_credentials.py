"""Tests for the shared Google Cloud credential chain.

These assert against the real ``google.auth`` types (constructing a credential
makes no network calls — the chain is only probed on refresh), so they catch a
google-auth release that renames or drops something we depend on.
"""

import json
from unittest.mock import patch

import pytest
from google.auth import compute_engine
from google.oauth2 import service_account

from cloudrift.core.gcp_credentials import (
    CLOUD_PLATFORM_SCOPE,
    build_credentials,
    build_storage_token_kwargs,
    close_credentials,
)

# A syntactically valid but throwaway RSA key — service_account.Credentials
# parses the PEM at construction time, so a fake string will not do.
_TEST_KEY = None


def _service_account_info() -> dict:
    global _TEST_KEY
    if _TEST_KEY is None:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        _TEST_KEY = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()
    return {
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "abc123",
        "private_key": _TEST_KEY,
        "client_email": "svc@test-project.iam.gserviceaccount.com",
        "client_id": "1234567890",
        "token_uri": "https://oauth2.googleapis.com/token",
    }


# ---------------------------------------------------------------------------
# Explicit service account
# ---------------------------------------------------------------------------


def test_service_account_info_builds_service_account_credentials():
    credentials = build_credentials(service_account_info=_service_account_info())
    assert isinstance(credentials, service_account.Credentials)
    assert credentials.service_account_email == "svc@test-project.iam.gserviceaccount.com"
    assert CLOUD_PLATFORM_SCOPE in credentials.scopes


def test_service_account_file_builds_service_account_credentials(tmp_path):
    path = tmp_path / "sa.json"
    path.write_text(json.dumps(_service_account_info()))
    credentials = build_credentials(service_account_file=str(path))
    assert isinstance(credentials, service_account.Credentials)


def test_custom_scopes_are_honored():
    scope = "https://www.googleapis.com/auth/sqlservice.login"
    credentials = build_credentials(service_account_info=_service_account_info(), scopes=[scope])
    assert credentials.scopes == [scope]


# ---------------------------------------------------------------------------
# prefer_metadata — the GOOGLE_APPLICATION_CREDENTIALS shadowing guard
# ---------------------------------------------------------------------------


def test_prefer_metadata_bypasses_adc_entirely():
    """The whole point: a stray GOOGLE_APPLICATION_CREDENTIALS must not win.

    ``google.auth.default`` is patched to prove it is never consulted — if this
    ever regressed to calling it, an ambient key file would silently shadow the
    workload's attached identity.
    """
    with patch("google.auth.default") as adc:
        credentials = build_credentials(prefer_metadata=True)
    adc.assert_not_called()
    assert isinstance(credentials, compute_engine.Credentials)


def test_default_path_uses_adc():
    sentinel = object()
    with patch("google.auth.default", return_value=(sentinel, "proj")) as adc:
        credentials = build_credentials()
    adc.assert_called_once()
    assert credentials is sentinel


# ---------------------------------------------------------------------------
# Mutually exclusive arguments fail loudly
# ---------------------------------------------------------------------------


def test_file_and_info_together_raise():
    with pytest.raises(ValueError, match="not both"):
        build_credentials(
            service_account_file="/tmp/sa.json",
            service_account_info=_service_account_info(),
        )


def test_prefer_metadata_with_explicit_account_raises():
    with pytest.raises(ValueError, match="cannot be combined"):
        build_credentials(service_account_info=_service_account_info(), prefer_metadata=True)


def test_prefer_metadata_with_file_raises():
    with pytest.raises(ValueError, match="cannot be combined"):
        build_credentials(service_account_file="/tmp/sa.json", prefer_metadata=True)


# ---------------------------------------------------------------------------
# Storage token kwargs (the gcloud-aio surface)
# ---------------------------------------------------------------------------


def test_storage_token_kwargs_empty_for_adc():
    assert build_storage_token_kwargs() == {}


def test_storage_token_kwargs_passes_file_path_through():
    kwargs = build_storage_token_kwargs(service_account_file="/etc/gcp/sa.json")
    assert kwargs == {"service_file": "/etc/gcp/sa.json"}


def test_storage_token_kwargs_wraps_info_in_file_object():
    """gcloud-aio reads service_file with .read(), so a dict must be wrapped.

    Verifying the wrapper is readable JSON matters: passing the dict itself would
    fail inside the library rather than here.
    """
    info = _service_account_info()
    kwargs = build_storage_token_kwargs(service_account_info=info)
    parsed = json.loads(kwargs["service_file"].read())
    assert parsed["client_email"] == info["client_email"]


def test_storage_token_kwargs_file_and_info_together_raise():
    with pytest.raises(ValueError, match="not both"):
        build_storage_token_kwargs(
            service_account_file="/tmp/sa.json", service_account_info={"a": 1}
        )


# ---------------------------------------------------------------------------
# close_credentials
# ---------------------------------------------------------------------------


async def test_close_credentials_tolerates_credentials_without_close():
    class Bare:
        pass

    await close_credentials(Bare())  # must not raise


async def test_close_credentials_awaits_async_close():
    closed = []

    class AsyncClosable:
        async def close(self):
            closed.append(True)

    await close_credentials(AsyncClosable())
    assert closed == [True]


async def test_close_credentials_calls_sync_close():
    closed = []

    class SyncClosable:
        def close(self):
            closed.append(True)

    await close_credentials(SyncClosable())
    assert closed == [True]
