"""Unit tests for AzureBlobBackend.presigned_url auth routing.

No Azurite/emulator here — the account-scoped BlobServiceClient and the
azure-storage-blob SAS helpers are mocked. Covers the three auth paths:
account_key (local HMAC signing), AAD credential (managed identity /
service principal, via a user delegation key), and the no-credential
error case.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cloudrift.core.exceptions import StorageError
from cloudrift.storage.azure_blob import AzureBlobBackend

CONTAINER = "test-container"
ACCOUNT = "testaccount"


def _backend(*, account_key=None, credential=None) -> AzureBlobBackend:
    service = SimpleNamespace(
        account_name=ACCOUNT,
        get_user_delegation_key=AsyncMock(return_value="fake-delegation-key"),
    )
    client = SimpleNamespace(
        _service=service,
        _account_key=account_key,
        _credential=credential,
    )
    return AzureBlobBackend(CONTAINER, client)


async def test_presigned_url_signs_with_account_key_when_present():
    backend = _backend(account_key="dGhla2V5")
    with patch(
        "cloudrift.storage.azure_blob.generate_blob_sas", return_value="sig=abc"
    ) as mock_sas:
        url = await backend.presigned_url("docs/file.pdf", expires_in=600)

    assert url == f"https://{ACCOUNT}.blob.core.windows.net/{CONTAINER}/docs/file.pdf?sig=abc"
    assert mock_sas.call_args.kwargs["account_key"] == "dGhla2V5"
    assert "user_delegation_key" not in mock_sas.call_args.kwargs
    backend._service.get_user_delegation_key.assert_not_awaited()


async def test_presigned_url_falls_back_to_user_delegation_key_for_aad_auth():
    """Managed identity / service principal: no account_key, but a live AAD
    credential — must sign via get_user_delegation_key(), not raise."""
    credential = MagicMock(name="DefaultAzureCredential")
    backend = _backend(account_key=None, credential=credential)

    with patch(
        "cloudrift.storage.azure_blob.generate_blob_sas", return_value="sig=xyz"
    ) as mock_sas:
        url = await backend.presigned_url("images/a.png", expires_in=3600)

    assert url == f"https://{ACCOUNT}.blob.core.windows.net/{CONTAINER}/images/a.png?sig=xyz"
    backend._service.get_user_delegation_key.assert_awaited_once()
    start, expiry = backend._service.get_user_delegation_key.call_args.args
    assert isinstance(start, datetime) and isinstance(expiry, datetime)
    assert expiry - start >= timedelta(minutes=15)
    assert mock_sas.call_args.kwargs["user_delegation_key"] == "fake-delegation-key"
    assert "account_key" not in mock_sas.call_args.kwargs or mock_sas.call_args.kwargs["account_key"] is None


async def test_presigned_url_raises_without_any_credential():
    backend = _backend(account_key=None, credential=None)

    with pytest.raises(StorageError, match="requires account_key"):
        await backend.presigned_url("orphan.txt")

    backend._service.get_user_delegation_key.assert_not_awaited()
