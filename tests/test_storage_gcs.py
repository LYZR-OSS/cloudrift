"""Tests for the GCS storage backend.

Verified against a mocked ``gcloud-aio-storage`` ``Storage`` — there is no
in-process GCS mock in the dev extra (``fake-gcs-server`` needs Docker, so it
belongs in an opt-in integration suite).
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import ClientResponseError

from cloudrift.core.exceptions import (
    ObjectNotFoundError,
    StorageError,
    StoragePermissionError,
)
from cloudrift.storage import get_storage, get_storage_client
from cloudrift.storage.gcs import GCSBackend, GCSClient

BUCKET = "test-bucket"


def _http_error(status: int) -> ClientResponseError:
    return ClientResponseError(request_info=MagicMock(), history=(), status=status, message="boom")


def _storage(*, private_key: str | None = None):
    storage = MagicMock()
    storage.upload = AsyncMock(return_value={})
    storage.download = AsyncMock(return_value=b"data")
    storage.delete = AsyncMock()
    storage.copy = AsyncMock(return_value={})
    storage.download_metadata = AsyncMock(return_value={})
    storage.list_objects = AsyncMock(return_value={"items": []})
    storage.close = AsyncMock()
    storage.token.service_data = {"private_key": private_key} if private_key else {}
    return storage


def _backend(storage=None, *, signer_email=None, owns_client=False):
    client = GCSClient(storage or _storage(), signer_service_account_email=signer_email)
    return GCSBackend(BUCKET, client, owns_client=owns_client), client


# ---------------------------------------------------------------------------
# upload / download / delete
# ---------------------------------------------------------------------------


async def test_upload_returns_the_key_and_passes_content_type():
    storage = _storage()
    backend, _ = _backend(storage)
    assert await backend.upload("a/b.txt", b"hello", content_type="text/plain") == "a/b.txt"
    storage.upload.assert_awaited_once_with(BUCKET, "a/b.txt", b"hello", content_type="text/plain")


async def test_download_returns_bytes():
    storage = _storage()
    storage.download = AsyncMock(return_value=b"payload")
    backend, _ = _backend(storage)
    assert await backend.download("k") == b"payload"


async def test_delete_targets_the_bucket_and_key():
    storage = _storage()
    backend, _ = _backend(storage)
    await backend.delete("k")
    storage.delete.assert_awaited_once_with(BUCKET, "k")


async def test_upload_stream_buffers_the_iterator():
    """gcloud-aio takes bytes, not an async iterator — same trade-off as S3."""

    async def chunks():
        yield b"one "
        yield b"two"

    storage = _storage()
    backend, _ = _backend(storage)
    assert await backend.upload_stream("k", chunks()) == "k"
    assert storage.upload.await_args.args[2] == b"one two"


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------


async def test_exists_true_when_metadata_returns():
    backend, _ = _backend()
    assert await backend.exists("k") is True


async def test_exists_false_on_404():
    storage = _storage()
    storage.download_metadata = AsyncMock(side_effect=_http_error(404))
    backend, _ = _backend(storage)
    assert await backend.exists("k") is False


async def test_exists_propagates_a_permission_error():
    """A 403 must not be reported as 'does not exist' — that would hide a
    misconfigured IAM binding behind a falsy result."""
    storage = _storage()
    storage.download_metadata = AsyncMock(side_effect=_http_error(403))
    backend, _ = _backend(storage)
    with pytest.raises(StoragePermissionError):
        await backend.exists("k")


# ---------------------------------------------------------------------------
# list / list_iter pagination
# ---------------------------------------------------------------------------


async def test_list_iter_follows_page_tokens():
    storage = _storage()
    storage.list_objects = AsyncMock(
        side_effect=[
            {"items": [{"name": "a"}, {"name": "b"}], "nextPageToken": "t1"},
            {"items": [{"name": "c"}]},
        ]
    )
    backend, _ = _backend(storage)
    assert [key async for key in backend.list_iter()] == ["a", "b", "c"]
    assert storage.list_objects.await_args_list[1].kwargs["params"]["pageToken"] == "t1"


async def test_list_applies_the_prefix():
    storage = _storage()
    storage.list_objects = AsyncMock(return_value={"items": [{"name": "logs/x"}]})
    backend, _ = _backend(storage)
    assert await backend.list("logs/") == ["logs/x"]
    assert storage.list_objects.await_args.kwargs["params"] == {"prefix": "logs/"}


async def test_list_sends_no_prefix_param_when_empty():
    storage = _storage()
    backend, _ = _backend(storage)
    await backend.list()
    assert storage.list_objects.await_args.kwargs["params"] == {}


async def test_list_empty_bucket():
    backend, _ = _backend()
    assert await backend.list() == []


# ---------------------------------------------------------------------------
# copy / move
# ---------------------------------------------------------------------------


async def test_copy_within_the_bucket():
    storage = _storage()
    backend, _ = _backend(storage)
    assert await backend.copy("src", "dst") == "dst"
    storage.copy.assert_awaited_once_with(BUCKET, "src", BUCKET, new_name="dst")


async def test_copy_across_buckets_is_native():
    storage = _storage()
    backend, _ = _backend(storage)
    await backend.copy("src", "dst", dst_bucket="other")
    storage.copy.assert_awaited_once_with(BUCKET, "src", "other", new_name="dst")


async def test_move_copies_then_deletes():
    storage = _storage()
    backend, _ = _backend(storage)
    assert await backend.move("src", "dst") == "dst"
    storage.copy.assert_awaited_once()
    storage.delete.assert_awaited_once_with(BUCKET, "src")


# ---------------------------------------------------------------------------
# get_metadata — normalized to match S3/Azure
# ---------------------------------------------------------------------------


async def test_get_metadata_normalizes_gcs_field_names():
    storage = _storage()
    storage.download_metadata = AsyncMock(
        return_value={
            "contentType": "application/json",
            "size": "1234",
            "updated": "2026-01-15T10:30:00.000Z",
            "etag": "CJ0=",
            "metadata": {"owner": "svc"},
        }
    )
    backend, _ = _backend(storage)
    meta = await backend.get_metadata("k")

    assert meta["content_type"] == "application/json"
    # GCS reports size as a string; the other backends return int.
    assert meta["size"] == 1234
    assert isinstance(meta["last_modified"], datetime)
    assert meta["etag"] == "CJ0="
    assert meta["metadata"] == {"owner": "svc"}


async def test_get_metadata_tolerates_missing_fields():
    backend, _ = _backend()
    meta = await backend.get_metadata("k")
    assert meta["size"] is None
    assert meta["last_modified"] is None


async def test_get_metadata_falls_back_on_an_unparseable_timestamp():
    """A metadata read must not fail over a timestamp format change."""
    storage = _storage()
    storage.download_metadata = AsyncMock(return_value={"updated": "not-a-date"})
    backend, _ = _backend(storage)
    assert (await backend.get_metadata("k"))["last_modified"] == "not-a-date"


# ---------------------------------------------------------------------------
# presigned_url — the two signing paths
# ---------------------------------------------------------------------------


async def test_presigned_url_signs_locally_with_a_private_key():
    storage = _storage(private_key="-----BEGIN PRIVATE KEY-----")
    blob = MagicMock()
    blob.get_signed_url = AsyncMock(return_value="https://signed")
    storage.get_bucket.return_value.new_blob.return_value = blob
    backend, _ = _backend(storage)

    assert await backend.presigned_url("k", expires_in=900) == "https://signed"
    # No IAM client needed when the key can sign locally.
    blob.get_signed_url.assert_awaited_once_with(900)


async def test_presigned_url_uses_the_iam_api_without_a_private_key():
    storage = _storage()
    blob = MagicMock()
    blob.get_signed_url = AsyncMock(return_value="https://signed")
    storage.get_bucket.return_value.new_blob.return_value = blob
    backend, client = _backend(storage, signer_email="svc@p.iam.gserviceaccount.com")

    with patch("gcloud.aio.auth.IamClient") as iam_ctor:
        assert await backend.presigned_url("k") == "https://signed"

    iam_ctor.assert_called_once()
    kwargs = blob.get_signed_url.await_args.kwargs
    assert kwargs["service_account_email"] == "svc@p.iam.gserviceaccount.com"
    assert kwargs["iam_client"] is client._iam_client


async def test_presigned_url_without_a_signer_identity_raises():
    """Workload Identity has no local key, so signing needs a named service
    account — the GCS analog of Azure Blob requiring account_key."""
    storage = _storage()
    backend, _ = _backend(storage)
    with pytest.raises(StorageError, match="signer_service_account_email"):
        await backend.presigned_url("k")


async def test_presigned_url_rejects_an_expiry_over_seven_days():
    backend, _ = _backend(_storage(private_key="key"))
    with pytest.raises(StorageError, match="604800"):
        await backend.presigned_url("k", expires_in=604801)


async def test_iam_client_is_built_once_and_reused():
    storage = _storage()
    blob = MagicMock()
    blob.get_signed_url = AsyncMock(return_value="https://signed")
    storage.get_bucket.return_value.new_blob.return_value = blob
    backend, _ = _backend(storage, signer_email="svc@p.iam.gserviceaccount.com")

    with patch("gcloud.aio.auth.IamClient") as iam_ctor:
        await backend.presigned_url("a")
        await backend.presigned_url("b")

    iam_ctor.assert_called_once()


async def test_user_credentials_cannot_sign():
    """gcloud user ADC raises TypeError inside IamClient; that must surface as an
    actionable StorageError rather than a bare TypeError."""
    storage = _storage()
    storage.get_bucket.return_value.new_blob.return_value = MagicMock()
    backend, _ = _backend(storage, signer_email="svc@p.iam.gserviceaccount.com")

    with patch("gcloud.aio.auth.IamClient", side_effect=TypeError("not a SA")):
        with pytest.raises(StorageError, match="service-account or metadata-server"):
            await backend.presigned_url("k")


# ---------------------------------------------------------------------------
# Error translation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status,expected",
    [
        (404, ObjectNotFoundError),
        (403, StoragePermissionError),
        (401, StoragePermissionError),
        (500, StorageError),
    ],
)
async def test_http_errors_are_translated(status, expected):
    storage = _storage()
    storage.download = AsyncMock(side_effect=_http_error(status))
    backend, _ = _backend(storage)
    with pytest.raises(expected):
        await backend.download("k")


async def test_not_found_names_the_key():
    storage = _storage()
    storage.download = AsyncMock(side_effect=_http_error(404))
    backend, _ = _backend(storage)
    with pytest.raises(ObjectNotFoundError, match="missing.txt"):
        await backend.download("missing.txt")


# ---------------------------------------------------------------------------
# Lifecycle: who owns the connection pool
# ---------------------------------------------------------------------------


async def test_a_shared_view_does_not_close_the_client():
    storage = _storage()
    client = GCSClient(storage)
    view = client.bucket(BUCKET)
    await view.close()
    storage.close.assert_not_awaited()


async def test_the_account_client_closes_the_pool():
    storage = _storage()
    client = GCSClient(storage)
    client.bucket(BUCKET)
    await client.close()
    storage.close.assert_awaited_once()


async def test_a_single_bucket_view_owns_its_client():
    storage = _storage()
    backend, _ = _backend(storage, owns_client=True)
    await backend.close()
    storage.close.assert_awaited_once()


async def test_views_from_one_client_share_the_pool():
    storage = _storage()
    client = GCSClient(storage)
    first = client.bucket("one")
    second = client.bucket("two")
    assert first._storage is second._storage is storage


async def test_client_context_manager_closes():
    storage = _storage()
    async with GCSClient(storage):
        pass
    storage.close.assert_awaited_once()


async def test_health_check_is_true_when_reachable():
    storage = _storage()
    storage.download_metadata = AsyncMock(side_effect=_http_error(404))
    backend, _ = _backend(storage)
    # A 404 on the probe key still proves the API answered.
    assert await backend.health_check() is True


# ---------------------------------------------------------------------------
# Factory routing
# ---------------------------------------------------------------------------


def test_get_storage_routes_by_credential_keys():
    with patch.object(GCSBackend, "from_service_account_file") as target:
        get_storage("gcs", bucket=BUCKET, service_account_file="/tmp/sa.json")
    target.assert_called_once()

    with patch.object(GCSBackend, "from_service_account_info") as target:
        get_storage("gcs", bucket=BUCKET, service_account_info={})
    target.assert_called_once()

    with patch.object(GCSBackend, "from_application_default") as target:
        get_storage("gcs", bucket=BUCKET)
    target.assert_called_once()


def test_get_storage_client_routes_to_the_account_client():
    with patch.object(GCSClient, "from_application_default") as target:
        get_storage_client("gcs")
    target.assert_called_once()


def test_application_default_builds_a_storage_without_credentials_kwargs():
    """ADC must reach gcloud-aio as *no* service_file, letting it resolve the
    chain itself."""
    with patch("cloudrift.storage.gcs.Storage") as ctor:
        GCSClient.from_application_default()
    assert "service_file" not in ctor.call_args.kwargs


def test_service_account_file_is_passed_to_storage():
    with patch("cloudrift.storage.gcs.Storage") as ctor:
        GCSClient.from_service_account_file("/etc/gcp/sa.json")
    assert ctor.call_args.kwargs["service_file"] == "/etc/gcp/sa.json"


def test_api_root_is_forwarded_for_emulators():
    with patch("cloudrift.storage.gcs.Storage") as ctor:
        GCSClient.from_application_default(api_root="http://localhost:4443")
    assert ctor.call_args.kwargs["api_root"] == "http://localhost:4443"


def test_prefer_metadata_is_rejected_rather_than_ignored():
    """gcloud-aio resolves ADC internally with no hook, so the flag cannot be
    honored — it must fail loudly instead of silently doing nothing."""
    with pytest.raises(TypeError):
        get_storage("gcs", bucket=BUCKET, prefer_metadata=True)


def test_unknown_provider_error_lists_gcs():
    with pytest.raises(ValueError, match="gcs"):
        get_storage("nope")
