#!/usr/bin/env python
"""End-to-end smoke test of cloudrift's GCP backends via the *service* usage path.

This deliberately mirrors how `lyzr-agent` consumes cloudrift — a pydantic
settings object that reads `*_BACKEND_PROVIDER` env vars, assembles per-provider
kwargs, and constructs long-lived clients at startup — rather than calling the
factories directly. The point is to validate the thing a service actually does:
flip an env var, get a working backend.

    export CLOUD_PLATFORM=gcp
    export GCP_PROJECT=my-project
    export STORAGE_BACKEND_PROVIDER=gcs
    ...                                     # see .env.gcp.example
    uv run python scripts/gcp_service_smoke.py

Anything unconfigured is reported as SKIP, not a failure, so this is useful even
with a partial setup. Exit code is non-zero only if something configured broke.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import traceback
import uuid
from typing import Any, Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

RUN_ID = uuid.uuid4().hex[:8]
PREFIX = f"cloudrift-smoke/{RUN_ID}"


# ---------------------------------------------------------------------------
# Settings — the same shape as lyzr-agent's api/utils/settings.py, extended
# with the GCP provider values and credential fields.
# ---------------------------------------------------------------------------


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        arbitrary_types_allowed=True,
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
        env_file=".env",
        env_file_encoding="utf-8",
    )

    # "gcp" is the new member — lyzr-agent currently allows aws/azure/self_hosted.
    cloud_platform: Literal["aws", "azure", "gcp", "self_hosted"] = Field(
        default="gcp", validation_alias="CLOUD_PLATFORM"
    )

    # --- GCP credentials. All optional: unset means Application Default
    # --- Credentials, which is what a GKE/Cloud Run workload should use.
    gcp_project: Optional[str] = Field(default=None, validation_alias="GCP_PROJECT")
    gcp_service_account_file: Optional[str] = Field(
        default=None, validation_alias="GCP_SERVICE_ACCOUNT_FILE"
    )
    # A key held in a secret store, injected as JSON — never touches disk.
    gcp_service_account_json: Optional[str] = Field(
        default=None, validation_alias="GCP_SERVICE_ACCOUNT_JSON"
    )
    # Skip ADC's GOOGLE_APPLICATION_CREDENTIALS step so a stray key file cannot
    # shadow the workload's real identity.
    gcp_prefer_metadata: bool = Field(default=False, validation_alias="GCP_PREFER_METADATA")

    # --- Storage
    storage_backend_provider: Literal["s3", "azure_blob", "gcs"] = Field(
        default="gcs", validation_alias="STORAGE_BACKEND_PROVIDER"
    )
    files_bucket_name: Optional[str] = Field(default=None, validation_alias="STORAGE_FILES_BUCKET")
    # Needed to sign URLs when the credential has no local private key.
    gcs_signer_service_account: Optional[str] = Field(
        default=None, validation_alias="GCS_SIGNER_SERVICE_ACCOUNT"
    )
    storage_client: Optional[Any] = Field(default=None, exclude=True)

    # --- Document DB
    document_backend_provider: Literal["documentdb", "cosmos", "firestore"] = Field(
        default="firestore", validation_alias="DOCUMENT_BACKEND_PROVIDER"
    )
    mongo_url: Optional[str] = Field(default=None, validation_alias="MONGO_URL")
    firestore_uid: Optional[str] = Field(default=None, validation_alias="FIRESTORE_UID")
    firestore_location: Optional[str] = Field(default=None, validation_alias="FIRESTORE_LOCATION")
    firestore_database: Optional[str] = Field(default=None, validation_alias="FIRESTORE_DATABASE")
    async_mongo_client: Optional[Any] = Field(default=None, exclude=True)

    # --- Messaging
    messaging_backend_provider: Literal["sqs", "azure_bus", "gcp_pubsub"] = Field(
        default="gcp_pubsub", validation_alias="MESSAGING_BACKEND_PROVIDER"
    )
    credit_consumption_queue: Optional[str] = Field(
        default=None, validation_alias="CREDIT_CONSUMPTION_QUEUE"
    )
    # Pub/Sub needs a topic AND a subscription, but lyzr-agent's config carries a
    # single opaque queue identifier per queue. Deriving the subscription from the
    # topic by suffix keeps that one-string contract intact — see the report at
    # the end of this script.
    gcp_subscription_suffix: str = Field(
        default="-sub", validation_alias="GCP_PUBSUB_SUBSCRIPTION_SUFFIX"
    )
    messaging_backend: Optional[Any] = Field(default=None, exclude=True)

    # --- Cache
    cache_backend_provider: Literal["redis", "elasticache", "azure_redis", "memorystore"] = Field(
        default="redis", validation_alias="CACHE_BACKEND_PROVIDER"
    )
    redis_host: Optional[str] = Field(default=None, validation_alias="REDIS_HOST")
    redis_port: int = Field(default=6379, validation_alias="REDIS_PORT")
    memorystore_auth_string: Optional[str] = Field(
        default=None, validation_alias="MEMORYSTORE_AUTH_STRING"
    )

    # --- Secrets / crypto (not yet in lyzr-agent's settings; same shape)
    secrets_backend_provider: Optional[str] = Field(
        default=None, validation_alias="SECRETS_BACKEND_PROVIDER"
    )
    crypto_backend_provider: Optional[str] = Field(
        default=None, validation_alias="CRYPTO_BACKEND_PROVIDER"
    )
    gcp_kms_key: Optional[str] = Field(default=None, validation_alias="GCP_KMS_KEY")

    # ------------------------------------------------------------------
    # Credential kwargs — assembled once, shared by every GCP factory.
    # ------------------------------------------------------------------

    def gcp_credential_kwargs(self) -> dict:
        """The credential half of every GCP factory call.

        Mirrors how the AWS branch conditionally adds access keys: pass an
        explicit identity when configured, otherwise let ADC resolve it.
        """
        if self.gcp_service_account_json:
            return {"service_account_info": json.loads(self.gcp_service_account_json)}
        if self.gcp_service_account_file:
            return {"service_account_file": self.gcp_service_account_file}
        if self.gcp_prefer_metadata:
            return {"prefer_metadata": True}
        return {}

    # ------------------------------------------------------------------
    # Construction — one method per category, matching lyzr-agent's _init_*
    # ------------------------------------------------------------------

    def init_storage(self) -> None:
        from cloudrift.storage import get_storage_client

        if self.storage_backend_provider != "gcs":
            raise RuntimeError(f"expected gcs, got {self.storage_backend_provider}")

        kwargs = self.gcp_credential_kwargs()
        # prefer_metadata is not accepted by the GCS factories: gcloud-aio-storage
        # resolves ADC internally with no injection point, so cloudrift raises
        # TypeError rather than silently ignoring it. Drop it here.
        kwargs.pop("prefer_metadata", None)
        if self.gcs_signer_service_account:
            kwargs["signer_service_account_email"] = self.gcs_signer_service_account
        self.storage_client = get_storage_client("gcs", **kwargs)

    def storage_for(self, bucket: str) -> Any:
        """Per-bucket view over the shared pool.

        GCS uses .bucket() like S3 (Azure uses .container()), so lyzr-agent's
        existing routing works by adding "gcs" to the s3 branch.
        """
        if self.storage_backend_provider in ("s3", "gcs"):
            return self.storage_client.bucket(bucket)
        return self.storage_client.container(bucket)

    def init_mongo(self) -> None:
        from cloudrift.document import get_mongodb

        if self.document_backend_provider != "firestore":
            raise RuntimeError(f"expected firestore, got {self.document_backend_provider}")
        self.async_mongo_client = get_mongodb(
            "firestore",
            uid=self.firestore_uid,
            location=self.firestore_location,
            database=self.firestore_database,
        )

    def make_queue(self, queue_identifier: str) -> Any:
        from cloudrift import get_queue

        if self.messaging_backend_provider != "gcp_pubsub":
            raise RuntimeError(f"expected gcp_pubsub, got {self.messaging_backend_provider}")
        return get_queue(
            "gcp_pubsub",
            project=self.gcp_project,
            topic=queue_identifier,
            subscription=f"{queue_identifier}{self.gcp_subscription_suffix}",
            **self.gcp_credential_kwargs(),
        )

    def make_secrets(self) -> Any:
        from cloudrift.secrets import get_secrets

        return get_secrets(
            "gcp_secret_manager", project=self.gcp_project, **self.gcp_credential_kwargs()
        )

    def make_crypto(self) -> Any:
        from cloudrift.crypto import get_crypto

        return get_crypto("gcp_kms", key_id=self.gcp_kms_key, **self.gcp_credential_kwargs())

    def make_cache(self) -> Any:
        from cloudrift.cache import get_cache

        kwargs: dict[str, Any] = {"host": self.redis_host, "port": self.redis_port}
        if self.memorystore_auth_string:
            kwargs["auth_string"] = self.memorystore_auth_string
        # NOTE: get_cache is SYNC and takes auth_method positionally.
        return get_cache("memorystore", "from_auth_string", **kwargs)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

RESULTS: list[tuple[str, str, str]] = []


def record(name: str, status: str, detail: str = "") -> None:
    RESULTS.append((name, status, detail))
    icon = {"PASS": "\033[32m✓\033[0m", "FAIL": "\033[31m✗\033[0m", "SKIP": "\033[33m—\033[0m"}[status]
    print(f"  {icon} {name}" + (f"  \033[2m{detail}\033[0m" if detail else ""), flush=True)


class check:
    """Context manager recording PASS/FAIL, keeping the run going on failure."""

    def __init__(self, name: str) -> None:
        self.name = name

    async def __aenter__(self):
        self.started = time.monotonic()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        elapsed = f"{(time.monotonic() - self.started) * 1000:.0f}ms"
        if exc is None:
            record(self.name, "PASS", elapsed)
        else:
            record(self.name, "FAIL", f"{type(exc).__name__}: {exc}")
            if os.environ.get("SMOKE_TRACEBACK"):
                traceback.print_exception(exc_type, exc, tb)
        return True  # swallow; the report carries the verdict


# ---------------------------------------------------------------------------
# Per-category exercises
# ---------------------------------------------------------------------------


async def exercise_storage(settings: Settings) -> None:
    print("\n\033[1mStorage — Cloud Storage\033[0m")
    if not settings.files_bucket_name:
        record("storage", "SKIP", "STORAGE_FILES_BUCKET unset")
        return

    settings.init_storage()
    storage = settings.storage_for(settings.files_bucket_name)
    key = f"{PREFIX}/hello.bin"
    payload = b"cloudrift gcp smoke \x00\xff"

    async with check("upload + download round-trip"):
        await storage.upload(key, payload, content_type="application/octet-stream")
        assert await storage.download(key) == payload, "payload mismatch"

    async with check("exists"):
        assert await storage.exists(key) is True

    async with check("get_metadata normalizes size to int"):
        meta = await storage.get_metadata(key)
        assert isinstance(meta["size"], int) and meta["size"] == len(payload), meta

    async with check("list by prefix"):
        keys = await storage.list(prefix=f"{PREFIX}/")
        assert key in keys, keys

    async with check("copy"):
        await storage.copy(key, f"{key}.copy")
        assert await storage.download(f"{key}.copy") == payload

    async with check("presigned_url is fetchable over HTTP"):
        import httpx

        url = await storage.presigned_url(key, expires_in=300)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url)
        assert response.status_code == 200, f"{response.status_code}: {response.text[:300]}"
        assert response.content == payload, "signed URL returned different bytes"

    async with check("ObjectNotFoundError on missing key"):
        from cloudrift.core.exceptions import ObjectNotFoundError

        try:
            await storage.download(f"{PREFIX}/absent")
            raise AssertionError("expected ObjectNotFoundError")
        except ObjectNotFoundError:
            pass

    async with check("cleanup"):
        await asyncio.gather(storage.delete(key), storage.delete(f"{key}.copy"))

    await settings.storage_client.close()


async def exercise_secrets(settings: Settings) -> None:
    print("\n\033[1mSecrets — Secret Manager\033[0m")
    if not settings.gcp_project:
        record("secrets", "SKIP", "GCP_PROJECT unset")
        return

    secrets = settings.make_secrets()
    name = f"cloudrift-smoke-{RUN_ID}"

    async with check("create + read"):
        await secrets.set_secret(name, "s3cret-v1")
        assert await secrets.get_secret(name) == "s3cret-v1"

    async with check("immutable versions: latest moves, old stays readable"):
        await secrets.set_secret(name, "s3cret-v2")
        assert await secrets.get_secret(name) == "s3cret-v2"
        assert await secrets.get_secret(name, version="1") == "s3cret-v1"

    async with check("get_secret_json"):
        await secrets.set_secret(name, json.dumps({"port": 5432}))
        assert await secrets.get_secret_json(name) == {"port": 5432}

    async with check("SecretNotFoundError on missing secret"):
        from cloudrift.core.exceptions import SecretNotFoundError

        try:
            await secrets.get_secret(f"cloudrift-smoke-{RUN_ID}-absent")
            raise AssertionError("expected SecretNotFoundError")
        except SecretNotFoundError:
            pass

    async with check("health_check"):
        assert await secrets.health_check() is True

    async with check("cleanup"):
        await secrets.delete_secret(name)

    await secrets.close()


async def exercise_crypto(settings: Settings) -> None:
    print("\n\033[1mCrypto — Cloud KMS\033[0m")
    if not settings.gcp_kms_key:
        record("crypto", "SKIP", "GCP_KMS_KEY unset")
        return

    crypto = settings.make_crypto()

    async with check("encrypt + decrypt round-trip"):
        ciphertext = await crypto.encrypt(b"envelope me")
        assert ciphertext != b"envelope me"
        assert await crypto.decrypt(ciphertext) == b"envelope me"

    async with check("encrypt_str / decrypt_str (base64)"):
        assert await crypto.decrypt_str(await crypto.encrypt_str("token")) == "token"

    async with check("ciphertext decrypts with the native Google client"):
        # Proves cloudrift does not re-wrap the provider's format.
        from google.cloud.kms import KeyManagementServiceAsyncClient

        ciphertext = await crypto.encrypt(b"interop")
        native = KeyManagementServiceAsyncClient()
        try:
            response = await native.decrypt(
                request={"name": settings.gcp_kms_key, "ciphertext": ciphertext}
            )
            assert response.plaintext == b"interop"
        finally:
            await native.transport.close()

    await crypto.close()


async def exercise_messaging(settings: Settings) -> None:
    print("\n\033[1mMessaging — Pub/Sub\033[0m")
    if not (settings.gcp_project and settings.credit_consumption_queue):
        record("messaging", "SKIP", "GCP_PROJECT or CREDIT_CONSUMPTION_QUEUE unset")
        return

    queue = settings.make_queue(settings.credit_consumption_queue)
    body = {"action": "consume_credits", "org_id": f"smoke-{RUN_ID}", "amount": 12}

    async with check("send returns a message id"):
        assert await queue.send(body, attributes={"trace": RUN_ID})

    received: list = []

    async with check("receive returns the dict that was sent"):
        for _ in range(15):
            received.extend(await queue.receive(max_messages=10, wait_time=5))
            if any(m.data.get("org_id") == f"smoke-{RUN_ID}" for m in received):
                break
        mine = [m for m in received if m.data.get("org_id") == f"smoke-{RUN_ID}"]
        assert mine, "message never arrived — check the subscription is on this topic"
        assert mine[0].data == body, mine[0].data
        assert mine[0].attributes.get("trace") == RUN_ID

    async with check("ack (delete)"):
        for m in received:
            await queue.delete(m.receipt_handle)

    async with check("get_queue_depth raises NotImplementedError (documented gap)"):
        try:
            await queue.get_queue_depth()
            raise AssertionError("expected NotImplementedError")
        except NotImplementedError:
            pass

    async with check("health_check"):
        assert await queue.health_check() is True

    await queue.close()


async def exercise_document(settings: Settings) -> None:
    print("\n\033[1mDocument DB — Firestore (MongoDB compatibility)\033[0m")
    if not (settings.firestore_uid and settings.firestore_location and settings.firestore_database):
        record("document", "SKIP", "FIRESTORE_UID / _LOCATION / _DATABASE unset")
        return

    settings.init_mongo()
    client = settings.async_mongo_client
    collection = client["cloudrift_smoke"][f"docs_{RUN_ID}"]
    doc_id = str(uuid.uuid4())

    async with check("mandatory URI options reached the driver"):
        assert client.options.load_balanced is True, "loadBalanced not applied"
        assert client.options.retry_writes is False, "retryWrites not disabled"

    async with check("insert + find (proves the URI options are server-accepted)"):
        await collection.insert_one({"_id": doc_id, "name": "Alice", "age": 30})
        found = await collection.find_one({"_id": doc_id})
        assert found and found["name"] == "Alice", found

    async with check("update"):
        await collection.update_one({"_id": doc_id}, {"$set": {"age": 31}})
        assert (await collection.find_one({"_id": doc_id}))["age"] == 31

    async with check("query + async iteration"):
        seen = [d["_id"] async for d in collection.find({"age": {"$gte": 18}})]
        assert doc_id in seen

    async with check("cleanup"):
        await collection.delete_many({"_id": doc_id})

    client.close()


async def exercise_cache(settings: Settings) -> None:
    print("\n\033[1mCache — Memorystore\033[0m")
    if settings.cache_backend_provider != "memorystore" or not settings.redis_host:
        record(
            "cache",
            "SKIP",
            "CACHE_BACKEND_PROVIDER!=memorystore or REDIS_HOST unset "
            "(Memorystore is VPC-private — run from inside the VPC)",
        )
        return

    cache = settings.make_cache()
    key = f"cloudrift-smoke:{RUN_ID}"

    async with check("ping"):
        assert await cache.ping() is True

    async with check("set/get with TTL"):
        await cache.set(key, b"value", ttl=60)
        assert await cache.get(key) == b"value"
        assert await cache.ttl(key) > 0

    async with check("transactional pipeline"):
        async with cache.pipeline() as pipe:
            pipe.sadd(f"{key}:set", "a", "b")
            pipe.expire(f"{key}:set", 60)
        assert await cache.scard(f"{key}:set") == 2

    async with check("cleanup"):
        await cache.delete(key, f"{key}:set")

    await cache.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> int:
    print(f"\033[1mcloudrift GCP service smoke test\033[0m  run={RUN_ID}")

    try:
        settings = Settings()
    except Exception as e:
        print(f"\n\033[31mSettings failed to load:\033[0m {e}")
        return 2

    creds = settings.gcp_credential_kwargs()
    identity = (
        "service_account_info (in-memory JSON)"
        if "service_account_info" in creds
        else "service_account_file"
        if "service_account_file" in creds
        else "ADC (metadata-only)"
        if creds.get("prefer_metadata")
        else "ADC"
    )
    print(f"cloud_platform={settings.cloud_platform}  project={settings.gcp_project}")
    print(f"identity={identity}")

    for exercise in (
        exercise_storage,
        exercise_secrets,
        exercise_crypto,
        exercise_messaging,
        exercise_document,
        exercise_cache,
    ):
        try:
            await exercise(settings)
        except Exception as e:  # a construction failure, not an operation failure
            record(exercise.__name__.removeprefix("exercise_"), "FAIL", f"setup: {e}")
            if os.environ.get("SMOKE_TRACEBACK"):
                traceback.print_exc()

    passed = sum(1 for _, s, _ in RESULTS if s == "PASS")
    failed = sum(1 for _, s, _ in RESULTS if s == "FAIL")
    skipped = sum(1 for _, s, _ in RESULTS if s == "SKIP")

    print(f"\n\033[1m{'=' * 60}\033[0m")
    print(f"\033[1mpassed={passed}  failed={failed}  skipped={skipped}\033[0m")
    if failed:
        print("\nfailures:")
        for name, status, detail in RESULTS:
            if status == "FAIL":
                print(f"  \033[31m✗\033[0m {name}: {detail}")
        print("\nre-run with SMOKE_TRACEBACK=1 for full tracebacks")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
