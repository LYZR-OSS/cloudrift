"""Live GCP integration tests.

These hit real Google Cloud APIs and cost real (small) money. They exist to cover
what the mocked unit tests structurally cannot:

- that the **credential chain** actually authenticates,
- that a **signed URL is genuinely valid** — fetched over HTTP and byte-compared,
  which is the one thing a mocked signer can never prove,
- that **error translation** fires on real API responses rather than
  hand-constructed exceptions,
- that the **Firestore URI options** are accepted by a real endpoint, not just by
  ``uri_parser``,
- that **Pub/Sub ordering keys, ack/nack, and seek** behave as documented.

Every test cleans up what it creates. Resource names carry a per-run token, so a
crashed run leaves identifiable litter rather than blocking the next run.
"""

import asyncio
import json
import re
import uuid

import httpx
import pytest

from cloudrift.cache import get_cache
from cloudrift.core.exceptions import (
    CacheError,
    CryptoError,
    ObjectNotFoundError,
    SecretNotFoundError,
)
from cloudrift.crypto import get_crypto
from cloudrift.document import get_mongodb, get_mongodb_sync
from cloudrift.messaging import get_queue
from cloudrift.messaging.base import OutgoingMessage
from cloudrift.pubsub import get_pubsub
from cloudrift.secrets import get_secrets
from cloudrift.storage import get_storage, get_storage_client

from .conftest import RESOURCE_PREFIX

# ---------------------------------------------------------------------------
# Storage — Cloud Storage
# ---------------------------------------------------------------------------


@pytest.fixture
async def storage(gcs_bucket, signer_sa):
    kwargs = {"bucket": gcs_bucket}
    if signer_sa:
        kwargs["signer_service_account_email"] = signer_sa
    backend = get_storage("gcs", **kwargs)
    yield backend
    await backend.close()


@pytest.fixture
def key(run_id, request) -> str:
    return f"{RESOURCE_PREFIX}/{run_id}/{request.node.name}.bin"


async def test_storage_round_trip(storage, key):
    payload = b"hello from cloudrift \x00\xff binary-safe"
    assert await storage.upload(key, payload, content_type="application/octet-stream") == key
    try:
        assert await storage.download(key) == payload
        assert await storage.exists(key) is True
    finally:
        await storage.delete(key)
    assert await storage.exists(key) is False


async def test_storage_missing_object_raises_translated_error(storage, run_id):
    """Real 404 from GCS must arrive as ObjectNotFoundError, not aiohttp's error."""
    with pytest.raises(ObjectNotFoundError):
        await storage.download(f"{RESOURCE_PREFIX}/{run_id}/definitely-absent")


async def test_storage_metadata_is_normalized(storage, key):
    from datetime import datetime

    await storage.upload(key, b"12345", content_type="text/plain")
    try:
        meta = await storage.get_metadata(key)
        assert meta["content_type"] == "text/plain"
        # GCS returns size as a string over the wire; cloudrift normalizes to int
        # so callers can swap providers without re-typing this field.
        assert meta["size"] == 5
        assert isinstance(meta["size"], int)
        assert isinstance(meta["last_modified"], datetime)
        assert meta["etag"]
    finally:
        await storage.delete(key)


async def test_storage_list_and_pagination(storage, run_id):
    prefix = f"{RESOURCE_PREFIX}/{run_id}/list/"
    keys = [f"{prefix}{i:02d}.txt" for i in range(5)]
    await asyncio.gather(*(storage.upload(k, b"x") for k in keys))
    try:
        assert sorted(await storage.list(prefix=prefix)) == sorted(keys)
        streamed = [k async for k in storage.list_iter(prefix=prefix)]
        assert sorted(streamed) == sorted(keys)
    finally:
        await asyncio.gather(*(storage.delete(k) for k in keys))


async def test_storage_cross_bucket_copy_is_native(storage, key):
    """GCS copies server-side in one call, unlike Azure's async start_copy."""
    await storage.upload(key, b"source bytes")
    dst = f"{key}.copy"
    try:
        assert await storage.copy(key, dst) == dst
        assert await storage.download(dst) == b"source bytes"
    finally:
        await asyncio.gather(storage.delete(key), storage.delete(dst), return_exceptions=True)


async def test_storage_upload_stream(storage, key):
    async def chunks():
        for i in range(3):
            yield f"chunk{i}".encode()

    await storage.upload_stream(key, chunks(), content_type="text/plain")
    try:
        assert await storage.download(key) == b"chunk0chunk1chunk2"
    finally:
        await storage.delete(key)


async def test_presigned_url_is_actually_fetchable(storage, key, signer_sa):
    """The test a mock cannot write: does the signature actually verify?

    Signing is the highest-risk part of the GCS backend — under Workload Identity
    there is no local key and it goes through the IAM signBlob API. A wrong
    canonical request still *produces* a URL; only fetching it proves correctness.
    """
    # Under ADC / Workload Identity there is no local private key, so signing needs
    # a service account to sign as (CLOUDRIFT_GCS_SIGNER_SA) plus signBlob rights on
    # it. Without that this is an environment gap, not a failure — skip cleanly. A
    # service-account key file, by contrast, signs locally and needs no signer.
    if not signer_sa:
        pytest.skip(
            "presigned_url needs CLOUDRIFT_GCS_SIGNER_SA (+ signBlob) under ADC, "
            "or a service-account key file for local signing"
        )
    payload = b"signed-url payload"
    await storage.upload(key, payload, content_type="application/octet-stream")
    try:
        url = await storage.presigned_url(key, expires_in=300)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url)
        assert response.status_code == 200, response.text[:500]
        assert response.content == payload
    finally:
        await storage.delete(key)


async def test_storage_client_shares_one_pool_across_buckets(gcs_bucket, run_id):
    """The account-scoped client is the documented way to serve many buckets."""
    client = get_storage_client("gcs")
    try:
        view = client.bucket(gcs_bucket)
        other = client.bucket(gcs_bucket)
        assert view._storage is other._storage
        k = f"{RESOURCE_PREFIX}/{run_id}/shared-pool.txt"
        await view.upload(k, b"v")
        assert await other.download(k) == b"v"
        await view.delete(k)
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Secrets — Secret Manager
# ---------------------------------------------------------------------------


@pytest.fixture
async def secrets(project):
    backend = get_secrets("gcp_secret_manager", project=project)
    yield backend
    await backend.close()


@pytest.fixture
def secret_name(run_id, request) -> str:
    return f"{RESOURCE_PREFIX}-{run_id}-{request.node.name.replace('_', '-')}"[:255]


async def test_secret_create_read_delete(secrets, secret_name):
    await secrets.set_secret(secret_name, "s3cret-value")
    try:
        assert await secrets.get_secret(secret_name) == "s3cret-value"
    finally:
        await secrets.delete_secret(secret_name)


async def test_secret_versions_are_immutable(secrets, secret_name):
    """GCP adds a version rather than overwriting — `latest` moves, old stays.

    This is the one place GCP's semantics differ from AWS/Azure, so it is worth
    proving against the real service rather than a mock.
    """
    await secrets.set_secret(secret_name, "v1")
    try:
        await secrets.set_secret(secret_name, "v2")
        assert await secrets.get_secret(secret_name) == "v2"
        assert await secrets.get_secret(secret_name, version="1") == "v1"
        assert await secrets.get_secret(secret_name, version="2") == "v2"
    finally:
        await secrets.delete_secret(secret_name)


async def test_secret_json(secrets, secret_name):
    await secrets.set_secret(secret_name, json.dumps({"user": "admin", "port": 5432}))
    try:
        assert await secrets.get_secret_json(secret_name) == {"user": "admin", "port": 5432}
    finally:
        await secrets.delete_secret(secret_name)


async def test_secret_missing_raises_translated_error(secrets, run_id):
    with pytest.raises(SecretNotFoundError):
        await secrets.get_secret(f"{RESOURCE_PREFIX}-{run_id}-absent")


async def test_secret_list_prefix_is_anchored(secrets, run_id):
    """cloudrift filters client-side because GCP's filter is a substring match.

    Creating a decoy whose name *contains* but does not *start with* the prefix is
    what distinguishes the two behaviors.
    """
    stem = f"{RESOURCE_PREFIX}-{run_id}"
    wanted = f"{stem}-prod-db"
    decoy = f"{stem}-x-prod-db"
    await secrets.set_secret(wanted, "a")
    await secrets.set_secret(decoy, "b")
    try:
        names = await secrets.list_secrets(prefix=f"{stem}-prod")
        assert wanted in names
        assert decoy not in names
    finally:
        await asyncio.gather(
            secrets.delete_secret(wanted),
            secrets.delete_secret(decoy),
            return_exceptions=True,
        )


async def test_secrets_health_check(secrets):
    assert await secrets.health_check() is True


# ---------------------------------------------------------------------------
# Crypto — Cloud KMS
# ---------------------------------------------------------------------------


@pytest.fixture
async def crypto(kms_key):
    backend = get_crypto("gcp_kms", key_id=kms_key)
    yield backend
    await backend.close()


async def test_kms_round_trip(crypto):
    plaintext = b"envelope me \x00\x01\xfe"
    ciphertext = await crypto.encrypt(plaintext)
    assert ciphertext != plaintext
    assert await crypto.decrypt(ciphertext) == plaintext


async def test_kms_string_helpers_round_trip(crypto):
    assert await crypto.decrypt_str(await crypto.encrypt_str("hello")) == "hello"


async def test_kms_aad_must_match(kms_key):
    """AAD is bound into the ciphertext; decrypting without it must fail.

    Proves the additional_authenticated_data plumbing reaches both directions —
    a mock can only show the field was passed, not that it was enforced.
    """
    bound = get_crypto("gcp_kms", key_id=kms_key, additional_authenticated_data=b"tenant-42")
    unbound = get_crypto("gcp_kms", key_id=kms_key)
    try:
        ciphertext = await bound.encrypt(b"secret")
        assert await bound.decrypt(ciphertext) == b"secret"
        with pytest.raises(CryptoError):
            await unbound.decrypt(ciphertext)
    finally:
        await asyncio.gather(bound.close(), unbound.close())


async def test_kms_ciphertext_is_native_format(kms_key, project):
    """cloudrift must not re-wrap ciphertext: the native SDK has to decrypt it."""
    from google.cloud.kms import KeyManagementServiceAsyncClient

    backend = get_crypto("gcp_kms", key_id=kms_key)
    try:
        ciphertext = await backend.encrypt(b"interop check")
    finally:
        await backend.close()

    native = KeyManagementServiceAsyncClient()
    try:
        response = await native.decrypt(request={"name": kms_key, "ciphertext": ciphertext})
        assert response.plaintext == b"interop check"
    finally:
        await native.transport.close()


# ---------------------------------------------------------------------------
# Pub/Sub — messaging (queue semantics)
# ---------------------------------------------------------------------------


@pytest.fixture
async def pubsub_resources(project, run_id, request):
    """Create a topic + subscription for one test, and tear both down after."""
    from google.pubsub_v1.services.publisher import PublisherAsyncClient
    from google.pubsub_v1.services.subscriber import SubscriberAsyncClient

    # request.node.name includes the parametrize id in brackets for parametrized
    # tests (e.g. "...round_trips[pubsub_resources0]"). Pub/Sub resource names allow
    # only [A-Za-z0-9-._~+%], so strip everything else — otherwise the bracketed
    # suffix yields INVALID_ARGUMENT at create_topic.
    node = re.sub(r"[^A-Za-z0-9-]", "-", request.node.name)
    suffix = f"{RESOURCE_PREFIX}-{run_id}-{node}"[:250]
    topic_path = f"projects/{project}/topics/{suffix}"
    sub_path = f"projects/{project}/subscriptions/{suffix}"
    ordered = getattr(request, "param", {}).get("ordered", False)

    publisher = PublisherAsyncClient()
    subscriber = SubscriberAsyncClient()
    await publisher.create_topic(name=topic_path)
    await subscriber.create_subscription(
        request={
            "name": sub_path,
            "topic": topic_path,
            "ack_deadline_seconds": 30,
            "enable_message_ordering": ordered,
        }
    )
    try:
        yield {"topic": suffix, "subscription": suffix}
    finally:
        await subscriber.delete_subscription(subscription=sub_path)
        await publisher.delete_topic(topic=topic_path)
        await subscriber.transport.close()
        await publisher.transport.close()


@pytest.fixture
async def queue(project, pubsub_resources):
    backend = get_queue(
        "gcp_pubsub",
        project=project,
        topic=pubsub_resources["topic"],
        subscription=pubsub_resources["subscription"],
    )
    yield backend
    await backend.close()


async def _receive_until(queue, count, attempts=15, **kwargs):
    """Pull until `count` messages arrive. Pub/Sub delivery is not instant and a
    single pull may legitimately return nothing, so polling is required."""
    collected = []
    for _ in range(attempts):
        collected += await queue.receive(max_messages=count, wait_time=5, **kwargs)
        if len(collected) >= count:
            break
    return collected


async def test_queue_send_receive_ack(queue):
    body = {"action": "process", "id": 42, "nested": {"ok": True}}
    msg_id = await queue.send(body, attributes={"trace": "abc-123"})
    assert msg_id

    messages = await _receive_until(queue, 1)
    assert len(messages) == 1
    message = messages[0]
    # data goes out as a dict and comes back as one — the round trip the
    # dict-primitive contract promises.
    assert message.data == body
    assert message.attributes["trace"] == "abc-123"
    assert isinstance(message.body, bytes)
    await queue.delete(message.receipt_handle)


async def test_queue_send_batch(queue):
    ids = await queue.send_batch(
        [OutgoingMessage(body={"n": i}, attributes={"seq": str(i)}) for i in range(5)]
    )
    assert len(ids) == 5
    messages = await _receive_until(queue, 5)
    assert {m.data["n"] for m in messages} == {0, 1, 2, 3, 4}
    for m in messages:
        await queue.delete(m.receipt_handle)


async def test_queue_nack_redelivers(queue):
    await queue.send({"retry": "me"})
    first = await _receive_until(queue, 1)
    assert first
    await queue.nack(first[0].receipt_handle)

    second = await _receive_until(queue, 1)
    assert second, "nack should make the message immediately available again"
    assert second[0].data == {"retry": "me"}
    await queue.delete(second[0].receipt_handle)


async def test_queue_health_check(queue):
    assert await queue.health_check() is True


async def test_queue_depth_raises_not_implemented(queue):
    """Documented gap: backlog is a Cloud Monitoring metric, not a data-plane call."""
    with pytest.raises(NotImplementedError):
        await queue.get_queue_depth()


@pytest.mark.parametrize("pubsub_resources", [{"ordered": True}], indirect=True)
async def test_queue_ordering_key_round_trips(queue):
    """group_id maps to Pub/Sub's ordering_key and must survive the round trip."""
    await queue.send({"step": 1}, group_id="tenant-1")
    messages = await _receive_until(queue, 1)
    assert messages
    assert messages[0].group_id == "tenant-1"
    await queue.delete(messages[0].receipt_handle)


async def test_queue_purge_via_seek(queue):
    await queue.send_batch([OutgoingMessage(body={"n": i}) for i in range(3)])
    # Let the backlog register before seeking past it.
    await asyncio.sleep(5)
    await queue.purge()
    leftover = await queue.receive(max_messages=10, wait_time=5)
    assert leftover == [], "seek(time=now) should have discarded the backlog"


async def test_queue_dead_letter_emulation(project, run_id, pubsub_resources):
    """publish-to-DLQ then ack, with the reason attached as an attribute."""
    from google.pubsub_v1.services.publisher import PublisherAsyncClient
    from google.pubsub_v1.services.subscriber import SubscriberAsyncClient

    dlq = f"{RESOURCE_PREFIX}-{run_id}-dlq"
    dlq_topic = f"projects/{project}/topics/{dlq}"
    dlq_sub_path = f"projects/{project}/subscriptions/{dlq}"
    publisher = PublisherAsyncClient()
    subscriber = SubscriberAsyncClient()
    await publisher.create_topic(name=dlq_topic)
    await subscriber.create_subscription(request={"name": dlq_sub_path, "topic": dlq_topic})

    backend = get_queue(
        "gcp_pubsub",
        project=project,
        topic=pubsub_resources["topic"],
        subscription=pubsub_resources["subscription"],
        dead_letter_topic=dlq,
    )
    try:
        await backend.send({"poison": True})
        messages = await _receive_until(backend, 1)
        assert messages
        await backend.dead_letter(messages[0].receipt_handle, "poison payload")

        # The original must now be in the DLQ, carrying the reason.
        for _ in range(15):
            pulled = await subscriber.pull(subscription=dlq_sub_path, max_messages=1, timeout=5.0)
            if pulled.received_messages:
                break
        assert pulled.received_messages, "message never arrived in the dead-letter topic"
        received = pulled.received_messages[0]
        assert json.loads(received.message.data) == {"poison": True}
        assert received.message.attributes["DeadLetterReason"] == "poison payload"
        await subscriber.acknowledge(subscription=dlq_sub_path, ack_ids=[received.ack_id])
    finally:
        await backend.close()
        await subscriber.delete_subscription(subscription=dlq_sub_path)
        await publisher.delete_topic(topic=dlq_topic)
        await subscriber.transport.close()
        await publisher.transport.close()


# ---------------------------------------------------------------------------
# Pub/Sub — fan-out category
# ---------------------------------------------------------------------------


async def test_pubsub_publish_and_batch(project, pubsub_resources):
    backend = get_pubsub("gcp_pubsub", project=project)
    try:
        msg_id = await backend.publish(
            pubsub_resources["topic"], "hello", attributes={"version": 2}
        )
        assert msg_id
        # One request for 25 messages — no 10-message chunking as on SNS.
        ids = await backend.publish_batch(
            pubsub_resources["topic"], [{"message": str(i)} for i in range(25)]
        )
        assert len(ids) == 25
        assert await backend.health_check() is True
    finally:
        await backend.close()


# ---------------------------------------------------------------------------
# Document DB — Firestore with MongoDB compatibility
# ---------------------------------------------------------------------------


def _firestore_access_token() -> str:
    """Mint an OAuth 2.0 access token from ADC for the Firestore access-token path.

    connect_oidc is the production auth path, but ENVIRONMENT:gcp OIDC fetches its
    token from the GCE metadata server, which does not exist off-GCP — so it cannot
    run from a laptop or non-GKE CI. The access-token path is the laptop-viable
    equivalent: it exercises the same URI construction and the same live endpoint,
    differing only in how the bearer token is obtained.
    """
    import google.auth
    import google.auth.transport.requests

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


@pytest.fixture
async def firestore(firestore_config):
    client = get_mongodb("firestore", access_token=_firestore_access_token(), **firestore_config)
    yield client
    client.close()


def _firestore_db(client, firestore_config):
    """Return the database handle.

    Firestore with MongoDB compatibility maps the Mongo database to the Firestore
    DATABASE_ID: only ``client[<database_id>]`` is valid, and any other name is
    rejected with "Invalid database name". This is a caller constraint, not a
    cloudrift one — cloudrift returns a raw Motor client by design.
    """
    return client[firestore_config["database"]]


async def test_firestore_crud(firestore, firestore_config, run_id):
    """The real proof that the mandatory URI options are right.

    A wrong loadBalanced/tls/retryWrites combination does not fail at
    construction — it fails here, as a server-selection timeout.
    """
    collection = _firestore_db(firestore, firestore_config)[f"docs_{run_id}"]
    doc_id = str(uuid.uuid4())
    await collection.insert_one({"_id": doc_id, "name": "Alice", "age": 30})
    try:
        found = await collection.find_one({"_id": doc_id})
        assert found["name"] == "Alice"

        await collection.update_one({"_id": doc_id}, {"$set": {"age": 31}})
        assert (await collection.find_one({"_id": doc_id}))["age"] == 31

        assert await collection.count_documents({"_id": doc_id}) == 1
    finally:
        await collection.delete_many({"_id": doc_id})


async def test_firestore_query_and_iterate(firestore, firestore_config, run_id):
    collection = _firestore_db(firestore, firestore_config)[f"query_{run_id}"]
    docs = [{"_id": f"{run_id}-{i}", "n": i} for i in range(5)]
    await collection.insert_many(docs)
    try:
        seen = [d["n"] async for d in collection.find({"n": {"$gte": 2}})]
        assert sorted(seen) == [2, 3, 4]
    finally:
        await collection.delete_many({"_id": {"$in": [d["_id"] for d in docs]}})


def test_firestore_sync_client(firestore_config, run_id):
    """The sync twin must reach the same database with the same options."""
    client = get_mongodb_sync(
        "firestore", access_token=_firestore_access_token(), **firestore_config
    )
    try:
        collection = client[firestore_config["database"]][f"sync_{run_id}"]
        doc_id = str(uuid.uuid4())
        collection.insert_one({"_id": doc_id, "v": 1})
        try:
            assert collection.find_one({"_id": doc_id})["v"] == 1
        finally:
            collection.delete_many({"_id": doc_id})
    finally:
        client.close()


def test_firestore_required_options_reached_the_driver(firestore):
    assert firestore.options.load_balanced is True
    assert firestore.options.retry_writes is False


# ---------------------------------------------------------------------------
# Cache — Memorystore (requires running inside the VPC)
# ---------------------------------------------------------------------------


@pytest.fixture
async def memorystore(memorystore_host):
    import os

    auth_string = os.environ.get("CLOUDRIFT_MEMORYSTORE_AUTH")
    ca_cert = os.environ.get("CLOUDRIFT_MEMORYSTORE_CA")
    kwargs = {"host": memorystore_host}
    if auth_string:
        kwargs["auth_string"] = auth_string
    if ca_cert:
        kwargs["ssl_ca_certs"] = ca_cert
        kwargs["ssl"] = True
    backend = get_cache("memorystore", "from_auth_string", **kwargs)
    yield backend
    await backend.close()


async def test_memorystore_round_trip(memorystore, run_id):
    key = f"{RESOURCE_PREFIX}:{run_id}:k"
    await memorystore.set(key, b"value", ttl=60)
    try:
        assert await memorystore.get(key) == b"value"
        assert await memorystore.ttl(key) > 0
        assert await memorystore.ping() is True
    finally:
        await memorystore.delete(key)


async def test_memorystore_pipeline_is_transactional(memorystore, run_id):
    key = f"{RESOURCE_PREFIX}:{run_id}:set"
    try:
        async with memorystore.pipeline() as pipe:
            pipe.sadd(key, "a", "b")
            pipe.expire(key, 60)
        assert await memorystore.scard(key) == 2
    finally:
        await memorystore.delete(key)


async def test_memorystore_error_translation(memorystore, run_id):
    """A wrong-type operation must surface as CacheError, not RedisError."""
    key = f"{RESOURCE_PREFIX}:{run_id}:str"
    await memorystore.set(key, b"not-a-set")
    try:
        with pytest.raises(CacheError):
            await memorystore.sadd(key, "member")
    finally:
        await memorystore.delete(key)
