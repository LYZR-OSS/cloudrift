# cloudrift

Cloud-agnostic abstraction for **storage**, **messaging**, **document databases**, and **cache** — built for Lyzr microservices.

- **Async-first.** Every public method is `async def`. All four categories use native-async SDK clients (`aioboto3`, `azure.*.aio`, `motor`, `redis.asyncio`) — no thread-pool wrapping.
- **Drop-in providers.** Same interface across AWS, Azure, and self-hosted backends. Swap `s3` ↔ `azure_blob` (or `sqs` ↔ `azure_bus`, `documentdb` ↔ `cosmos`, `redis` ↔ `elasticache` ↔ `azure_redis`) by changing one string.
- **Multiple auth methods per provider.** Static keys, IAM roles, profiles, managed identity, service principals, SAS tokens, mTLS, IAM auth — pick what your microservice already has.

| Category | AWS | Azure | Self-hosted |
|---|---|---|---|
| Storage | S3 | Blob Storage | — |
| Messaging | SQS | Service Bus | — |
| Document DB | DocumentDB | Cosmos DB (Core/SQL) | — |
| Cache | ElastiCache | Azure Cache for Redis | Redis |

---

## Install

Pick the extras your service needs:

```bash
pip install "cloudrift[aws]"          # S3 + SQS + DocumentDB + Redis client
pip install "cloudrift[azure]"        # Blob + Service Bus + Cosmos + Redis client
pip install "cloudrift[cache]"        # Just Redis (any flavour)
pip install "cloudrift[all]"          # Everything
```

Python 3.11+.

---

## Quick start

Every backend is constructed via a factory function and held for the lifetime of the service. Reuse one instance per resource — the underlying client is connection-pooled.

```python
from cloudrift.storage import get_storage

# Construct once at startup
storage = get_storage(
    "s3",
    bucket="my-bucket",
    aws_access_key_id="AKIA...",
    aws_secret_access_key="...",
    region="us-east-1",
)

# Use anywhere
await storage.upload("docs/hello.txt", b"hello world", content_type="text/plain")
data = await storage.download("docs/hello.txt")
url = await storage.presigned_url("docs/hello.txt", expires_in=3600)

# Release sockets at shutdown
await storage.close()
```

Or as an async context manager (auto-close):

```python
async with get_storage("s3", bucket="b", region="us-east-1") as storage:
    await storage.upload("k", b"v")
```

---

## Microservice integration

### Configuration via env vars

Pick the provider per environment with a single env var:

```python
import os
from cloudrift.storage import get_storage

storage = get_storage(
    os.environ["STORAGE_PROVIDER"],   # "s3" in prod, "azure_blob" in dev
    **{
        k.lower().removeprefix("storage_"): v
        for k, v in os.environ.items()
        if k.startswith("STORAGE_") and k != "STORAGE_PROVIDER"
    },
)
```

---

## Storage

```python
from cloudrift.storage import get_storage

# AWS S3
s3 = get_storage("s3", bucket="b", region="us-east-1")                       # IAM role
s3 = get_storage("s3", bucket="b", aws_access_key_id="...",                  # static keys
                 aws_secret_access_key="...", region="us-east-1")
s3 = get_storage("s3", bucket="b", profile_name="dev")                       # ~/.aws/credentials

# Azure Blob
blob = get_storage("azure_blob", connection_string="...", container="c")
blob = get_storage("azure_blob", account_url="https://acct.blob.core.windows.net",
                   account_key="...", container="c")
blob = get_storage("azure_blob", account_url="...", sas_token="...", container="c")
blob = get_storage("azure_blob", account_url="...", container="c")           # managed identity
blob = get_storage("azure_blob", account_url="...", container="c",
                   tenant_id="...", client_id="...", client_secret="...")    # service principal
```

**Operations** — same on every backend:

```python
await storage.upload(key, data, content_type="application/json")
data: bytes = await storage.download(key)
await storage.delete(key)
exists: bool = await storage.exists(key)
keys: list[str] = await storage.list(prefix="logs/")
url: str = await storage.presigned_url(key, expires_in=3600)
await storage.close()
```

---

## Messaging

```python
from cloudrift.messaging import get_queue

# AWS SQS
sqs = get_queue("sqs", queue_url="https://sqs.us-east-1.amazonaws.com/.../q",
                region="us-east-1")

# Azure Service Bus
bus = get_queue("azure_bus", connection_string="...", queue_name="my-queue")
bus = get_queue("azure_bus", fully_qualified_namespace="ns.servicebus.windows.net",
                queue_name="my-queue")  # managed identity
```

**Operations**:

```python
msg_id = await queue.send({"action": "process", "id": 42}, delay=0)
ids = await queue.send_batch([{"n": 1}, {"n": 2}])

messages = await queue.receive(max_messages=10, wait_time=20)   # long-poll
for m in messages:
    handle_job(m.body)
    await queue.delete(m.receipt_handle)   # ack (SQS only — see below)

await queue.purge()
await queue.close()
```

> **Azure Service Bus note:** `delete(receipt_handle)` raises `NotImplementedError` because Service Bus completes messages via the receiver's lock token, not by handle. Until the abstraction is reworked, complete messages inside a custom receiver loop using `azure-servicebus` directly, or use the `purge()` helper.

---

## Document Database

```python
from cloudrift.document import get_mongodb

# AWS DocumentDB (MongoDB-compatible)
db = get_mongodb(
    "documentdb",
    uri="mongodb://user:pass@cluster.docdb.amazonaws.com:27017/?tls=true",
    database="lyzr",
    tls_ca_file="/etc/ssl/rds-ca-bundle.pem",
    max_pool_size=200,
)

# Azure Cosmos DB (Core/SQL API)
cdb = get_mongodb("cosmos", connection_string="...", database="lyzr")
cdb = get_mongodb("cosmos", url="https://acct.documents.azure.com:443/",
                      account_key="...", database="lyzr")
```

**Operations** (MongoDB-style on both):

```python
doc_id = await db.insert_one("users", {"name": "Alice", "age": 30})
ids = await db.insert_many("events", [{"v": 1}, {"v": 2}])

doc = await db.find_one("users", {"name": "Alice"})
docs = await db.find("events", {"v": {"$gte": 1}}, limit=100, skip=0)

modified = await db.update_one("users", {"_id": doc_id}, {"$set": {"age": 31}})
deleted = await db.delete_many("events", {"v": 1})
total = await db.count("users", {"age": {"$gte": 18}})

await db.close()
```

---

## Cache

```python
from cloudrift.cache import get_cache

# Self-hosted Redis
cache = get_cache("redis", "from_url", url="redis://localhost:6379/0")
cache = get_cache("redis", "from_credentials",
                  host="redis.internal", port=6379, password="...", db=0)

# AWS ElastiCache
cache = get_cache("elasticache", "from_auth_token",
                  host="my-cluster.cache.amazonaws.com", auth_token="...")
cache = get_cache("elasticache", "from_iam_auth",
                  host="my-cluster.cache.amazonaws.com",
                  username="lyzr-app", region="us-east-1")  # SigV4 + auto-refresh

# Azure Cache for Redis
cache = get_cache("azure_redis", "from_access_key",
                  host="my-cache.redis.cache.windows.net", access_key="...")
cache = get_cache("azure_redis", "from_managed_identity",
                  host="my-cache.redis.cache.windows.net", username="lyzr-app")
```

**Operations** — KV, hash, list, counters:

```python
await cache.set("session:abc", b"data", ttl=3600)
value: bytes | None = await cache.get("session:abc")
await cache.delete("session:abc")

await cache.hset("user:1", "name", "Alice")
fields = await cache.hgetall("user:1")

await cache.lpush("jobs", "job-1", "job-2")
batch = await cache.lrange("jobs", 0, 99)

count = await cache.incr("hits:home")
ok = await cache.ping()
await cache.close()
```

---

## Connection pooling & lifecycle

Every backend holds **one long-lived async client** that is reused across all operations. This is the single biggest perf knob:

- **Don't** call `get_storage(...)` inside a request handler.
- **Do** construct it once at app startup and share it (e.g. `app.state.storage`, FastAPI dependency, or module-level singleton).

Pool sizes are configurable per backend:

```python
get_storage("s3", bucket="b", region="us-east-1",
            max_pool_connections=100, connect_timeout=5.0, read_timeout=30.0)

get_mongodb("documentdb", uri="...", database="db",
                max_pool_size=200, min_pool_size=10)
```

Always release sockets on shutdown with `await backend.close()` — or wrap the whole lifetime in `async with`.

---

## Errors

All backends raise from a single hierarchy under `cloudrift.core.exceptions`:

```python
from cloudrift.core.exceptions import (
    ObjectNotFoundError, StoragePermissionError, StorageError,
    QueueNotFoundError, MessageSendError, MessagingError,
    DocumentNotFoundError, DocumentConnectionError, DocumentError,
    CacheKeyNotFoundError, CacheConnectionError, CacheError,
)

try:
    await storage.download("missing.txt")
except ObjectNotFoundError:
    ...
```

Provider-specific exceptions (e.g. `botocore.ClientError`, `azure.core.exceptions.HttpResponseError`) are translated to the cloudrift hierarchy at the boundary.

---

## Testing

The dev extra ships moto + a Motor mock so unit tests don't need real cloud credentials:

```bash
pip install "cloudrift[dev]"
pytest
```

For local integration testing of the AWS backends, the suite uses `ThreadedMotoServer` (LocalStack-style in-process mock) — see `tests/test_storage.py` for the pattern. Azure backends are tested against Azurite / Service Bus / Cosmos emulators (configure endpoint via the relevant `*_url` kwarg).
