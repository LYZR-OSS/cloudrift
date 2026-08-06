# cloudrift

Cloud-agnostic abstraction for **storage**, **messaging**, **document databases**, **cache**, **secrets**, **SQL**, **crypto (KMS)**, **pub/sub**, and **email** — built for Lyzr microservices.

- **Async-first.** Every public method is `async def`, backed by native-async SDK clients (`aioboto3`, `azure.*.aio`, `gcloud-aio-storage`, the Google async GAPIC clients, `motor`, `redis.asyncio`, `aiosmtplib`) — no thread-pool wrapping.
- **Drop-in providers.** Same interface across AWS, Azure, GCP, and self-hosted backends. Swap `s3` ↔ `azure_blob` ↔ `gcs` (or `sqs` ↔ `azure_bus` ↔ `gcp_pubsub`, `documentdb` ↔ `cosmos` ↔ `firestore`, `redis` ↔ `elasticache` ↔ `azure_redis` ↔ `memorystore`, `ses` ↔ `azure_acs` ↔ `smtp`) by changing one string.
- **Multiple auth methods per provider.** Static keys, IAM roles, profiles, managed identity, service principals, SAS tokens, mTLS, Workload Identity, ADC, IAM auth — pick what your microservice already has.

| Category | AWS | Azure | GCP | Self-hosted |
|---|---|---|---|---|
| Storage | S3 | Blob Storage | Cloud Storage | — |
| Messaging | SQS | Service Bus | Pub/Sub | — |
| Document DB | DocumentDB | Cosmos DB (MongoDB API) | Firestore (MongoDB compatibility) | — |
| Cache | ElastiCache | Azure Cache for Redis | Memorystore | Redis |
| Secrets | Secrets Manager | Key Vault | Secret Manager | env / file / memory |
| SQL | RDS/Aurora IAM, Redshift | Azure SQL (Entra) | Cloud SQL / AlloyDB IAM | Postgres, MySQL, Oracle |
| Crypto (KMS) | KMS | Key Vault keys | Cloud KMS | — |
| Pub/Sub | SNS | Event Grid | Pub/Sub | — |
| Email | SES | Communication Services | — (see note) | SMTP |

> **GCP email:** Google Cloud has no transactional email service (no SES/ACS
> equivalent — the App Engine Mail API is legacy). On GCP, use the `smtp`
> provider pointed at SendGrid, Mailgun, or Postmark. That is the intended path,
> not a missing feature.

---

## Install

Pick the extras your service needs:

```bash
pip install "cloudrift[aws]"          # S3 + SQS + DocumentDB + SES + Redis client
pip install "cloudrift[azure]"        # Blob + Service Bus + Cosmos + ACS Email + Redis client
pip install "cloudrift[gcp]"          # GCS + Pub/Sub + Firestore + Secret Manager + KMS + Redis client
pip install "cloudrift[cache]"        # Just Redis (any flavour)
pip install "cloudrift[email]"        # Just raw SMTP (aiosmtplib)
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

### Azure authentication

Every Azure backend resolves its identity through the same chain, so one code
path works in all three environments a service runs in:

```
Workload Identity  →  Managed Identity  →  Azure CLI
   (AKS)              (App Service /        (local dev,
                       Container Apps /      after `az login`)
                       VM)
```

That means `get_storage("azure_blob", account_url=...)` with no credentials works
unchanged on AKS, on App Service, and on your laptop. Pass `client_id=` to select a
*user-assigned* managed identity; omit it for the system-assigned one.

Developer-machine credential sources are deliberately excluded: ambient
`AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` env vars (which would silently shadow the
workload's real identity — the Azure counterpart of SQS's `exclude_env_credentials`),
plus the shared token cache, VS Code, PowerShell, and `azd`.

To override — for example to lock a production service down to managed identity only:

```python
storage = get_storage("azure_blob", account_url="...", container="c",
                      credential_options={"exclude_cli_credential": True})
```

`credential_options` is accepted by every Azure `from_managed_identity` constructor and
is forwarded verbatim to `DefaultAzureCredential`. The chain itself is defined once in
`cloudrift/core/azure_credentials.py`.

> On a developer machine this means code intended to exercise managed identity will
> instead succeed using your `az login` identity rather than failing. Pass
> `credential_options={"exclude_cli_credential": True}` when you need to test the
> production path.

### GCP authentication

Every GCP backend resolves its identity through Application Default Credentials,
so one code path works everywhere a service runs:

```
GOOGLE_APPLICATION_CREDENTIALS  →  gcloud SDK ADC file  →  metadata server
   (explicit key file)             (local dev, after       (GKE Workload Identity,
                                    `gcloud auth            Cloud Run, GCE)
                                    application-default
                                    login`)
```

That means `get_storage("gcs", bucket="b")` with no credentials works unchanged on
GKE, on Cloud Run, and on your laptop. To pin an identity explicitly:

```python
storage = get_storage("gcs", bucket="b", service_account_file="/etc/gcp/sa.json")
storage = get_storage("gcs", bucket="b", service_account_info=json.loads(key_json))
```

`service_account_info` takes already-parsed JSON, for the common case where the key
lives in a secret store and should never touch disk.

Note the ordering: ADC checks `GOOGLE_APPLICATION_CREDENTIALS` **first**, so a stray
key path left in the environment silently shadows the workload's real identity. Pass
`prefer_metadata=True` to skip ADC entirely and read the attached service account
straight from the metadata server — the GCP counterpart of Azure's excluded
`environment` credential and SQS's `exclude_env_credentials`:

```python
secrets = get_secrets("gcp_secret_manager", project="my-project", prefer_metadata=True)
```

The chain is defined once in `cloudrift/core/gcp_credentials.py`.

> `prefer_metadata` is **not** accepted by the GCS factories. Cloud Storage has no
> first-party async client, so cloudrift uses `gcloud-aio-storage`, which resolves ADC
> internally with no injection point. Passing the flag there raises `TypeError` rather
> than being silently ignored; pin a GCS identity with `service_account_file` /
> `service_account_info` instead.

---

## Storage

```python
from cloudrift.storage import get_storage

# AWS S3
s3 = get_storage("s3", bucket="b", region="us-east-1")                       # IAM role
s3 = get_storage("s3", bucket="b", aws_access_key_id="...",                  # static keys
                 aws_secret_access_key="...", region="us-east-1")
s3 = get_storage("s3", bucket="b", profile_name="dev")                       # ~/.aws/credentials
s3 = get_storage("s3", bucket="b",                                           # STS AssumeRole
                 role_arn="arn:aws:iam::123456789012:role/cross",
                 external_id="my-external-id", region="us-east-1")

# Azure Blob
blob = get_storage("azure_blob", connection_string="...", container="c")
blob = get_storage("azure_blob", account_url="https://acct.blob.core.windows.net",
                   account_key="...", container="c")
blob = get_storage("azure_blob", account_url="...", sas_token="...", container="c")
blob = get_storage("azure_blob", account_url="...", container="c")           # managed identity
blob = get_storage("azure_blob", account_url="...", container="c",
                   tenant_id="...", client_id="...", client_secret="...")    # service principal

# Google Cloud Storage
gcs = get_storage("gcs", bucket="b")                                         # ADC / Workload Identity
gcs = get_storage("gcs", bucket="b", service_account_file="/etc/gcp/sa.json")
gcs = get_storage("gcs", bucket="b", service_account_info={...})             # key from a secret store
gcs = get_storage("gcs", bucket="b",                                         # sign URLs under
                  signer_service_account_email="svc@p.iam.gserviceaccount.com")  # Workload Identity
```

**Signed URLs on GCS.** V4 signing needs a private key. A service-account key file
has one and signs locally; a Workload Identity or metadata credential does not, and
signs through the IAM `signBlob` API — which must be told *which* service account to
sign as. Pass `signer_service_account_email` and grant the caller
`roles/iam.serviceAccountTokenCreator` on that account. This is the GCS analog of the
Azure Blob backend requiring `account_key`, except GCS can sign with no key at all.

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

# AWS SQS via STS AssumeRole (cross-account)
sqs = get_queue("sqs", queue_url="https://sqs.../q",
                role_arn="arn:aws:iam::123456789012:role/cross-account",
                external_id="my-external-id", region="us-east-1")

# Azure Service Bus
bus = get_queue("azure_bus", connection_string="...", queue_name="my-queue")
bus = get_queue("azure_bus", fully_qualified_namespace="ns.servicebus.windows.net",
                queue_name="my-queue")  # managed identity

# GCP Pub/Sub — a topic to send, a subscription to receive
ps = get_queue("gcp_pubsub", project="my-project",
               topic="jobs", subscription="jobs-worker")
ps = get_queue("gcp_pubsub", project="my-project", topic="jobs")              # send only
ps = get_queue("gcp_pubsub", project="my-project", subscription="jobs-worker")  # receive only
```

**Pub/Sub differences.** Pub/Sub splits what SQS and Service Bus call a queue into a
topic (publish) and a pull subscription (receive), so pass whichever halves your
service uses — calling the other direction without it raises `MessagingError`. Four
operations do not map cleanly and say so rather than differing silently:

| Operation | On Pub/Sub |
|---|---|
| `get_queue_depth()` | `NotImplementedError` — backlog is the Cloud Monitoring metric `subscription/num_undelivered_messages`, not a data-plane call |
| `dedup_id` | `FeatureNotSupportedError` — enable exactly-once delivery on the subscription instead |
| `delay` | `FeatureNotSupportedError` — use Cloud Tasks for delayed delivery |
| `wait_time` | Becomes the pull RPC timeout; Pub/Sub has no long-poll parameter |

`group_id` maps to Pub/Sub's `ordering_key` (enable message ordering on both the
publish and the subscription). `purge()` seeks the subscription to the current time.
`dead_letter()` is emulated as publish-then-ack, like the SQS backend, and needs
`dead_letter_topic=` at construction — Pub/Sub cannot report its configured
dead-letter topic without the Admin API, so cloudrift does not guess.

**Operations** — pass a **dict**; cloudrift serializes it to JSON. Optionally add a
flat `attributes` map (string → string), which maps to SQS `MessageAttributes`
(String type) / Service Bus `application_properties`:

```python
from cloudrift.messaging import OutgoingMessage

msg_id = await queue.send({"action": "process", "id": 42}, attributes={"v": "1"})
ids = await queue.send_batch([
    OutgoingMessage(body={"n": 1}, attributes={"k": "1"}),
    OutgoingMessage(body={"n": 2}),
])

messages = await queue.receive(max_messages=10, wait_time=20)   # long-poll
for m in messages:
    handle_job(m.data)           # the payload dict; m.body is the raw bytes
    print(m.attributes)          # str -> str map
    await queue.delete(m.receipt_handle)   # ack
    # or: await queue.nack(m.receipt_handle)  # return for immediate redelivery

await queue.purge()
await queue.close()
```

`send()` takes a `dict` and nothing else — passing bytes, a string, or a list raises
`TypeError`. Values `json.dumps` can't encode natively (`datetime`, `Decimal`, `UUID`)
are stringified via the backend's `json_default`, which defaults to `str`.

`Message.body` stays **raw bytes** on the receive path so a malformed or non-UTF-8
payload from a foreign producer is still inspectable for dead-letter triage;
`Message.data` is the decoded dict.

> **Azure Service Bus note:** receipt handles are lock tokens — they are only
> valid on the same backend instance that received the message, and only within
> the message lock duration. SQS receipt handles, by contrast, are plain strings
> usable from any client.

### FIFO queues / ordered delivery

For SQS FIFO queues (URL ending in `.fifo`) and session-enabled Service Bus
queues, pass `group_id` (ordering key) and `dedup_id` (deduplication key):

```python
# SQS FIFO — group_id is required, dedup_id optional if the queue has
# content-based deduplication enabled
fifo = get_queue("sqs", queue_url="https://sqs.../jobs.fifo", region="us-east-1")
await fifo.send({"task": "extract"}, group_id="owner-123", dedup_id="evt-abc")

# Azure Service Bus — queue must be created with sessions enabled;
# pass session_enabled=True so the backend uses session receivers
bus = get_queue("azure_bus", connection_string="...", queue_name="jobs",
                session_enabled=True)
await bus.send({"task": "extract"}, group_id="owner-123", dedup_id="evt-abc")

messages = await fifo.receive(max_messages=10, wait_time=20, visibility_timeout=300)
for m in messages:
    print(m.group_id, m.dedup_id, m.receive_count)
    await fifo.delete(m.receipt_handle)            # ack
# Azure only: receive from a specific session
messages = await bus.receive(group_id="owner-123")
```

Semantic differences to be aware of:

| | SQS FIFO | Azure Service Bus (sessions) |
|---|---|---|
| Ordering | Per `MessageGroupId`, groups interleave on receive | Per session; `receive()` without `group_id` drains one session at a time (`NEXT_AVAILABLE_SESSION`) |
| Deduplication | Fixed 5-minute window by `dedup_id` or content hash | By `message_id`, only if the queue enables duplicate detection (window 20s–7d) |
| Per-message `delay` | Not supported — raises `FeatureNotSupportedError` | Supported (scheduled enqueue) |
| `receive(group_id=...)` | Not supported — raises `FeatureNotSupportedError` | Supported |
| `visibility_timeout` on receive | Supported | Ignored (lock duration is queue-level config) |
| `nack()` | `change_message_visibility(0)` — does not bump receive count until redelivery | `abandon_message` — increments `delivery_count` |

---

## Document Database

`get_mongodb(...)` returns a configured [Motor](https://motor.readthedocs.io/)
`AsyncIOMotorClient`. All three providers speak the MongoDB wire protocol — AWS
DocumentDB natively, Azure Cosmos via its MongoDB-API endpoint, and Firestore via
MongoDB compatibility mode — so the caller uses Motor's API directly:

```python
from cloudrift.document import get_mongodb

# AWS DocumentDB (MongoDB-compatible)
client = get_mongodb(
    "documentdb",
    uri="mongodb://user:pass@cluster.docdb.amazonaws.com:27017/?tls=true",
    tls_ca_file="/etc/ssl/rds-ca-bundle.pem",
    max_pool_size=200,
)

# AWS DocumentDB via IAM auth (MONGODB-AWS) — credentials from the AWS chain
# (env vars, ECS task role, EC2 instance profile); requires cloudrift[aws].
client = get_mongodb(
    "documentdb",
    auth="iam",
    host="cluster.docdb.amazonaws.com",
    port=27017,
    tls_ca_file="/etc/ssl/rds-ca-bundle.pem",
)

# Azure Cosmos DB (MongoDB API)
client = get_mongodb("cosmos", connection_string="mongodb://...")
client = get_mongodb("cosmos", account="myacct", account_key="...")

# Firestore with MongoDB compatibility
client = get_mongodb("firestore", uid="f116f93a-519c-...", location="nam5",
                     database="mydb")                      # Google Cloud OIDC — no secret
client = get_mongodb("firestore", uid="...", location="nam5", database="mydb",
                     username="u", password="p")           # SCRAM-SHA-256
client = get_mongodb("firestore", uid="...", location="nam5", database="mydb",
                     access_token="ya29....")              # short-lived OAuth token
client = get_mongodb("firestore",                           # gcloud firestore databases
                     connection_string="mongodb://...firestore.goog:443/...")  # connection-string
```

**Firestore notes.** This is a database created in **MongoDB compatibility mode**, not
classic Firestore — that mode is the only Firestore flavor speaking the Mongo wire
protocol, and therefore the only one that fits this category's contract.

- `uid` is the system-generated UUID in the endpoint hostname, **not** the database ID.
- Three connection options are mandatory and applied for you on every auth path:
  `loadBalanced=true`, `tls=true`, `retryWrites=false` (Firestore has no retryable
  writes). Omitting any one surfaces as an opaque server-selection timeout, so
  cloudrift fills them in — including on a connection string you supply.
- OIDC auth requires `pymongo>=4.7`, which is why the `gcp` extra pins that floor. The
  SCRAM and connection-string paths work on the base `4.6.3`.
- Grant the caller `roles/datastore.user`.

`get_mongodb_sync("firestore", ...)` mirrors all of the above and returns a blocking
`pymongo.MongoClient`.

**Operations** — full Motor / pymongo surface, no wrappers:

```python
db = client["lyzr"]
users = db["users"]

result = await users.insert_one({"name": "Alice", "age": 30})
doc_id = result.inserted_id

doc = await users.find_one({"name": "Alice"})
async for u in users.find({"age": {"$gte": 18}}).skip(0).limit(100):
    ...

await users.update_one({"_id": doc_id}, {"$set": {"age": 31}})
await db["events"].delete_many({"v": 1})
total = await users.count_documents({"age": {"$gte": 18}})

# bulk writes, aggregations, change streams, transactions, GridFS — all
# of Motor is available; nothing is hidden behind a wrapper.

client.close()
```

> **Cosmos auth note.** Cosmos for MongoDB (RU) is keys-only at the wire
> protocol layer — Azure AD tokens are not accepted. Use the connection
> string from the portal or the account name + account key. Earlier
> versions of cloudrift exposed managed-identity / service-principal
> factories for Cosmos that called the SQL API; those have been removed
> in favour of a single Motor-based path.

### Optional sync client

For services that don't run an event loop, `get_mongodb_sync(...)` returns a
blocking [PyMongo](https://pymongo.readthedocs.io/) `MongoClient` — the sync
driver Motor wraps — with identical provider and auth routing:

```python
from cloudrift.document import get_mongodb_sync

client = get_mongodb_sync("documentdb", uri="mongodb://...")
client = get_mongodb_sync("cosmos", account="myacct", account_key="...")
client = get_mongodb_sync(
    "documentdb", auth="iam",
    host="cluster.docdb.amazonaws.com", tls_ca_file="/etc/ssl/rds-ca-bundle.pem")

users = client["lyzr"]["users"]
users.insert_one({"name": "Alice"})
doc = users.find_one({"name": "Alice"})

client.close()
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

# GCP Memorystore (Redis / Valkey)
cache = get_cache("memorystore", "from_auth_string",
                  host="10.0.0.3", auth_string="...")
cache = get_cache("memorystore", "from_server_ca_cert",         # in-transit encryption
                  host="10.0.0.3", ssl_ca_certs="/etc/ssl/memorystore-ca.pem")
cache = get_cache("memorystore", "from_iam_auth", host="10.0.0.3")  # OAuth token + auto-refresh
```

> Memorystore leaves **both** AUTH and in-transit encryption off by default, unlike
> ElastiCache and Azure Cache for Redis — so `from_auth_string` defaults to `ssl=False`
> to match the product. Its server certificate is signed by a per-instance CA that is
> not in the system trust store, which is why `from_server_ca_cert` *requires*
> `ssl_ca_certs` rather than treating it as optional. `from_iam_auth` needs
> `roles/redis.dbConnectionUser`.

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

## Email

```python
from cloudrift.email import get_email

# AWS SES (SESv2)
ses = get_email("ses", region="us-east-1", default_from="noreply@example.com")     # IAM / env
ses = get_email("ses", aws_access_key_id="AKIA...",
                aws_secret_access_key="...", region="us-east-1",
                default_from="noreply@example.com")                                 # static keys
ses = get_email("ses", profile_name="dev", region="us-east-1",
                default_from="noreply@example.com")                                 # ~/.aws profile

# Azure Communication Services
acs = get_email("azure_acs",
                connection_string="endpoint=https://...;accesskey=...",
                default_from="DoNotReply@example.com")                              # connection string
acs = get_email("azure_acs", endpoint="https://x.communication.azure.com",
                default_from="DoNotReply@example.com")                              # managed identity
acs = get_email("azure_acs", endpoint="https://x.communication.azure.com",
                tenant_id="...", client_id="...", client_secret="...",
                default_from="DoNotReply@example.com")                              # service principal

# Raw SMTP (SendGrid, Mailgun, Postmark, Office365, MailHog, ...)
smtp = get_email("smtp", host="smtp.sendgrid.net",
                 username="apikey", password="...",
                 default_from="noreply@example.com")                                # STARTTLS, port 587 (default)
smtp = get_email("smtp", mode="tls", host="smtp.example.com", port=465,
                 username="user", password="pw",
                 default_from="noreply@example.com")                                # implicit TLS
smtp = get_email("smtp", mode="plaintext", host="localhost", port=1025,
                 default_from="noreply@example.test")                               # MailHog / Mailpit (dev)
```

**Operations** — same on every backend:

```python
from cloudrift.email import Attachment, EmailMessage

# Single send (text, HTML, or multipart/alternative)
msg_id: str = await email.send(
    "alice@example.com",
    "Welcome",
    body_text="Plain text body",
    body_html="<p>HTML body</p>",
    cc=["bob@example.com"], bcc=["audit@example.com"],
    reply_to=["support@example.com"],
    attachments=[Attachment(filename="welcome.pdf",
                            content=pdf_bytes,
                            content_type="application/pdf")],
    headers={"X-Campaign": "welcome-v2"},
)

# Batch send (loops `send()` by default; subclasses override when the
# provider exposes a true bulk API)
ids: list[str] = await email.send_batch([
    EmailMessage(to=["alice@example.com"], subject="hi",  body_text="hi"),
    EmailMessage(to=["bob@example.com"],   subject="hi2", body_html="<b>hi2</b>"),
])

ok: bool = await email.health_check()
await email.close()
```

> **Default sender.** Each backend accepts a `default_from` at construction time; calls that omit `from_` fall back to it. SES requires the sender (address or domain) to be verified; ACS requires the sending domain to be linked to the resource.

---

## Secrets, crypto, pub/sub & SQL

```python
from cloudrift.secrets import get_secrets
from cloudrift.crypto import get_crypto
from cloudrift.pubsub import get_pubsub
from cloudrift.sql import get_sql

# Secrets
sec = get_secrets("aws_secrets_manager", region="us-east-1")
sec = get_secrets("azure_keyvault", vault_url="https://myvault.vault.azure.net")
sec = get_secrets("gcp_secret_manager", project="my-project")
sec = get_secrets("env", prefix="SECRET_")                    # non-cloud: env / file / memory

# Crypto (KMS) — encrypt/decrypt small payloads against a managed key
kms = get_crypto("aws_kms", key_id="alias/my-key", region="us-east-1")
kms = get_crypto("azure_keyvault", key_id="https://myvault.vault.azure.net/keys/k")
kms = get_crypto("gcp_kms", key_id="projects/p/locations/us/keyRings/r/cryptoKeys/k")

# Pub/Sub (topic fan-out)
ps = get_pubsub("sns", region="us-east-1")
ps = get_pubsub("azure_eventgrid", endpoint="https://...", access_key="...")
ps = get_pubsub("gcp_pubsub", project="my-project")

# SQL — cloudrift authenticates and hands back a native driver connection
db = get_sql("postgres", "from_credentials", host="db", port=5432,
             user="u", password="p", database="app")
db = get_sql("postgres", "from_iam_auth", host="rds...", port=5432,          # AWS RDS IAM
             user="u", database="app", region="us-east-1")
db = get_sql("postgres", "from_gcp_iam_auth", host="10.20.0.5",              # Cloud SQL / AlloyDB
             user="sa@project.iam", database="app")
db = get_sql("mysql", "from_gcp_iam_auth", host="10.20.0.5",
             user="sa-name", database="app")
conn = await db.connect(timeout=10)
```

**GCP secret semantics.** Secret Manager versions are immutable, so `set_secret()` adds
a new version rather than overwriting (the old one stays readable by version number)
and creates the secret first if it does not exist. Reads resolve the `latest` alias, or
pass `version=` to `get_secret()`. `list_secrets(prefix=...)` filters client-side
because Secret Manager's server-side filter is a substring match, which would also
return secrets that merely *contain* the prefix.

**Cloud KMS notes.** Symmetric keys accept up to **64 KiB** of plaintext — considerably
more headroom than AWS KMS's 4 KB or a Key Vault RSA key's ~190–446 bytes. Unlike AWS,
Cloud KMS needs the key name on **decrypt** as well as encrypt.
`additional_authenticated_data=` is the analog of an AWS KMS encryption context and
must match on both sides.

**Cloud SQL IAM auth.** The credential is built once and cached; its short-lived OAuth
token is used in place of a password and refreshed automatically only once it actually
expires — never rebuilt or re-refreshed on every `connect()` — so no database password
is stored. The database username
differs per engine: PostgreSQL uses the service-account email with the
`.gserviceaccount.com` suffix removed (`sa@project.iam`), MySQL uses only the local
part (`sa-name`, because MySQL caps usernames at 32 characters). cloudrift passes `user`
through untouched — use exactly what the instance's user list shows. Requires
`roles/cloudsql.instanceUser`.

---

## Connection pooling & lifecycle

Every backend holds **one long-lived async client** that is reused across all operations. This is the single biggest perf knob:

- **Don't** call `get_storage(...)` inside a request handler.
- **Do** construct it once at app startup and share it (e.g. `app.state.storage`, FastAPI dependency, or module-level singleton).

Pool sizes are configurable per backend:

```python
get_storage("s3", bucket="b", region="us-east-1",
            max_pool_connections=100, connect_timeout=5.0, read_timeout=30.0)

get_mongodb("documentdb", uri="...",
            max_pool_size=200, min_pool_size=10)
```

Always release sockets on shutdown with `await backend.close()` — or wrap the whole lifetime in `async with`.

---

## Errors

All backends raise from a single hierarchy under `cloudrift.core.exceptions`:

```python
from cloudrift.core.exceptions import (
    ObjectNotFoundError, StoragePermissionError, StorageError,
    QueueNotFoundError, MessageSendError, MessagingError, FeatureNotSupportedError,
    DocumentConnectionError,
    CacheKeyNotFoundError, CacheConnectionError, CacheError,
    EmailError, EmailSendError,
    RecipientRejectedError, SenderUnverifiedError, EmailThrottledError,
)

try:
    await storage.download("missing.txt")
except ObjectNotFoundError:
    ...
```

Provider-specific exceptions (e.g. `botocore.ClientError`, `azure.core.exceptions.HttpResponseError`, `google.api_core.exceptions.*`, `aiohttp.ClientResponseError` from GCS) are translated to the cloudrift hierarchy at the boundary. The document layer is the exception: `get_mongodb(...)` returns a Motor client and any operation errors propagate as native pymongo exceptions (e.g. `pymongo.errors.OperationFailure`, `DuplicateKeyError`). Connect-time failures still surface as `DocumentConnectionError`.

---

## Testing

The dev extra ships moto and fakeredis so unit tests don't need real cloud credentials:

```bash
pip install "cloudrift[dev]"
pytest
```

For local integration testing of the AWS backends, the suite uses `ThreadedMotoServer` (LocalStack-style in-process mock) — see `tests/test_storage.py` for the pattern. Azure backends are tested against Azurite / Service Bus emulators (configure endpoint via the relevant `*_url` kwarg). For DocumentDB and Cosmos (MongoDB API), `tests/test_document.py` covers connection construction; for live integration smoke tests, see `scripts/test_cosmos_*.py`.

GCP has no in-process equivalent of moto, so those backends are tested against mocked
SDK clients (`tests/test_storage_gcs.py`, `tests/test_messaging_gcp.py`, …) — the same
approach as `tests/test_messaging_azure.py`. Two exceptions get stronger coverage:
Memorystore runs against `fakeredis` like the other Redis backends, and every Firestore
URI is parsed with pymongo's own `uri_parser`, so a malformed URI fails in tests rather
than at connect time. Real GCP emulators exist (`fake-gcs-server` honors
`STORAGE_EMULATOR_HOST`, and the Pub/Sub emulator honors `PUBSUB_EMULATOR_HOST`) but
need Docker or a JVM; the GCS factories accept `api_root=` to point at one.

### Live GCP integration suite

`tests/integration/` runs against a **real** GCP project, covering what mocks
structurally cannot: that the credential chain authenticates, that a signed URL is
genuinely fetchable (it is HTTP-GET and byte-compared), that error translation fires on
real API responses, and that Firestore's mandatory URI options are accepted by a live
endpoint. CI excludes it (`--ignore=tests/integration`), and every test **skips** when
its environment variable is unset, so it is a no-op unless deliberately configured.

```bash
./scripts/gcp_integration_setup.sh YOUR_PROJECT_ID us-central1   # provision + print exports
gcloud auth application-default login                            # interactive, one-time
uv run pytest tests/integration -v
./scripts/gcp_integration_teardown.sh YOUR_PROJECT_ID us-central1
```

Storage, secrets, KMS, Pub/Sub (both categories), and Firestore all run from a laptop
for cents. Two do not, and skip by default:

- **Memorystore** has only a private VPC IP, so it is unreachable from outside the VPC —
  it needs a GCE VM, GKE pod, or bastion tunnel, and a minimum instance runs ~$35–50/mo.
- **Cloud SQL IAM** needs an instance plus either authorized networks or the Auth Proxy,
  and the drivers live in the `sql-*` extras rather than `dev`.

> **Cloud KMS is permanent.** Key rings and keys can never be deleted on GCP — only key
> *versions* can be destroyed. The teardown script destroys the versions (stopping the
> ~$0.06/version/month charge) but the ring and key remain in the project forever. This
> is the only resource here that outlives teardown.
