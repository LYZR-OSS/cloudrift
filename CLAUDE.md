# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`lyzr-cloudrift` is a cloud-agnostic abstraction layer for Lyzr microservices, covering nine categories: **storage**, **messaging**, **document DB**, **cache**, **secrets**, **SQL**, **crypto (KMS)**, **pub/sub**, and **email**. Each category exposes the same interface across AWS, Azure, GCP, and (for cache/email) self-hosted backends, so a service swaps providers by changing a single string. Everything is **async-first** — public methods are `async def`, backed by native-async SDK clients (`aioboto3`, `azure.*.aio`, `gcloud-aio-storage`, Google async GAPIC clients, `motor`, `redis.asyncio`, `aiosmtplib`); there is no thread-pool wrapping. The one deliberate exception: the document category also exposes an **optional sync factory** (`get_mongodb_sync`, returning a raw `pymongo.MongoClient`) for services that don't run an event loop.

## Commands

The project uses `uv`. Optional-dependency extras gate which providers install.

```bash
uv sync --extra dev          # install with test/lint tooling (matches CI)
uv run pytest tests/ -v      # run the full test suite
uv run pytest tests/test_cache.py            # single file
uv run pytest tests/test_cache.py::test_set_and_get   # single test
uv run ruff check .          # lint (line-length 100, target py311)
uv run ruff format .         # format
```

Tests run against in-process mocks (`fakeredis`, `moto`/`ThreadedMotoServer`, and recording stand-ins for the Mongo clients), so **no real cloud credentials are ever needed**. `asyncio_mode = "auto"` is set, so `async def test_*` functions need no `@pytest.mark.asyncio` decorator.

GCP has no moto equivalent, so those backends are tested against mocked SDK clients (the `tests/test_messaging_azure.py` pattern), with two exceptions worth keeping: Memorystore runs on `fakeredis` like every other Redis backend, and Firestore URIs are validated by parsing them with pymongo's own `uri_parser` — the component that would actually reject them at connect time. Because no GCP test may touch the network, a test that constructs a backend without patching the credential build will try real ADC and fail in CI; patch `cloudrift.core.gcp_credentials.build_credentials` or inject the client directly.

CI: pushes to `develop` test + publish to TestPyPI; pushes to `main` test + publish to PyPI (`.github/workflows/`).

## Architecture

Eight of the nine categories (all but document) are self-contained packages under `cloudrift/` following an identical three-part shape:

1. **`base.py`** — an `ABC` defining the provider-neutral interface (e.g. `StorageBackend`, `CacheBackend`, `MessagingBackend`). All `@abstractmethod`s are async. Concrete, non-abstract helpers (`__aenter__`/`__aexit__`, `health_check`, default `pipeline`) live here too.
2. **Per-provider modules** — e.g. `s3.py` + `azure_blob.py`, `redis_standalone.py` + `redis_elasticache.py` + `redis_azure.py`. Each subclasses the ABC and is constructed **only** via `from_*` classmethods (`from_iam_role`, `from_access_key`, `from_connection_string`, `from_managed_identity`, etc.) — never a bare `__init__` with credentials.
3. **`__init__.py`** — a `get_*` factory function that selects the provider and routes to the right `from_*` constructor.

Provider SDKs are imported **lazily inside the factory branch**, not at module top level, so a service installing only `cloudrift[aws]` never imports Azure or Google packages. Provider *modules* may import their own SDK at top level (`gcp_secret_manager.py` imports `google.cloud.secretmanager` directly) — the laziness lives in the factory that imports the module.

### Document DB is different — no wrappers

The document category deliberately has **no `base.py` ABC and no backend wrapper classes** (they were removed in the v0.2.0 refactor — don't reintroduce them). All three providers speak the MongoDB wire protocol, so `get_mongodb` returns a raw `motor` `AsyncIOMotorClient` and the caller uses Motor's native API directly. `documentdb.py`, `cosmos.py`, and `firestore.py` contain only plain `connect_*` factory functions (`connect_uri`, `connect_credentials`, `connect_tls_cert`; `connect_connection_string`, `connect_account_key`; `connect_oidc`, `connect_scram`, `connect_access_token`) that build the URI, translate construction failures to `DocumentConnectionError`, and return the client. The sync variant mirrors this exactly: `get_mongodb_sync` returns a raw `pymongo.MongoClient` via identical `connect_*` functions in `documentdb_sync.py`/`cosmos_sync.py`/`firestore_sync.py`. Cosmos here is the **MongoDB API** (keys-only auth — AAD tokens don't work at the wire-protocol layer), not the SQL/Core API.

GCP is **Firestore with MongoDB compatibility**, not classic Firestore — that mode is the only Firestore flavor speaking the Mongo wire protocol, so it is the only one that fits the category's return-type contract. Classic Firestore's document API would need a wrapper, which is exactly what this package refuses to have. Firestore imposes three non-negotiable URI options (`loadBalanced=true`, `tls=true`, `retryWrites=false`); getting any wrong surfaces as an opaque server-selection timeout, so URI construction lives in **`document/_firestore_uri.py`** (shared by the async and sync modules, like `sql/_url.py`) and applies them on every path — including to a caller-supplied connection string, via `ensure_required_params`. Because both factories now have four auth paths, the routing is shared in `_route_firestore` so async and sync cannot diverge. OIDC auth needs `pymongo>=4.7`, which is why the `gcp` extra raises that floor above the base `4.6.3`.

### Two factory-dispatch styles — don't conflate them

- **Cache and SQL** use an explicit auth-method argument: `get_cache(provider, auth_method, **kwargs)` / `get_sql(provider, auth_method, **kwargs)` where `auth_method` is the literal `from_*` method name (e.g. `get_cache("redis", "from_url", url=...)`).
- **All other categories** infer the constructor from **which credential keys are present** in `**kwargs`: `get_storage(provider, **kwargs)` calls `from_access_key` if `aws_access_key_id` is present, `from_connection_string` if `connection_string` is present, and falls through to the managed-identity/IAM-role default. GCP branches follow the same rule: `service_account_info` → `from_service_account_info`, `service_account_file` → `from_service_account_file`, else `from_application_default`. When adding an auth method here, add both the constructor (a `from_*` classmethod, or a `connect_*` function for document) and a routing branch in the factory — for document, in **both** `get_mongodb` and `get_mongodb_sync`.

  AWS `from_assume_role` (STS AssumeRole, cross-account) is wired for **SQS** (`get_queue`) and **S3** (`get_storage` + `get_storage_client`): `role_arn` present → assume-role path, checked **before** `aws_access_key_id`. It does a synchronous `boto3` `sts:AssumeRole` (optional `external_id` → `ExternalId`) and threads the temporary creds into the `aioboto3.Session`. Temp creds are not auto-refreshed — construct a fresh backend when the session expires.

### Azure credentials — one chain, defined once

Every Azure `from_managed_identity` builds its credential through **`cloudrift/core/azure_credentials.py`** (`build_async_credential` / `build_credential`), never by instantiating an `azure.identity` class directly. The chain is `DefaultAzureCredential` with the developer-machine sources excluded, yielding **workload identity → managed identity → az CLI**. Excluding `environment` is the point, not an accident: ambient `AZURE_CLIENT_ID`/`AZURE_CLIENT_SECRET` would otherwise shadow the workload's real identity — the same reasoning as SQS's `exclude_env_credentials`. When adding an Azure backend, call the helper; **do not** reintroduce `ManagedIdentityCredential`.

Each constructor takes `credential_options: dict | None = None`, forwarded verbatim to `DefaultAzureCredential` (overrides win over the defaults). It is deliberately an explicit dict rather than `**kwargs`: with `**kwargs` a misspelled *backend* option (e.g. `session_enable` for `session_enabled`) would be silently swallowed as a credential option instead of raising `TypeError`. Two backends pass it positionally-adjacent to their own `**kwargs` (`crypto`, `sql/mssql`), which is the same reason. `azure-identity` is pinned `>=1.15.0`, which predates `exclude_broker_credential` — so that one is not set by default; callers on newer releases can pass it through `credential_options`.

Credentials are **not** shared between backends: each owns the one it built and closes it in `close()`, so a module-level singleton would let one backend's shutdown break another's.

### GCP credentials — same idea, two surfaces

Every GCP backend builds its credential through **`cloudrift/core/gcp_credentials.py`**, never by calling `google.auth.default()` or instantiating an `oauth2` class directly. `build_credentials(...)` resolves `service_account_info` → `service_account_file` → `prefer_metadata` → ADC, and is what the GAPIC-based backends (Pub/Sub, Secret Manager, Cloud KMS) pass as `credentials=`.

`prefer_metadata=True` is the direct counterpart of Azure's `exclude_environment_credential` and SQS's `exclude_env_credentials`, and exists for the same reason: **ADC checks `GOOGLE_APPLICATION_CREDENTIALS` first**, so a stray key path in the environment silently shadows the workload's real identity. It goes straight to `compute_engine.Credentials()`. It is mutually exclusive with an explicit service account and raises `ValueError` if combined — a test asserts `google.auth.default` is never called on that path, because a regression there is silent.

The awkward part, and it is deliberate: **Cloud Storage is a second auth surface.** There is no first-party async GCS client, so storage uses `gcloud-aio-storage`, whose `gcloud.aio.auth.Token` re-implements the ADC precedence internally and does *not* accept a `google.auth` credential. `build_storage_token_kwargs(...)` serves it by returning `{"service_file": ...}` (a path, or a `StringIO` wrapping in-memory JSON, since `service_file` accepts a file object). `Token` has no injection point for the precedence order, so **`prefer_metadata` cannot be honored for GCS** — the storage factories simply don't accept it, and passing it raises `TypeError` rather than being silently ignored. Don't "fix" this by poking at `Token.service_data` / `token_type`; those are internals.

Two more GCP-specific notes:

- **Sync refresh in an async world.** `google.auth` credential refresh is synchronous. Where a refresh has to happen inside otherwise-async code it is offloaded (`asyncio.to_thread` in `sql/_gcp_iam.py`) or deliberately kept sync because the caller is sync (`_GCPIAMCredentialProvider.get_credentials`, which redis-py invokes synchronously on each reconnect — the same reasoning as the ElastiCache SigV4 and Azure Entra providers).
- **Scopes are not interchangeable.** `cloud-platform` is the default, but Cloud SQL IAM database auth **rejects** it — it requires `https://www.googleapis.com/auth/sqlservice.login` (`CLOUD_SQL_LOGIN_SCOPE` in `sql/_gcp_iam.py`). GCS scopes are chosen by `gcloud-aio-storage` itself, which is why the identity is passed to `Storage(service_file=...)` rather than a hand-built `Token`.

### Messaging payload contract (dict-primitive)

The messaging primitive is a **dict + a flat `attributes` map** (string → string). Unlike every other category, `send`/`send_batch` are **concrete on the ABC**, not abstract: `MessagingBackend.send(payload: dict, attributes=None, delay=0, *, group_id, dedup_id)` serializes through the module-level `to_json(payload, default=...)` — the single serialization point — and calls the backend's `_send_json(body: str, ...)`. `send_batch(messages: list[OutgoingMessage], ...)` where `OutgoingMessage` is `{body: dict, attributes: dict[str,str] | None}` likewise calls `_send_json_batch(items: list[tuple[str, dict|None]], ...)`. **When adding a backend, implement `_send_json`/`_send_json_batch` and never override `send`/`send_batch`** — that is what keeps serialization uniform. `to_json` rejects non-dicts with `TypeError`, so a backend can never see an unserialized payload and a caller can never send raw bytes. The class attribute `json_default` (default `str`) is the `json.dumps` fallback for `datetime`/`Decimal`/`UUID`; override it per backend if fidelity matters.

The hook passes a **`str`, not `bytes`**, because that costs the fewest conversions overall: SQS's `MessageBody` is typed `string` (so bytes would force an `encode`/`decode` round-trip), and `ServiceBusMessage(str)` produces a byte-identical AMQP `DATA` body to `ServiceBusMessage(bytes)`. Pub/Sub *is* the one provider that wants bytes, so `messaging/gcp_pubsub.py` does a single `body.encode("utf-8")` in `_build_message` — one encode on one backend, rather than a decode on the other two. The invariant that matters is unchanged: `to_json` remains the single serialization point.

Attributes map to SQS `MessageAttributes` (String type), Service Bus `application_properties`, and Pub/Sub `attributes` (natively string → string, so no type wrapper). On the **receive** side `Message.body` is still `bytes` — deliberately asymmetric, so a malformed or non-UTF-8 payload from a foreign producer stays inspectable for DLQ triage — with `Message.data` as the `json.loads`'d dict. `Message.attributes` is the stringified provider attributes. Receipt-handle / ack / FIFO semantics are unchanged.

### Pub/Sub serves two categories

Google Pub/Sub is both the SQS analog and the SNS analog, so it is implemented twice: `messaging/gcp_pubsub.py` (topic to send + pull subscription to receive) and `pubsub/gcp_pubsub.py` (publish only). They are separate classes with the same name in different packages — that is intentional, matching how the two categories are separate everywhere else. The messaging backend is the only one in the category that takes **two** resource names, each required only by the direction that uses it; the missing half raises `MessagingError`, so a send-only producer never needs a subscription and never opens a subscriber channel.

Pub/Sub is also where the interface stops mapping cleanly, and each gap fails loudly per the house convention: `get_queue_depth()` raises `NotImplementedError` (backlog is the Cloud Monitoring metric `subscription/num_undelivered_messages`, which would mean a new dependency and `roles/monitoring.viewer`), `dedup_id` and `delay` raise `FeatureNotSupportedError`, `wait_time` becomes the pull RPC timeout (Pub/Sub has no long-poll parameter), and `visibility_timeout` is applied as a post-pull `modifyAckDeadline` because the pull request cannot carry a deadline override. `group_id` → `ordering_key`; `purge()` → `seek(time=now)`; `dead_letter()` is the same publish-then-ack emulation as SQS and requires an explicit `dead_letter_topic=` because Pub/Sub cannot report its configured dead-letter topic without the Admin API.

### Redis cache specifics

The four Redis backends (`redis`, `elasticache`, `azure_redis`, `memorystore`) share **all** their operation logic through `_RedisMixin` in `cache/base.py` — the per-provider modules contain *only* the `from_*` constructors that build the `aioredis.Redis` client with the right auth (URL, IAM SigV4 auto-refresh, access key, managed identity, Memorystore AUTH string / OAuth token). A new Redis command is implemented **once** in `_RedisMixin` and added as an `@abstractmethod` on `CacheBackend`; never reimplement it per provider. `CacheBackend.pipeline()` ships a no-atomicity `_SequentialPipeline` fallback that the Redis mixin overrides with a real server-side transactional pipeline.

Memorystore differs from the other two managed Redis products in its **defaults**, and the constructors deliberately match the product rather than the house style: AUTH and in-transit encryption are both opt-in on Memorystore, so `from_auth_string` defaults to `ssl=False` (ElastiCache and Azure default to TLS on). Its server certificate is signed by a per-instance CA absent from the system trust store, so `from_server_ca_cert` makes `ssl_ca_certs` a **required** positional-ish argument instead of the optional one it is elsewhere — TLS without it always fails verification. Don't "harmonize" these defaults; they encode real product behavior.

### Errors

All backends raise from one hierarchy in `cloudrift/core/exceptions.py`, rooted at `CloudRiftError` with a per-category base (`StorageError`, `CacheError`, `MessagingError`, `DocumentError`, `SecretError`, `SQLError`, `CryptoError`, `PubSubError`, `EmailError`) and specific subclasses. Provider-native exceptions (`botocore.ClientError`, `RedisError`, `azure.core.exceptions.*`, `google.api_core.exceptions.*`, and `aiohttp.ClientResponseError` for GCS) are **caught and translated to this hierarchy at the backend boundary** — callers should only ever see cloudrift exceptions.

Match the *existing* provider's mapping for a category rather than inventing a better one: the GCP Pub/Sub fan-out backend falls back to `PubSubError` (not `PublishError`) on an unclassified publish failure precisely because the SNS backend does, and a caller catching `PublishError` must behave the same on either provider. GCP status mapping is by exception type, not code: `NotFound` → the category's not-found error, `PermissionDenied` → the permission error, and for Cloud KMS `FailedPrecondition` → `CryptoKeyNotFoundError` (a disabled or destroyed key version exists but is unusable — the analog of AWS `KeyUnavailableException`).

### Lifecycle

Backends hold one long-lived, connection-pooled async client meant to be constructed **once at service startup** and reused — never per-request. Always release sockets with `await backend.close()` or `async with backend:` (the ABCs implement the async-context-manager protocol).

## Provider/category matrix

| Category | Factory | AWS | Azure | GCP | Self-hosted |
|---|---|---|---|---|---|
| Storage | `get_storage`, `get_storage_client` | `s3` | `azure_blob` | `gcs` | — |
| Messaging | `get_queue` | `sqs` | `azure_bus` | `gcp_pubsub` | — |
| Document DB | `get_mongodb`, `get_mongodb_sync` | `documentdb` | `cosmos` | `firestore` | — |
| Cache | `get_cache` | `elasticache` | `azure_redis` | `memorystore` | `redis` |
| Secrets | `get_secrets` | `aws_secrets_manager` | `azure_keyvault` | `gcp_secret_manager` | `env`, `file`, `memory` |
| SQL | `get_sql` | `postgres`/`mysql` + `from_iam_auth`, `redshift` | `mssql`/`azuresql` + Entra | `from_gcp_iam_auth` on `postgres`/`mysql` | `oracle`, `databricks` |
| Crypto (KMS) | `get_crypto` | `aws_kms` | `azure_keyvault` | `gcp_kms` | — |
| Pub/Sub | `get_pubsub` | `sns` | `azure_eventgrid` | `gcp_pubsub` | — |
| Email | `get_email` | `ses` | `azure_acs` | — (use `smtp`) | `smtp` |

## Known abstraction gaps

The interface is uniform but not every operation maps cleanly to every provider. When a provider can't honor a method, it raises `NotImplementedError` (or `FeatureNotSupportedError` where the category defines one) rather than silently differing — e.g. Azure Service Bus `delete(receipt_handle)` (Service Bus acks via the receiver's lock token, not a handle), and the Pub/Sub set listed above. Preserve this fail-loud convention; document the gap in the method docstring as the existing code does.

Two gaps are structural rather than per-method:

- **GCP has no transactional email service.** No SES/ACS equivalent exists (the App Engine Mail API is legacy), so there is no `email` backend for GCP and there should not be one — the intended path is the existing `smtp` provider pointed at SendGrid/Mailgun/Postmark. Documented in the README rather than represented as a stub.
- **GCS signed URLs need a signing identity.** V4 signing requires a private key; under Workload Identity there isn't one locally, so signing routes through the IAM `signBlob` API and needs `signer_service_account_email` on the client. Without it, `presigned_url` raises `StorageError` with the fix in the message — the same shape as Azure Blob requiring `account_key`. The `IamClient` is built lazily on first sign, not at construction, because it rejects `gcloud auth application-default login` user credentials and eager construction would break local dev for callers that never sign a URL.
