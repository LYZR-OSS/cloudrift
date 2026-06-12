# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`lyzr-cloudrift` is a cloud-agnostic abstraction layer for Lyzr microservices, covering six categories: **storage**, **messaging**, **document DB**, **cache**, **secrets**, and **pub/sub**. Each category exposes the same interface across AWS, Azure, and (for cache) self-hosted backends, so a service swaps providers by changing a single string. Everything is **async-first** — public methods are `async def`, backed by native-async SDK clients (`aioboto3`, `azure.*.aio`, `motor`, `redis.asyncio`); there is no thread-pool wrapping. The one deliberate exception: the document category also exposes an **optional sync factory** (`get_mongodb_sync`, returning a raw `pymongo.MongoClient`) for services that don't run an event loop.

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

CI: pushes to `develop` test + publish to TestPyPI; pushes to `main` test + publish to PyPI (`.github/workflows/`).

## Architecture

Five of the six categories (all but document) are self-contained packages under `cloudrift/` following an identical three-part shape:

1. **`base.py`** — an `ABC` defining the provider-neutral interface (e.g. `StorageBackend`, `CacheBackend`, `MessagingBackend`). All `@abstractmethod`s are async. Concrete, non-abstract helpers (`__aenter__`/`__aexit__`, `health_check`, default `pipeline`) live here too.
2. **Per-provider modules** — e.g. `s3.py` + `azure_blob.py`, `redis_standalone.py` + `redis_elasticache.py` + `redis_azure.py`. Each subclasses the ABC and is constructed **only** via `from_*` classmethods (`from_iam_role`, `from_access_key`, `from_connection_string`, `from_managed_identity`, etc.) — never a bare `__init__` with credentials.
3. **`__init__.py`** — a `get_*` factory function that selects the provider and routes to the right `from_*` constructor.

Provider SDKs are imported **lazily inside the factory branch**, not at module top level, so a service installing only `cloudrift[aws]` never imports Azure packages.

### Document DB is different — no wrappers

The document category deliberately has **no `base.py` ABC and no backend wrapper classes** (they were removed in the v0.2.0 refactor — don't reintroduce them). Both providers speak the MongoDB wire protocol, so `get_mongodb` returns a raw `motor` `AsyncIOMotorClient` and the caller uses Motor's native API directly. `documentdb.py` and `cosmos.py` contain only plain `connect_*` factory functions (`connect_uri`, `connect_credentials`, `connect_tls_cert`; `connect_connection_string`, `connect_account_key`) that build the URI, translate construction failures to `DocumentConnectionError`, and return the client. The sync variant mirrors this exactly: `get_mongodb_sync` returns a raw `pymongo.MongoClient` via identical `connect_*` functions in `documentdb_sync.py`/`cosmos_sync.py`. Cosmos here is the **MongoDB API** (keys-only auth — AAD tokens don't work at the wire-protocol layer), not the SQL/Core API.

### Two factory-dispatch styles — don't conflate them

- **Cache** uses an explicit auth-method argument: `get_cache(provider, auth_method, **kwargs)` where `auth_method` is the literal `from_*` method name (e.g. `get_cache("redis", "from_url", url=...)`).
- **All five other categories** infer the constructor from **which credential keys are present** in `**kwargs`: `get_storage(provider, **kwargs)` calls `from_access_key` if `aws_access_key_id` is present, `from_connection_string` if `connection_string` is present, and falls through to the managed-identity/IAM-role default. When adding an auth method here, add both the constructor (a `from_*` classmethod, or a `connect_*` function for document) and a routing branch in the factory — for document, in **both** `get_mongodb` and `get_mongodb_sync`.

### Redis cache specifics

The three Redis backends (`redis`, `elasticache`, `azure_redis`) share **all** their operation logic through `_RedisMixin` in `cache/base.py` — the per-provider modules contain *only* the `from_*` constructors that build the `aioredis.Redis` client with the right auth (URL, IAM SigV4 auto-refresh, access key, managed identity). A new Redis command is implemented **once** in `_RedisMixin` and added as an `@abstractmethod` on `CacheBackend`; never reimplement it per provider. `CacheBackend.pipeline()` ships a no-atomicity `_SequentialPipeline` fallback that the Redis mixin overrides with a real server-side transactional pipeline.

### Errors

All backends raise from one hierarchy in `cloudrift/core/exceptions.py`, rooted at `CloudRiftError` with a per-category base (`StorageError`, `CacheError`, `MessagingError`, `DocumentError`, `SecretError`, `PubSubError`) and specific subclasses. Provider-native exceptions (`botocore.ClientError`, `RedisError`, `azure.core.exceptions.*`) are **caught and translated to this hierarchy at the backend boundary** — callers should only ever see cloudrift exceptions.

### Lifecycle

Backends hold one long-lived, connection-pooled async client meant to be constructed **once at service startup** and reused — never per-request. Always release sockets with `await backend.close()` or `async with backend:` (the ABCs implement the async-context-manager protocol).

## Provider/category matrix

| Category | Factory | AWS | Azure | Self-hosted |
|---|---|---|---|---|
| Storage | `get_storage` | `s3` | `azure_blob` | — |
| Messaging | `get_queue` | `sqs` | `azure_bus` | — |
| Document DB | `get_mongodb` | `documentdb` | `cosmos` | — |
| Cache | `get_cache` | `elasticache` | `azure_redis` | `redis` |
| Secrets | `get_secrets` | `aws_secrets_manager` | `azure_keyvault` | — |
| Pub/Sub | `get_pubsub` | `sns` | `azure_eventgrid` | — |

## Known abstraction gaps

The interface is uniform but not every operation maps cleanly to every provider. When a provider can't honor a method, it raises `NotImplementedError` rather than silently differing — e.g. Azure Service Bus `delete(receipt_handle)` (Service Bus acks via the receiver's lock token, not a handle). Preserve this fail-loud convention; document the gap in the method docstring as the existing code does.
