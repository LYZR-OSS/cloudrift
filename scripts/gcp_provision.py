#!/usr/bin/env python
"""Provision (or tear down) the GCP resources cloudrift's live tests need.

Uses **Application Default Credentials over the REST APIs**, so it needs no
`gcloud` CLI login — `gcloud auth application-default login` alone is enough.
That is deliberate: the CLI and ADC are separate credential systems, and the
shell variant of this script fails confusingly when only ADC is present.

    uv run python scripts/gcp_provision.py PROJECT_ID [REGION]
    uv run python scripts/gcp_provision.py PROJECT_ID [REGION] --skip-kms
    uv run python scripts/gcp_provision.py PROJECT_ID [REGION] --teardown

Idempotent: an existing resource is reported and reused.

WARNING — Cloud KMS is permanent. Key rings and keys can never be deleted on
GCP; only key *versions* can be destroyed. Teardown destroys the versions
(stopping the ~$0.06/version/month charge) but the ring and key stay in the
project forever. Pass --skip-kms to avoid creating them at all.
"""

from __future__ import annotations

import json
import sys
import time
from urllib.parse import quote

import google.auth
import google.auth.transport.requests
import httpx

PREFIX = "cloudrift-it"
TIMEOUT = 90


def bold(msg: str) -> None:
    print(f"\n\033[1m==> {msg}\033[0m", flush=True)


def ok(msg: str) -> None:
    print(f"  \033[32m✓\033[0m {msg}", flush=True)


def skip(msg: str) -> None:
    print(f"  \033[33m—\033[0m {msg}", flush=True)


def fail(msg: str) -> None:
    print(f"  \033[31m✗\033[0m {msg}", flush=True)


class Gcp:
    """Thin authenticated REST client.

    Sends ``x-goog-user-project`` on every call: user-type ADC (from
    ``gcloud auth application-default login``) has no quota project of its own,
    and several APIs reject the request without one.
    """

    def __init__(self, project: str) -> None:
        self.project = project
        credentials, _ = google.auth.default()
        credentials.refresh(google.auth.transport.requests.Request())
        self._credentials = credentials
        self._client = httpx.Client(timeout=TIMEOUT)

    def _headers(self) -> dict:
        if not self._credentials.valid:
            self._credentials.refresh(google.auth.transport.requests.Request())
        return {
            "Authorization": f"Bearer {self._credentials.token}",
            "x-goog-user-project": self.project,
        }

    def request(self, method: str, url: str, **kwargs) -> httpx.Response:
        return self._client.request(method, url, headers=self._headers(), **kwargs)

    def close(self) -> None:
        self._client.close()

    @staticmethod
    def already_exists(response: httpx.Response) -> bool:
        if response.status_code == 409:
            return True
        if response.status_code == 400 and "already exists" in response.text.lower():
            return True
        return False

    @staticmethod
    def error(response: httpx.Response) -> str:
        try:
            return json.loads(response.text)["error"]["message"]
        except Exception:
            return response.text[:300]

    def delete(self, url: str, label: str) -> bool:
        """DELETE, reporting the real outcome.

        Exists because reporting success unconditionally is worse than useless
        during teardown: it tells you a resource is gone when it is still there
        and still billing.
        """
        response = self.request("DELETE", url)
        if response.status_code in (200, 204):
            ok(f"deleted {label}")
            return True
        if response.status_code == 404:
            skip(f"{label} already absent")
            return True
        fail(f"{label}: {self.error(response)}")
        return False

    def wait_operation(self, url: str, label: str, attempts: int = 60) -> dict | None:
        """Poll a long-running operation until done."""
        for _ in range(attempts):
            response = self.request("GET", url)
            if response.status_code != 200:
                fail(f"{label}: polling failed — {self.error(response)}")
                return None
            body = response.json()
            if body.get("done"):
                if "error" in body:
                    fail(f"{label}: {body['error'].get('message')}")
                    return None
                return body.get("response", {})
            time.sleep(3)
        fail(f"{label}: timed out waiting for the operation")
        return None


# ---------------------------------------------------------------------------
# Provisioning steps
# ---------------------------------------------------------------------------

APIS = [
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudkms.googleapis.com",
    "firestore.googleapis.com",
    "iamcredentials.googleapis.com",
]


def enable_apis(gcp: Gcp) -> None:
    bold("Enabling APIs")
    response = gcp.request(
        "GET",
        f"https://serviceusage.googleapis.com/v1/projects/{gcp.project}/services",
        params={"filter": "state:ENABLED", "pageSize": 200},
    )
    enabled = set()
    if response.status_code == 200:
        enabled = {s["config"]["name"] for s in response.json().get("services", [])}

    for api in APIS:
        if api in enabled:
            skip(f"{api} already enabled")
            continue
        response = gcp.request(
            "POST",
            f"https://serviceusage.googleapis.com/v1/projects/{gcp.project}/services/{api}:enable",
            json={},
        )
        if response.status_code == 200:
            ok(f"enabled {api}")
        else:
            fail(f"{api}: {gcp.error(response)}")


def create_bucket(gcp: Gcp, region: str) -> str:
    bucket = f"{PREFIX}-{gcp.project}"
    bold(f"GCS bucket gs://{bucket}")
    response = gcp.request(
        "POST",
        "https://storage.googleapis.com/storage/v1/b",
        params={"project": gcp.project},
        json={
            "name": bucket,
            "location": region,
            "iamConfiguration": {"uniformBucketLevelAccess": {"enabled": True}},
        },
    )
    if response.status_code == 200:
        ok(f"created (location {region})")
    elif gcp.already_exists(response):
        skip("already exists")
    else:
        fail(gcp.error(response))
    return bucket


def create_pubsub(gcp: Gcp, topic: str, suffix: str = "-sub") -> tuple[str, str]:
    subscription = f"{topic}{suffix}"
    bold(f"Pub/Sub topic '{topic}' + subscription '{subscription}'")
    response = gcp.request(
        "PUT", f"https://pubsub.googleapis.com/v1/projects/{gcp.project}/topics/{topic}", json={}
    )
    if response.status_code == 200:
        ok("topic created")
    elif gcp.already_exists(response):
        skip("topic already exists")
    else:
        fail(gcp.error(response))

    response = gcp.request(
        "PUT",
        f"https://pubsub.googleapis.com/v1/projects/{gcp.project}/subscriptions/{subscription}",
        json={
            "topic": f"projects/{gcp.project}/topics/{topic}",
            "ackDeadlineSeconds": 30,
        },
    )
    if response.status_code == 200:
        ok("subscription created")
    elif gcp.already_exists(response):
        skip("subscription already exists")
    else:
        fail(gcp.error(response))
    return topic, subscription


def create_kms(gcp: Gcp, region: str) -> str | None:
    ring, key = f"{PREFIX}-ring", f"{PREFIX}-key"
    bold(f"Cloud KMS key ring '{ring}' + key '{key}'  \033[33m(PERMANENT)\033[0m")
    base = f"https://cloudkms.googleapis.com/v1/projects/{gcp.project}/locations/{region}"

    response = gcp.request("POST", f"{base}/keyRings", params={"keyRingId": ring}, json={})
    if response.status_code == 200:
        ok("key ring created")
    elif gcp.already_exists(response):
        skip("key ring already exists")
    else:
        fail(gcp.error(response))
        return None

    response = gcp.request(
        "POST",
        f"{base}/keyRings/{ring}/cryptoKeys",
        params={"cryptoKeyId": key},
        json={"purpose": "ENCRYPT_DECRYPT"},
    )
    if response.status_code == 200:
        ok("key created")
    elif gcp.already_exists(response):
        skip("key already exists")
    else:
        fail(gcp.error(response))
        return None

    return f"projects/{gcp.project}/locations/{region}/keyRings/{ring}/cryptoKeys/{key}"


def create_firestore(gcp: Gcp, region: str) -> dict | None:
    database = f"{PREFIX}-mongo"
    bold(f"Firestore database '{database}' (MongoDB compatibility)")
    response = gcp.request(
        "POST",
        f"https://firestore.googleapis.com/v1/projects/{gcp.project}/databases",
        params={"databaseId": database},
        # databaseEdition=ENTERPRISE is what yields a MongoDB wire-protocol
        # endpoint. A STANDARD database does not speak the Mongo protocol and
        # cannot be used through cloudrift's document category.
        json={
            "locationId": region,
            "type": "FIRESTORE_NATIVE",
            "databaseEdition": "ENTERPRISE",
        },
    )
    if response.status_code == 200:
        body = response.json()
        if not body.get("done"):
            ok("creation started, waiting (this takes ~1 minute)")
            operation = body.get("name")
            if operation and not gcp.wait_operation(
                f"https://firestore.googleapis.com/v1/{operation}", "firestore create"
            ):
                return None
        ok("created")
    elif gcp.already_exists(response):
        skip("already exists")
    else:
        fail(gcp.error(response))
        return None

    # `uid` is read-only and only available on the resource — it is the UUID in
    # the connection hostname, and is NOT the database id.
    response = gcp.request(
        "GET",
        f"https://firestore.googleapis.com/v1/projects/{gcp.project}/databases/{database}",
    )
    if response.status_code != 200:
        fail(f"could not read the database back: {gcp.error(response)}")
        return None
    body = response.json()
    uid = body.get("uid")
    if not uid:
        fail("database has no uid — is it really ENTERPRISE edition?")
        return None
    ok(f"uid={uid}  edition={body.get('databaseEdition')}  location={body.get('locationId')}")
    return {"uid": uid, "location": body.get("locationId", region), "database": database}


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------


def teardown(gcp: Gcp, region: str) -> None:
    bucket = f"{PREFIX}-{gcp.project}"

    bold(f"Deleting objects in gs://{bucket}")
    while True:
        response = gcp.request(
            "GET", f"https://storage.googleapis.com/storage/v1/b/{bucket}/o",
            params={"maxResults": 1000},
        )
        if response.status_code != 200:
            skip("bucket not present or unreadable")
            break
        items = response.json().get("items", [])
        if not items:
            break
        for item in items:
            # Object names contain slashes, which must be percent-encoded to stay
            # a single path segment.
            encoded = quote(item["name"], safe="")
            gcp.request(
                "DELETE",
                f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{encoded}",
            )
        ok(f"deleted {len(items)} objects")

    response = gcp.request("DELETE", f"https://storage.googleapis.com/storage/v1/b/{bucket}")
    ok("bucket deleted") if response.status_code in (200, 204) else skip(
        f"bucket not deleted: {gcp.error(response)}"
    )

    bold("Deleting Pub/Sub subscriptions and topics")
    for kind in ("subscriptions", "topics"):
        response = gcp.request(
            "GET", f"https://pubsub.googleapis.com/v1/projects/{gcp.project}/{kind}"
        )
        if response.status_code != 200:
            continue
        for item in response.json().get(kind, []):
            name = item["name"]
            if PREFIX not in name:
                continue
            gcp.delete(f"https://pubsub.googleapis.com/v1/{name}", name.rsplit("/", 1)[-1])

    bold("Deleting secrets")
    response = gcp.request(
        "GET", f"https://secretmanager.googleapis.com/v1/projects/{gcp.project}/secrets",
        params={"pageSize": 200},
    )
    if response.status_code == 200:
        for secret in response.json().get("secrets", []):
            if PREFIX in secret["name"] or "cloudrift-smoke" in secret["name"]:
                gcp.delete(
                    f"https://secretmanager.googleapis.com/v1/{secret['name']}",
                    secret["name"].rsplit("/", 1)[-1],
                )

    bold("Destroying KMS key versions (ring and key are permanent)")
    ring, key = f"{PREFIX}-ring", f"{PREFIX}-key"
    base = (
        f"https://cloudkms.googleapis.com/v1/projects/{gcp.project}/locations/{region}"
        f"/keyRings/{ring}/cryptoKeys/{key}"
    )
    response = gcp.request("GET", f"{base}/cryptoKeyVersions")
    if response.status_code == 200:
        versions = response.json().get("cryptoKeyVersions", [])
        if not versions:
            skip("no key versions present")
        for version in versions:
            short = version["name"].rsplit("/", 1)[-1]
            state = version.get("state")
            if state != "ENABLED":
                skip(f"version {short} already {state}")
                continue
            # Destroying needs roles/cloudkms.admin — Editor can CREATE a key but
            # not destroy its versions, so this legitimately fails for many
            # principals. Report it: a silent failure here leaves the version
            # ENABLED and still billing.
            r = gcp.request(
                "POST", f"https://cloudkms.googleapis.com/v1/{version['name']}:destroy", json={}
            )
            if r.status_code == 200:
                ok(f"version {short} -> {r.json().get('state', 'DESTROY_SCHEDULED')}")
            else:
                fail(f"version {short} NOT destroyed: {gcp.error(r)}")
                fail("  this key version keeps billing (~$0.06/mo) until destroyed")
                fail("  needs roles/cloudkms.admin — ask a project Owner")
    else:
        skip(f"could not list key versions: {gcp.error(response)}")

    bold(f"Deleting Firestore database {PREFIX}-mongo")
    response = gcp.request(
        "DELETE",
        f"https://firestore.googleapis.com/v1/projects/{gcp.project}/databases/{PREFIX}-mongo",
    )
    if response.status_code == 200:
        ok("deletion started")
    else:
        skip(f"not deleted: {gcp.error(response)}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}
    if not args:
        print(__doc__)
        return 2
    project = args[0]
    region = args[1] if len(args) > 1 else "us-central1"

    print(f"\033[1mcloudrift GCP provisioner\033[0m  project={project} region={region}")
    try:
        gcp = Gcp(project)
    except Exception as e:
        print(f"\n\033[31mADC not usable:\033[0m {e}")
        print("Run: gcloud auth application-default login")
        return 2

    try:
        if "--teardown" in flags:
            teardown(gcp, region)
            return 0

        enable_apis(gcp)
        bucket = create_bucket(gcp, region)
        topic, subscription = create_pubsub(gcp, f"{PREFIX}-credit-consumption")
        kms_key = None if "--skip-kms" in flags else create_kms(gcp, region)
        firestore = create_firestore(gcp, region)

        bold("Export these, then run the smoke test")
        lines = [
            "export CLOUD_PLATFORM=gcp",
            f"export GCP_PROJECT={project}",
            "export STORAGE_BACKEND_PROVIDER=gcs",
            f"export STORAGE_FILES_BUCKET={bucket}",
            "export MESSAGING_BACKEND_PROVIDER=gcp_pubsub",
            f"export CREDIT_CONSUMPTION_QUEUE={topic}",
            "export GCP_PUBSUB_SUBSCRIPTION_SUFFIX=-sub",
            "export SECRETS_BACKEND_PROVIDER=gcp_secret_manager",
        ]
        if kms_key:
            lines += ["export CRYPTO_BACKEND_PROVIDER=gcp_kms", f"export GCP_KMS_KEY={kms_key}"]
        if firestore:
            lines += [
                "export DOCUMENT_BACKEND_PROVIDER=firestore",
                f"export FIRESTORE_UID={firestore['uid']}",
                f"export FIRESTORE_LOCATION={firestore['location']}",
                f"export FIRESTORE_DATABASE={firestore['database']}",
            ]
        lines += [
            "# Memorystore is VPC-private; leave the cache on self-hosted redis locally.",
            "export CACHE_BACKEND_PROVIDER=redis",
        ]
        print()
        print("\n".join(lines))
        print(
            "\nThen:\n  uv run python scripts/gcp_service_smoke.py"
            "\n  uv run pytest tests/integration -v"
        )
        return 0
    finally:
        gcp.close()


if __name__ == "__main__":
    sys.exit(main())
