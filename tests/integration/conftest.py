"""Shared configuration for the live GCP integration suite.

Every fixture here reads its target from the environment and **skips** rather
than failing when it is unset, so running the suite with nothing configured is a
no-op rather than a wall of errors. The unit suite never touches the network;
this directory always does, which is why CI excludes it
(``pytest --ignore=tests/integration``).

Run it with:

    scripts/gcp_integration_env.sh          # emits the exports to eval
    uv run pytest tests/integration -v

See ``scripts/gcp_integration_setup.sh`` for provisioning the resources.
"""

import os
import uuid

import pytest

#: Prefix for every resource this suite creates, so anything left behind by an
#: interrupted run is obvious and greppable in the console.
RESOURCE_PREFIX = "cloudrift-it"


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} is not set — see scripts/gcp_integration_setup.sh")
    return value


@pytest.fixture(scope="session")
def run_id() -> str:
    """Short unique token so concurrent or repeated runs never collide."""
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="session")
def project() -> str:
    return _require("CLOUDRIFT_GCP_PROJECT")


@pytest.fixture(scope="session")
def gcs_bucket() -> str:
    return _require("CLOUDRIFT_GCS_BUCKET")


@pytest.fixture(scope="session")
def signer_sa() -> str | None:
    """Service account to sign URLs as, when the credential has no private key.

    Optional: unset means the signed-URL tests that need the IAM signBlob path
    skip, while local-key signing (if a key file is configured) still runs.
    """
    return os.environ.get("CLOUDRIFT_GCS_SIGNER_SA")


@pytest.fixture(scope="session")
def kms_key() -> str:
    return _require("CLOUDRIFT_GCP_KMS_KEY")


@pytest.fixture(scope="session")
def firestore_config() -> dict:
    return {
        "uid": _require("CLOUDRIFT_FIRESTORE_UID"),
        "location": _require("CLOUDRIFT_FIRESTORE_LOCATION"),
        "database": _require("CLOUDRIFT_FIRESTORE_DATABASE"),
    }


@pytest.fixture(scope="session")
def memorystore_host() -> str:
    """Memorystore is VPC-private, so this only resolves from inside the VPC.

    Left unset on a laptop, which skips the Memorystore tests.
    """
    return _require("CLOUDRIFT_MEMORYSTORE_HOST")
