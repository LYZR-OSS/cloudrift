"""Shared Google Cloud credential construction.

Every GCP backend authenticates the same way, so the chain is defined once here
instead of being copy-pasted into each provider module — the GCP counterpart of
:mod:`cloudrift.core.azure_credentials`.

The chain that results from :func:`build_credentials` is Application Default
Credentials (ADC)::

    GOOGLE_APPLICATION_CREDENTIALS  ->  gcloud SDK ADC file  ->  metadata server

which covers the environments a Lyzr service actually runs in — GKE with
Workload Identity, Cloud Run / GCE with an attached service account, and a
developer machine with ``gcloud auth application-default login`` — without any
per-environment code.

``prefer_metadata=True`` collapses that to the metadata server alone. This is
the GCP counterpart of Azure's ``exclude_environment_credential`` and SQS's
``exclude_env_credentials``, and it exists for the same reason: ADC checks
``GOOGLE_APPLICATION_CREDENTIALS`` *first*, so a stray service-account key path
left in the process environment silently shadows the workload's real identity.
On GKE/Cloud Run the attached service account is the identity you want, and it
is the one ADC reaches last.

Two auth surfaces, deliberately
-------------------------------
The GAPIC clients (Pub/Sub, Secret Manager, Cloud KMS) take a
``google.auth.credentials.Credentials``. Cloud Storage has no first-party async
client, so cloudrift uses ``gcloud-aio-storage``, which does its own token
handling via ``gcloud.aio.auth.Token`` and does *not* accept a ``google.auth``
credential. :func:`build_storage_token_kwargs` serves that surface.

``Token`` re-implements the ADC precedence internally with no injection point,
so ``prefer_metadata`` cannot be honored there — the storage factories simply do
not accept it, and passing it raises ``TypeError`` rather than being silently
ignored. To pin a storage identity, pass the service account explicitly.

Credentials are **not** shared between backends: each backend owns the one it
built, so a module-level singleton would let one backend's shutdown break
another's.

GCP SDK imports are deliberately lazy so a service installing only
``cloudrift[aws]`` never imports ``google.auth``.
"""

import io
import json
from typing import Any

#: Scope requested for every service-account credential. GCP authorizes the
#: individual API call against IAM, so the coarse ``cloud-platform`` scope is
#: the documented default for service accounts; per-API scopes exist for user
#: credentials, which cloudrift does not target.
CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def build_credentials(
    service_account_file: str | None = None,
    service_account_info: dict | None = None,
    scopes: list[str] | None = None,
    prefer_metadata: bool = False,
):
    """Return a ``google.auth`` credential for the GAPIC-based backends.

    Args:
        service_account_file: Path to a service-account JSON key file. Takes
            precedence over everything else.
        service_account_info: Parsed service-account JSON, for the common case
            where the key itself lives in a secret store and never touches disk.
        scopes: OAuth scopes. Defaults to ``cloud-platform``.
        prefer_metadata: Skip the ADC lookup and read the attached service
            account straight from the metadata server, so an ambient
            ``GOOGLE_APPLICATION_CREDENTIALS`` cannot shadow the workload's real
            identity. See the module docstring.

    Raises:
        ValueError: if both ``service_account_file`` and ``service_account_info``
            are given, or if ``prefer_metadata`` is combined with either.
    """
    if service_account_file and service_account_info:
        raise ValueError("Pass service_account_file or service_account_info, not both.")
    if prefer_metadata and (service_account_file or service_account_info):
        raise ValueError(
            "prefer_metadata=True reads the attached service account from the "
            "metadata server; it cannot be combined with an explicit service account."
        )

    scopes = scopes or [CLOUD_PLATFORM_SCOPE]

    if service_account_info is not None:
        from google.oauth2 import service_account

        return service_account.Credentials.from_service_account_info(
            service_account_info, scopes=scopes
        )

    if service_account_file is not None:
        from google.oauth2 import service_account

        return service_account.Credentials.from_service_account_file(
            service_account_file, scopes=scopes
        )

    if prefer_metadata:
        from google.auth import compute_engine

        return compute_engine.Credentials(scopes=scopes)

    import google.auth

    credentials, _ = google.auth.default(scopes=scopes)
    return credentials


def build_storage_token_kwargs(
    service_account_file: str | None = None,
    service_account_info: dict | None = None,
) -> dict[str, Any]:
    """Return ``gcloud.aio.storage.Storage`` kwargs selecting the identity.

    ``Storage`` builds its own ``Token`` with the correct storage scopes when
    handed a ``service_file``, so cloudrift passes the identity through rather
    than constructing the ``Token`` itself — that keeps scope selection the
    library's job.

    ``service_account_info`` is wrapped in a ``StringIO``: ``service_file``
    accepts a file object as well as a path, which keeps an in-memory key off
    disk. Returns ``{}`` for the plain ADC case.

    Raises:
        ValueError: if both arguments are given.
    """
    if service_account_file and service_account_info:
        raise ValueError("Pass service_account_file or service_account_info, not both.")
    if service_account_info is not None:
        return {"service_file": io.StringIO(json.dumps(service_account_info))}
    if service_account_file is not None:
        return {"service_file": service_account_file}
    return {}


async def close_credentials(credentials) -> None:
    """Release a credential's transport, if it holds one.

    ``google.auth`` credentials are refreshed over a transport the credential
    does not own, so most have nothing to release — this exists so backends can
    call it unconditionally in ``close()`` and stay symmetric with the Azure
    backends, which always close theirs.
    """
    close = getattr(credentials, "close", None)
    if close is None:
        return
    result = close()
    if result is not None and hasattr(result, "__await__"):
        await result
