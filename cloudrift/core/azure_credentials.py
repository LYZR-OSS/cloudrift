"""Shared Azure AD credential construction.

Every Azure backend authenticates the same way, so the chain is defined once
here instead of being copy-pasted into each provider module.

The chain that results from the defaults below is::

    Workload Identity  ->  Managed Identity  ->  Azure CLI

which covers the three environments a Lyzr service actually runs in — AKS with
workload identity, App Service / Container Apps / VM with a managed identity,
and a developer machine with ``az login`` — without any per-environment code.

Everything excluded below is a developer-machine credential source that is
either ambiguous or actively harmful in a service:

- ``environment`` — ambient ``AZURE_CLIENT_ID`` / ``AZURE_CLIENT_SECRET`` env
  vars would silently shadow the workload's real identity. This is the Azure
  counterpart of the SQS ``exclude_env_credentials`` option on
  ``AWSSQSBackend.from_iam_role``.
- ``shared_token_cache``, ``visual_studio_code``, ``powershell``,
  ``developer_cli`` — stale or user-scoped caches that must never authenticate
  a production workload.

Azure SDK imports are deliberately lazy so a service installing only
``cloudrift[aws]`` never imports ``azure.identity``.

Note: ``azure-identity`` is pinned ``>=1.15.0``, which predates
``exclude_broker_credential``. It is therefore not set by default; pass it
through ``**overrides`` if you are on a newer release and want it.
"""

_EXCLUDED_BY_DEFAULT = {
    "exclude_environment_credential": True,
    "exclude_shared_token_cache_credential": True,
    "exclude_visual_studio_code_credential": True,
    "exclude_powershell_credential": True,
    "exclude_developer_cli_credential": True,
}


def _credential_options(client_id: str | None, overrides: dict) -> dict:
    """Merge the house defaults, the managed-identity client ID, and caller overrides.

    Overrides are applied last so a caller can always re-enable a source (or
    exclude one that is on by default).
    """
    options = dict(_EXCLUDED_BY_DEFAULT)
    if client_id:
        options["managed_identity_client_id"] = client_id
    options.update(overrides)
    return options


def build_async_credential(client_id: str | None = None, **overrides):
    """Return an async ``DefaultAzureCredential`` for the standard chain.

    Args:
        client_id: Client ID of a *user-assigned* managed identity. Omit to use
            the system-assigned identity.
        **overrides: Passed straight to ``DefaultAzureCredential``, applied after
            the defaults — e.g. ``exclude_cli_credential=True`` to lock a
            production service down to managed identity only.

    The caller owns the returned credential and must ``await credential.close()``.
    """
    from azure.identity.aio import DefaultAzureCredential

    return DefaultAzureCredential(**_credential_options(client_id, overrides))


def build_credential(client_id: str | None = None, **overrides):
    """Return a synchronous ``DefaultAzureCredential`` for the standard chain.

    Sync twin of :func:`build_async_credential`, for the backends whose SDK has
    no async client (Redis/Entra, ACS email, MS SQL token provider).
    """
    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential(**_credential_options(client_id, overrides))
