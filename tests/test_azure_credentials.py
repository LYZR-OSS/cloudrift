"""Tests for the shared Azure AD credential chain.

These assert against the real ``DefaultAzureCredential`` (constructing one makes
no network calls — the chain is only probed when a token is requested), so they
catch an azure-identity release that renames or drops a kwarg we depend on.
"""

from unittest.mock import patch

import pytest

from cloudrift.core.azure_credentials import (
    _EXCLUDED_BY_DEFAULT,
    build_async_credential,
    build_credential,
)


def _chain(credential):
    return [type(c).__name__ for c in credential.credentials]


# ---------------------------------------------------------------------------
# The resulting chain
# ---------------------------------------------------------------------------


def test_sync_chain_is_workload_then_managed_then_cli():
    chain = _chain(build_credential())
    assert "ManagedIdentityCredential" in chain
    assert "AzureCliCredential" in chain
    # developer-machine sources are excluded
    assert "EnvironmentCredential" not in chain
    assert "SharedTokenCacheCredential" not in chain
    assert "VisualStudioCodeCredential" not in chain
    assert "AzurePowerShellCredential" not in chain
    assert "AzureDeveloperCliCredential" not in chain


async def test_async_chain_is_workload_then_managed_then_cli():
    credential = build_async_credential()
    try:
        chain = _chain(credential)
        assert "ManagedIdentityCredential" in chain
        assert "AzureCliCredential" in chain
        assert "EnvironmentCredential" not in chain
        assert "SharedTokenCacheCredential" not in chain
    finally:
        await credential.close()


def test_managed_identity_precedes_cli():
    """Production identity must win over a developer's az login on the same box."""
    chain = _chain(build_credential())
    assert chain.index("ManagedIdentityCredential") < chain.index("AzureCliCredential")


# ---------------------------------------------------------------------------
# client_id / overrides plumbing
# ---------------------------------------------------------------------------


def test_client_id_becomes_managed_identity_client_id():
    with patch("azure.identity.DefaultAzureCredential") as cred_cls:
        build_credential("user-assigned-123")
    assert cred_cls.call_args.kwargs["managed_identity_client_id"] == "user-assigned-123"


def test_no_client_id_omits_the_kwarg_entirely():
    """Passing managed_identity_client_id=None would pin to the system identity oddly."""
    with patch("azure.identity.DefaultAzureCredential") as cred_cls:
        build_credential()
    assert "managed_identity_client_id" not in cred_cls.call_args.kwargs


def test_defaults_are_applied():
    with patch("azure.identity.aio.DefaultAzureCredential") as cred_cls:
        build_async_credential()
    for key, value in _EXCLUDED_BY_DEFAULT.items():
        assert cred_cls.call_args.kwargs[key] is value


def test_overrides_win_over_defaults():
    with patch("azure.identity.DefaultAzureCredential") as cred_cls:
        build_credential(exclude_environment_credential=False)
    assert cred_cls.call_args.kwargs["exclude_environment_credential"] is False


def test_overrides_can_lock_down_to_managed_identity_only():
    chain = _chain(build_credential(exclude_cli_credential=True))
    assert "AzureCliCredential" not in chain
    assert "ManagedIdentityCredential" in chain


def test_builder_does_not_mutate_the_module_defaults():
    build_credential("a", exclude_environment_credential=False)
    assert _EXCLUDED_BY_DEFAULT["exclude_environment_credential"] is True
    assert "managed_identity_client_id" not in _EXCLUDED_BY_DEFAULT


# ---------------------------------------------------------------------------
# Every Azure backend routes through the shared helper
# ---------------------------------------------------------------------------


# (label, provider SDK to skip on, cloudrift module, class, positional args)
ASYNC_FACTORIES = [
    (
        "messaging",
        "azure.servicebus",
        "cloudrift.messaging.azure_bus",
        "AzureServiceBusBackend",
        ("ns.servicebus.windows.net", "q"),
    ),
    (
        "storage",
        "azure.storage.blob",
        "cloudrift.storage.azure_blob",
        "AzureBlobClient",
        ("https://acct.blob.core.windows.net",),
    ),
    (
        "secrets",
        "azure.keyvault.secrets",
        "cloudrift.secrets.azure_keyvault",
        "AzureKeyVaultBackend",
        ("https://v.vault.azure.net",),
    ),
    (
        "pubsub",
        "azure.eventgrid",
        "cloudrift.pubsub.azure_eventgrid",
        "AzureEventGridBackend",
        ("https://t.eventgrid.azure.net",),
    ),
]


@pytest.mark.parametrize(
    "label,sdk,module,cls_name,args", ASYNC_FACTORIES, ids=[f[0] for f in ASYNC_FACTORIES]
)
def test_async_backends_use_the_shared_chain(label, sdk, module, cls_name, args):
    """Each async Azure backend must build its credential through the shared chain."""
    import importlib

    pytest.importorskip(sdk, reason=f"{label} extra not installed")
    backend_cls = getattr(importlib.import_module(module), cls_name)
    with patch("azure.identity.aio.DefaultAzureCredential") as cred_cls:
        backend_cls.from_managed_identity(*args, "mi-client-id")
    kwargs = cred_cls.call_args.kwargs
    assert kwargs["managed_identity_client_id"] == "mi-client-id"
    assert kwargs["exclude_environment_credential"] is True


def test_crypto_backend_uses_the_shared_chain():
    from cloudrift.crypto.azure_keyvault_keys import AzureKeyVaultKeysBackend

    with patch("azure.identity.aio.DefaultAzureCredential") as cred_cls:
        AzureKeyVaultKeysBackend.from_managed_identity(
            "https://v.vault.azure.net/keys/k",
            "mi-client-id",
            credential_options={"exclude_cli_credential": True},
        )
    kwargs = cred_cls.call_args.kwargs
    assert kwargs["managed_identity_client_id"] == "mi-client-id"
    assert kwargs["exclude_cli_credential"] is True
    assert kwargs["exclude_environment_credential"] is True


def test_email_backend_uses_the_shared_sync_chain():
    from cloudrift.email.azure_acs import AzureACSEmailBackend

    with patch("azure.identity.DefaultAzureCredential") as cred_cls:
        AzureACSEmailBackend.from_managed_identity(
            "https://acs.communication.azure.com", client_id="mi-client-id"
        )
    assert cred_cls.call_args.kwargs["managed_identity_client_id"] == "mi-client-id"


def test_mssql_token_provider_uses_the_shared_sync_chain():
    from cloudrift.sql.mssql import MSSQLSQLBackend

    backend = MSSQLSQLBackend.from_entra_managed_identity(
        "srv.database.windows.net", "db", client_id="mi-client-id"
    )
    with patch("azure.identity.DefaultAzureCredential") as cred_cls:
        cred_cls.return_value.get_token.return_value.token = "tok"
        assert backend._token_provider() == "tok"
    assert cred_cls.call_args.kwargs["managed_identity_client_id"] == "mi-client-id"


# ---------------------------------------------------------------------------
# Backend keyword typos must still fail loudly
# ---------------------------------------------------------------------------


def test_backend_kwarg_typo_is_not_swallowed_as_a_credential_option():
    """credential_options is an explicit dict, not **kwargs, precisely so that a
    misspelled backend option raises instead of silently reaching Azure."""
    from cloudrift.messaging.azure_bus import AzureServiceBusBackend

    with pytest.raises(TypeError, match="session_enable"):
        AzureServiceBusBackend.from_managed_identity("ns", "q", session_enable=True)
