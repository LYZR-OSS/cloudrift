"""Tests for the Entra workload identity verifier.

No network and no Azure credentials: a local RSA keypair stands in for Entra's
signing key and is injected into the verifier's cache, the same shape as the
other provider tests that mock their SDK client.

The three properties worth protecting, in order of blast radius if one
regressed:

  * a token from a **different tenant** must not verify -- Entra's signing keys
    are shared across tenants, so the signature alone proves nothing about
    which directory issued it;
  * a token for a **different audience** must not verify, or any application in
    the tenant could replay a token it already holds;
  * a **user-delegated** token must not verify, or a human ends up
    authenticating through a path built for workloads.
"""

import time

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jose import jwk, jwt

from cloudrift.core.exceptions import (
    IdentityNotConfiguredError,
    IdentityProviderUnavailableError,
    TokenVerificationError,
)
from cloudrift.identity import get_workload_verifier
from cloudrift.identity.entra import EntraWorkloadVerifier

TENANT = "11111111-1111-1111-1111-111111111111"
OTHER_TENANT = "22222222-2222-2222-2222-222222222222"
AUDIENCE = "api://my-service"
OID = "33333333-3333-3333-3333-333333333333"
APPID = "44444444-4444-4444-4444-444444444444"

V1_ISS = f"https://sts.windows.net/{TENANT}/"
V2_ISS = f"https://login.microsoftonline.com/{TENANT}/v2.0"


def _keypair(kid):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    public_pem = key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    public_jwk = {
        k: (v.decode() if isinstance(v, bytes) else v)
        for k, v in jwk.construct(public_pem, "RS256").to_dict().items()
    }
    public_jwk.update(kid=kid, use="sig")
    return private_pem, public_jwk


PRIVATE, PUBLIC = _keypair("entra-key-1")
OTHER_PRIVATE, _ = _keypair("entra-key-1")


def _token(private_pem=PRIVATE, **overrides):
    """An app-only managed-identity token, v1.0 shaped (the IMDS default)."""
    claims = {
        "iss": V1_ISS,
        "tid": TENANT,
        "aud": AUDIENCE,
        "oid": OID,
        "appid": APPID,
        "sub": OID,
        "exp": int(time.time()) + 300,
        "iat": int(time.time()),
    }
    claims.update(overrides)
    claims = {k: v for k, v in claims.items() if v is not None}
    return jwt.encode(claims, private_pem, algorithm="RS256", headers={"kid": "entra-key-1"})


def _verifier(**kwargs):
    kwargs.setdefault("tenant_id", TENANT)
    kwargs.setdefault("accepted_audiences", [AUDIENCE])
    v = get_workload_verifier("entra", **kwargs)
    v._keys = {"entra-key-1": PUBLIC}
    v._fetched_at = time.monotonic()
    return v


@pytest.fixture
def verifier():
    return _verifier()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def test_factory_returns_an_entra_verifier():
    v = get_workload_verifier("entra", tenant_id=TENANT, accepted_audiences=[AUDIENCE])
    assert isinstance(v, EntraWorkloadVerifier)
    assert v.provider == "entra"


def test_unknown_provider_raises():
    with pytest.raises(ValueError, match="Unsupported workload identity provider"):
        get_workload_verifier("not-a-cloud", tenant_id=TENANT)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

async def test_managed_identity_token_yields_its_principal(verifier):
    principal = await verifier.verify(_token())

    assert principal.provider == "entra"
    assert principal.principal_id == OID
    assert principal.tenant_id == TENANT
    assert principal.client_id == APPID
    assert principal.issuer == V1_ISS


async def test_v2_token_is_accepted_and_reads_azp(verifier):
    principal = await verifier.verify(
        _token(iss=V2_ISS, appid=None, azp=APPID, idtyp="app")
    )
    assert principal.principal_id == OID
    assert principal.client_id == APPID


async def test_audience_may_be_a_list(verifier):
    principal = await verifier.verify(_token(aud=["something-else", AUDIENCE]))
    assert principal.principal_id == OID


async def test_claims_are_available_for_the_caller(verifier):
    principal = await verifier.verify(_token())
    assert principal.claims["tid"] == TENANT


# ---------------------------------------------------------------------------
# Tenant isolation
# ---------------------------------------------------------------------------

async def test_token_from_another_tenant_is_rejected(verifier):
    """The realistic cross-tenant case: foreign issuer and foreign tid.

    Entra signs with keys shared across tenants, so this token's *signature* is
    genuine. It is the issuer pin that refuses it.
    """
    with pytest.raises(TokenVerificationError):
        await verifier.verify(
            _token(tid=OTHER_TENANT, iss=f"https://sts.windows.net/{OTHER_TENANT}/")
        )


async def test_tid_is_checked_independently_of_the_issuer(verifier):
    """Defence in depth: our issuer, someone else's tid, still refused.

    Real Entra would not mint this. The check exists so that a future issuer
    list that is broader than one tenant cannot silently admit another
    directory.
    """
    with pytest.raises(TokenVerificationError, match="different Entra tenant"):
        await verifier.verify(_token(tid=OTHER_TENANT))


async def test_foreign_issuer_is_rejected_even_with_our_tid(verifier):
    with pytest.raises(TokenVerificationError):
        await verifier.verify(_token(iss=f"https://sts.windows.net/{OTHER_TENANT}/"))


# ---------------------------------------------------------------------------
# Audience
# ---------------------------------------------------------------------------

async def test_token_for_another_resource_is_rejected(verifier):
    with pytest.raises(TokenVerificationError, match="not issued for this audience"):
        await verifier.verify(_token(aud="api://some-other-service"))


async def test_graph_token_is_rejected(verifier):
    # The realistic mistake: a workload sends a token it already had.
    with pytest.raises(TokenVerificationError):
        await verifier.verify(_token(aud="https://graph.microsoft.com"))


# ---------------------------------------------------------------------------
# App-only
# ---------------------------------------------------------------------------

async def test_delegated_token_is_rejected(verifier):
    with pytest.raises(TokenVerificationError, match="user-delegated"):
        await verifier.verify(_token(scp="user_impersonation"))


async def test_user_identity_type_is_rejected(verifier):
    with pytest.raises(TokenVerificationError, match="not an application token"):
        await verifier.verify(_token(idtyp="user"))


async def test_token_without_oid_is_rejected(verifier):
    with pytest.raises(TokenVerificationError, match="no object id"):
        await verifier.verify(_token(oid=None))


async def test_app_only_can_be_relaxed_explicitly():
    v = _verifier(require_app_only=False)
    principal = await v.verify(_token(scp="user_impersonation"))
    assert principal.principal_id == OID


# ---------------------------------------------------------------------------
# Signature and expiry
# ---------------------------------------------------------------------------

async def test_forged_signature_is_rejected(verifier):
    with pytest.raises(TokenVerificationError, match="invalid or expired"):
        await verifier.verify(_token(private_pem=OTHER_PRIVATE))


async def test_expired_token_is_rejected(verifier):
    with pytest.raises(TokenVerificationError):
        await verifier.verify(_token(exp=int(time.time()) - 3600))


async def test_malformed_token_is_rejected(verifier):
    with pytest.raises(TokenVerificationError, match="well-formed"):
        await verifier.verify("not-a-jwt")


async def test_unknown_kid_is_rejected_without_a_refetch_storm(verifier, monkeypatch):
    async def _never(self):
        raise AssertionError("rate limit should have suppressed the refetch")

    # Cache is warm and an attempt was just made, so an unknown kid must not
    # trigger another fetch — an unknown kid is what a forgery looks like.
    verifier._last_attempt = time.monotonic()
    monkeypatch.setattr(EntraWorkloadVerifier, "_fetch", _never)

    forged = jwt.encode(
        {"iss": V1_ISS, "tid": TENANT, "aud": AUDIENCE, "oid": OID,
         "exp": int(time.time()) + 300},
        PRIVATE, algorithm="RS256", headers={"kid": "unknown-kid"},
    )
    with pytest.raises(TokenVerificationError, match="unknown key"):
        await verifier.verify(forged)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

async def test_unconfigured_verifier_raises_its_own_error():
    v = EntraWorkloadVerifier(tenant_id=None, accepted_audiences=[])
    with pytest.raises(IdentityNotConfiguredError):
        await v.verify(_token())


def test_tenant_without_audiences_is_not_configured():
    assert not EntraWorkloadVerifier(TENANT, []).configured
    assert EntraWorkloadVerifier(TENANT, [AUDIENCE]).configured


def test_urls_are_derived_from_the_tenant():
    v = EntraWorkloadVerifier(TENANT, [AUDIENCE])
    assert v.jwks_url == f"https://login.microsoftonline.com/{TENANT}/discovery/v2.0/keys"
    assert set(v.expected_issuers) == {V1_ISS, V2_ISS}


def test_sovereign_cloud_overrides_are_honoured():
    v = EntraWorkloadVerifier(
        TENANT, [AUDIENCE],
        jwks_url="https://login.microsoftonline.us/x/discovery/v2.0/keys",
        extra_issuers=["https://login.microsoftonline.us/x/v2.0"],
    )
    assert v.jwks_url == "https://login.microsoftonline.us/x/discovery/v2.0/keys"
    assert "https://login.microsoftonline.us/x/v2.0" in v.expected_issuers


# ---------------------------------------------------------------------------
# Provider outage is not a bad token
# ---------------------------------------------------------------------------

async def test_unreachable_jwks_on_a_cold_cache_is_an_outage(monkeypatch):
    async def _fail(self):
        return None

    monkeypatch.setattr(EntraWorkloadVerifier, "_fetch", _fail)
    v = EntraWorkloadVerifier(TENANT, [AUDIENCE])

    with pytest.raises(IdentityProviderUnavailableError):
        await v.verify(_token())


async def test_cached_keys_survive_a_failed_refetch(verifier, monkeypatch):
    """A brief IdP outage must not fail requests we can already verify."""
    async def _fail(self):
        return None

    monkeypatch.setattr(EntraWorkloadVerifier, "_fetch", _fail)
    verifier._fetched_at = 0.0  # force the cache to look stale

    principal = await verifier.verify(_token())
    assert principal.principal_id == OID


async def test_close_is_safe(verifier):
    await verifier.close()
    async with verifier:
        pass
