"""Provider-neutral verification of cloud-issued workload identity tokens.

The category answers one question: *which cloud-managed workload is calling?*
A service running on AKS, EKS or GKE can obtain a token from its platform
identity with no stored secret; this verifies such a token and reports the
principal behind it. What the caller then does with that principal -- map it to
a tenant, authorize it, issue its own credential -- is application logic and
deliberately stays outside this package.

All three clouds issue these as OIDC JWTs signed with keys published at a JWKS
endpoint, so the fetch/cache/rotate machinery is shared here and each provider
module supplies only its issuer URLs and its claim rules.

Unlike every other cloudrift category, a verifier holds no cloud SDK client --
it is an HTTP client plus a key cache. ``close()`` still exists so the lifecycle
matches the rest of the library.
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from cloudrift.core.exceptions import (
    IdentityNotConfiguredError,
    IdentityProviderUnavailableError,
    TokenVerificationError,
)

logger = logging.getLogger(__name__)

JWKS_CACHE_TTL_SEC = 3600
# An unknown kid triggers a refetch; this is the floor between attempts.
JWKS_MIN_REFETCH_INTERVAL_SEC = 30
JWKS_HTTP_TIMEOUT_SEC = 5
# Clock skew tolerance between this host and the issuer.
LEEWAY_SEC = 30

# Asymmetric only. Never include "none" or an HMAC algorithm: with HMAC an
# attacker can sign a token using the issuer's *public* key, which is published.
ALLOWED_ALGORITHMS = [
    "RS256", "RS384", "RS512",
    "PS256", "PS384", "PS512",
    "ES256", "ES384", "ES512",
]


@dataclass(frozen=True)
class WorkloadPrincipal:
    """The identity behind a verified token.

    ``principal_id`` is the durable id of the workload -- Entra's ``oid``,
    GCP's ``sub``, the role session for AWS -- and is what an application
    should key its own registration on. ``client_id`` is the application id
    where the provider distinguishes the two, and is None where it does not.
    """

    provider: str
    issuer: str
    principal_id: str
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    claims: dict[str, Any] = field(default_factory=dict, repr=False)


class WorkloadIdentityVerifier(ABC):
    """Abstract base for workload identity token verification."""

    @property
    @abstractmethod
    def provider(self) -> str:
        """Short provider name, as passed to ``get_workload_verifier``."""

    @property
    @abstractmethod
    def configured(self) -> bool:
        """True when enough configuration was supplied to verify safely."""

    @abstractmethod
    async def verify(self, token: str) -> WorkloadPrincipal:
        """Verify ``token`` and return the principal, or raise.

        Raises:
            IdentityNotConfiguredError: the verifier was not configured.
            TokenVerificationError: the token is invalid, expired, or does not
                satisfy this provider's rules.
            IdentityProviderUnavailableError: the issuer's signing keys could
                not be fetched. This is an outage on our side, not a bad token,
                and callers should distinguish the two.
        """

    async def close(self) -> None:
        """Release the HTTP client. Default is a no-op."""

    async def __aenter__(self) -> "WorkloadIdentityVerifier":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class OIDCWorkloadVerifier(WorkloadIdentityVerifier):
    """Shared JWKS fetch/cache/verify for OIDC-issuing providers.

    Keys are cached by ``kid`` and refetched when a token presents an unknown
    one, which is how a signing-key rotation heals without a restart. That
    refetch is rate limited on purpose: an unknown ``kid`` is also exactly what
    a forged token looks like, and it must not turn into a fetch storm against
    the identity provider.

    Subclasses supply ``provider``, ``configured``, ``expected_issuers`` and
    ``jwks_url``, and implement ``_principal`` to apply their own claim rules.
    """

    def __init__(self) -> None:
        self._keys: dict[str, dict] = {}
        self._fetched_at = 0.0
        self._last_attempt = 0.0
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Subclass contract
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def expected_issuers(self) -> tuple[str, ...]:
        """Every ``iss`` value a token from this provider may carry.

        A tuple rather than a string because a provider may legitimately use
        more than one -- Entra issues v1.0 and v2.0 tokens from different URLs
        for the same tenant, and which one a workload gets is decided by the
        app registration rather than by the caller.
        """

    @property
    @abstractmethod
    def jwks_url(self) -> str:
        """Where this provider publishes its signing keys."""

    @abstractmethod
    def _principal(self, claims: dict) -> WorkloadPrincipal:
        """Apply provider claim rules and build the principal.

        Raise ``TokenVerificationError`` for anything this provider requires
        that the token does not satisfy (audience, tenant, app-only, ...).
        Signature, expiry and issuer are already checked by the time this runs.
        """

    # ------------------------------------------------------------------
    # Verification
    # ------------------------------------------------------------------

    async def verify(self, token: str) -> WorkloadPrincipal:
        if not self.configured:
            raise IdentityNotConfiguredError(
                f"The {self.provider} workload identity verifier is not configured"
            )

        from jose import jwt
        from jose.exceptions import JWTError

        try:
            kid = jwt.get_unverified_header(token).get("kid")
        except JWTError as exc:
            raise TokenVerificationError("Token is not a well-formed JWT") from exc

        key = await self._key_for(kid)

        try:
            claims = jwt.decode(
                token,
                key,
                algorithms=ALLOWED_ALGORITHMS,
                issuer=list(self.expected_issuers),
                options={
                    # Audience is checked by `_principal`, which knows the
                    # provider's conventions; a plain `aud` check is wrong for
                    # providers that put the relying party elsewhere.
                    "verify_aud": False,
                    "leeway": LEEWAY_SEC,
                },
            )
        except JWTError as exc:
            logger.warning("%s token rejected: %s", self.provider, exc)
            raise TokenVerificationError("Token is invalid or expired") from exc

        return self._principal(claims)

    # ------------------------------------------------------------------
    # Key cache
    # ------------------------------------------------------------------

    def _expired(self, now: Optional[float] = None) -> bool:
        return (now or time.monotonic()) - self._fetched_at > JWKS_CACHE_TTL_SEC

    async def _key_for(self, kid: Optional[str]) -> dict:
        key = self._keys.get(kid) if kid else None
        if key is not None and not self._expired():
            return key

        await self._refresh_for(kid)

        key = self._keys.get(kid) if kid else None
        if key is None:
            logger.warning("No signing key at %s for kid=%s", self.jwks_url, kid)
            raise TokenVerificationError("Token was signed by an unknown key")
        return key

    async def _refresh_for(self, kid: Optional[str]) -> None:
        async with self._lock:
            # Another caller may have refreshed while we waited for the lock.
            if kid and kid in self._keys and not self._expired():
                return

            now = time.monotonic()
            if self._keys and now - self._last_attempt < JWKS_MIN_REFETCH_INTERVAL_SEC:
                # Rate limit only once we have *some* keys: a cold cache must
                # always be allowed to fill, or the first request after boot
                # would fail for no reason.
                return

            self._last_attempt = now
            keys = await self._fetch()
            if keys is None:
                if not self._keys:
                    raise IdentityProviderUnavailableError(
                        f"Could not fetch signing keys from {self.jwks_url}"
                    )
                # Serve the cached keys rather than failing every request while
                # the provider is briefly unreachable.
                return

            self._keys = keys
            self._fetched_at = now

    async def _fetch(self) -> Optional[dict[str, dict]]:
        import httpx

        try:
            async with httpx.AsyncClient(timeout=JWKS_HTTP_TIMEOUT_SEC) as client:
                response = await client.get(self.jwks_url)
            response.raise_for_status()
            payload = response.json()
        except Exception:
            logger.exception("Could not fetch JWKS from %s", self.jwks_url)
            return None

        keys = {
            jwk["kid"]: jwk
            for jwk in (payload.get("keys") or [])
            if isinstance(jwk, dict)
            and jwk.get("kid")
            # Issuers publish encryption keys next to signing keys; only the
            # latter can verify a token.
            and jwk.get("use", "sig") == "sig"
        }
        if not keys:
            logger.error("JWKS at %s held no signing keys", self.jwks_url)
            return None

        logger.info("Loaded %d signing key(s) from %s", len(keys), self.jwks_url)
        return keys


def normalize_audiences(audiences: Optional[Iterable[str]]) -> set[str]:
    """Trim and drop empties from a caller-supplied audience list."""
    return {a.strip() for a in (audiences or []) if a and a.strip()}


def presented_audiences(claims: dict) -> set[str]:
    """The `aud` values on a token, whether it carried a string or a list."""
    raw = claims.get("aud")
    if isinstance(raw, str):
        return {raw}
    return set(raw or [])
