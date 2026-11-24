"""Microsoft Entra workload identity verification.

Covers both ways an Azure workload proves itself without a stored secret:
an **Azure managed identity** (system- or user-assigned, token from IMDS) and
**workload identity federation** (an AKS pod's projected service account token
exchanged for an Entra token). Both arrive here as the same app-only OIDC JWT,
so one verifier serves both.

Three checks beyond signature, expiry and issuer, each of which closes a hole
the signature alone leaves open:

* **tenant** -- the issuer is pinned to the configured tenant's own URLs, and
  ``tid`` is then checked against it as well. This matters because Entra signs
  with keys shared across tenants: a token minted in any other directory is
  genuinely signed, and only the issuer/tenant pin refuses it. The ``tid``
  check is defence in depth behind the issuer pin, not the primary control.
* **audience** -- ``aud`` must be one the relying party accepts. Without this,
  any application in the same tenant could replay a token it already holds.
* **app-only** -- the token must represent an application, not a person acting
  through one. A delegated token carries ``scp``; an app-only token carries
  ``roles``/``idtyp=app`` and never ``scp``.

``oid`` is the durable object id of the managed identity or service principal
and becomes ``principal_id``. ``appid`` (v1.0) / ``azp`` (v2.0) is the
application id and becomes ``client_id``.
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional

from cloudrift.core.exceptions import TokenVerificationError
from cloudrift.identity.base import (
    OIDCWorkloadVerifier,
    WorkloadPrincipal,
    normalize_audiences,
    presented_audiences,
)

logger = logging.getLogger(__name__)

# Entra issues v1.0 and v2.0 access tokens from different issuer URLs for the
# same tenant. Which one a workload receives is decided by the app
# registration's `accessTokenAcceptedVersion`, not by the workload, so both are
# accepted and the tenant is pinned separately via `tid`.
V1_ISSUER = "https://sts.windows.net/{tenant_id}/"
V2_ISSUER = "https://login.microsoftonline.com/{tenant_id}/v2.0"
DEFAULT_JWKS_URL = "https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"


class EntraWorkloadVerifier(OIDCWorkloadVerifier):
    """Verifies app-only Entra tokens for one tenant.

    Construct via :meth:`from_tenant`, not ``__init__`` directly, matching the
    ``from_*`` convention used across cloudrift.
    """

    def __init__(
        self,
        tenant_id: Optional[str],
        accepted_audiences: Optional[Iterable[str]] = None,
        jwks_url: Optional[str] = None,
        extra_issuers: Optional[Iterable[str]] = None,
        require_app_only: bool = True,
    ):
        super().__init__()
        self._tenant_id = (tenant_id or "").strip()
        self._accepted = normalize_audiences(accepted_audiences)
        self._jwks_url_override = (jwks_url or "").strip()
        self._extra_issuers = tuple(
            i.strip() for i in (extra_issuers or []) if i and i.strip()
        )
        self._require_app_only = require_app_only

    @classmethod
    def from_tenant(
        cls,
        tenant_id: str,
        accepted_audiences: Iterable[str],
        jwks_url: Optional[str] = None,
        extra_issuers: Optional[Iterable[str]] = None,
        require_app_only: bool = True,
    ) -> "EntraWorkloadVerifier":
        """Build a verifier for one Entra tenant.

        Args:
            tenant_id: the Entra tenant (directory) id.
            accepted_audiences: Application ID URIs or client ids this relying
                party answers for. **Required** -- see ``configured``.
            jwks_url: override for a sovereign cloud (.us / .cn / .de).
            extra_issuers: additional acceptable ``iss`` values.
            require_app_only: reject user-delegated tokens. Leave on unless the
                caller genuinely wants to accept a human.
        """
        return cls(
            tenant_id=tenant_id,
            accepted_audiences=accepted_audiences,
            jwks_url=jwks_url,
            extra_issuers=extra_issuers,
            require_app_only=require_app_only,
        )

    # ------------------------------------------------------------------

    @property
    def provider(self) -> str:
        return "entra"

    @property
    def configured(self) -> bool:
        # Audiences are mandatory, unlike some OIDC relying parties where they
        # are optional: an Entra token with no audience check is just "some
        # token from this tenant", which would admit any app in the directory.
        return bool(self._tenant_id and self._accepted)

    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    @property
    def expected_issuers(self) -> tuple[str, ...]:
        return (
            V1_ISSUER.format(tenant_id=self._tenant_id),
            V2_ISSUER.format(tenant_id=self._tenant_id),
        ) + self._extra_issuers

    @property
    def jwks_url(self) -> str:
        if self._jwks_url_override:
            return self._jwks_url_override
        return DEFAULT_JWKS_URL.format(tenant_id=self._tenant_id)

    # ------------------------------------------------------------------

    def _principal(self, claims: dict) -> WorkloadPrincipal:
        self._check_tenant(claims)
        self._check_audience(claims)
        if self._require_app_only:
            self._check_app_only(claims)

        object_id = claims.get("oid")
        if not object_id:
            logger.warning("Entra token carries no oid; cannot identify the workload")
            raise TokenVerificationError("Token carries no object id")

        return WorkloadPrincipal(
            provider="entra",
            issuer=claims.get("iss", ""),
            principal_id=object_id,
            tenant_id=claims.get("tid"),
            client_id=claims.get("appid") or claims.get("azp"),
            claims=claims,
        )

    def _check_tenant(self, claims: dict) -> None:
        tid = claims.get("tid")
        if tid != self._tenant_id:
            logger.warning(
                "Entra token from tenant %s rejected; this verifier accepts %s",
                tid, self._tenant_id,
            )
            raise TokenVerificationError("Token is from a different Entra tenant")

    def _check_audience(self, claims: dict) -> None:
        presented = presented_audiences(claims)
        if not presented & self._accepted:
            logger.warning(
                "Entra token audience %s matches none of the accepted %s",
                sorted(presented), sorted(self._accepted),
            )
            raise TokenVerificationError("Token was not issued for this audience")

    def _check_app_only(self, claims: dict) -> None:
        """Reject a token that carries a user.

        ``scp`` (delegated scopes) is present only when a human consented and is
        the clearest signal. ``idtyp`` is the explicit one when Entra emits it.
        Both are checked because ``idtyp`` is an optional claim that has to be
        enabled on the app registration, so it cannot be relied on alone.
        """
        if claims.get("scp"):
            logger.warning(
                "Entra token for oid=%s carries delegated scopes; "
                "workload identity requires an app-only token",
                claims.get("oid"),
            )
            raise TokenVerificationError("Token is user-delegated, not app-only")

        idtyp = claims.get("idtyp")
        if idtyp is not None and idtyp != "app":
            logger.warning("Entra token has idtyp=%s; expected 'app'", idtyp)
            raise TokenVerificationError("Token is not an application token")
