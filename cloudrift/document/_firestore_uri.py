"""Shared URI construction for Firestore with MongoDB compatibility.

Firestore's MongoDB-compatible endpoint imposes three connection options that
are not optional and not defaults on any driver:

- ``loadBalanced=true`` — Firestore fronts the database with a load balancer and
  does not expose a replica-set topology. Without this the driver runs topology
  discovery and fails to find a server it recognizes.
- ``tls=true`` — the endpoint accepts TLS only.
- ``retryWrites=false`` — Firestore does not implement retryable writes, and
  pymongo enables them by default.

Getting any of these wrong surfaces as an opaque server-selection timeout rather
than a clear error, so the helpers below apply them rather than trusting the
caller to remember. Shared by the async (:mod:`cloudrift.document.firestore`)
and sync (:mod:`cloudrift.document.firestore_sync`) factories so the two cannot
drift.
"""

from urllib.parse import parse_qsl, quote_plus, urlencode, urlsplit, urlunsplit

#: Applied to every Firestore connection. See the module docstring.
REQUIRED_PARAMS: dict[str, str] = {
    "loadBalanced": "true",
    "tls": "true",
    "retryWrites": "false",
}

#: OIDC properties selecting Google Cloud as the token environment. ``gcp`` makes
#: the driver fetch an identity token from the metadata server, so ADC / Workload
#: Identity Federation works with no key material in the process.
_OIDC_MECHANISM = "MONGODB-OIDC"
_OIDC_PROPERTIES = "ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE"

_SCRAM_MECHANISM = "SCRAM-SHA-256"


def firestore_host(uid: str, location: str) -> str:
    """Return the Firestore MongoDB-compatibility host for a database UID."""
    return f"{uid}.{location}.firestore.goog"


def build_oidc_uri(
    uid: str,
    location: str,
    database: str,
    *,
    port: int = 443,
) -> str:
    """Build a URI authenticating with Google Cloud credentials via OIDC.

    Requires ``pymongo>=4.7`` — that is the release where the Python driver
    gained OIDC support for Google Cloud environments.
    """
    params = dict(REQUIRED_PARAMS)
    params["authMechanism"] = _OIDC_MECHANISM
    params["authMechanismProperties"] = _OIDC_PROPERTIES
    return _assemble(firestore_host(uid, location), port, database, params)


def build_scram_uri(
    uid: str,
    location: str,
    database: str,
    username: str,
    password: str,
    *,
    port: int = 443,
) -> str:
    """Build a URI authenticating with a SCRAM-SHA-256 user credential.

    The username/password pair comes from creating a user credential on the
    database (console, ``gcloud``, or the admin client libraries). GCP shows the
    password once and cannot retrieve it again.
    """
    params = dict(REQUIRED_PARAMS)
    params["authMechanism"] = _SCRAM_MECHANISM
    userinfo = f"{quote_plus(username)}:{quote_plus(password)}@"
    return _assemble(firestore_host(uid, location), port, database, params, userinfo=userinfo)


def build_access_token_uri(
    uid: str,
    location: str,
    database: str,
    access_token: str,
    *,
    port: int = 443,
) -> str:
    """Build a URI authenticating with a short-lived OAuth 2.0 access token.

    Firestore accepts a bearer token through the ``PLAIN`` mechanism against the
    ``$external`` auth source. The token is embedded in the URI, so the client
    stops working when it expires — prefer :func:`build_oidc_uri`, which lets the
    driver refresh on its own, unless you are minting tokens yourself (e.g.
    impersonating another service account).
    """
    params = dict(REQUIRED_PARAMS)
    params["authMechanism"] = "PLAIN"
    params["authSource"] = "$external"
    # PLAIN sends the token as the password; the username is ignored but must be
    # present for the driver to send credentials at all.
    userinfo = f"{quote_plus('oidc')}:{quote_plus(access_token)}@"
    return _assemble(firestore_host(uid, location), port, database, params, userinfo=userinfo)


def ensure_required_params(uri: str) -> str:
    """Return ``uri`` with Firestore's mandatory options filled in.

    Applies each of :data:`REQUIRED_PARAMS` that the caller has not already set,
    so a connection string copied from ``gcloud firestore databases
    connection-string`` passes through untouched while a hand-written one still
    connects.

    Option names are matched case-insensitively (MongoDB URI options are
    case-insensitive), and ``ssl`` counts as ``tls`` already being specified —
    pymongo rejects a URI that sets both to conflicting values.
    """
    parts = urlsplit(uri)
    existing = parse_qsl(parts.query, keep_blank_values=True)
    present = {key.lower() for key, _ in existing}
    additions = []
    for key, value in REQUIRED_PARAMS.items():
        if key.lower() in present:
            continue
        if key == "tls" and "ssl" in present:
            continue
        additions.append((key, value))
    if not additions:
        return uri
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(existing + additions),
            parts.fragment,
        )
    )


def _assemble(
    host: str,
    port: int,
    database: str,
    params: dict[str, str],
    *,
    userinfo: str = "",
) -> str:
    # urlencode would percent-encode the ':' and ',' in authMechanismProperties
    # and the '$' in authSource, which pymongo does not accept, so the query is
    # joined literally — every value here is a fixed token or a validated
    # identifier, never caller free-text.
    query = "&".join(f"{key}={value}" for key, value in params.items())
    return f"mongodb://{userinfo}{host}:{port}/{quote_plus(database)}?{query}"
