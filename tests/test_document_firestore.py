"""Tests for Firestore with MongoDB compatibility.

URI construction is validated by parsing every built URI with pymongo's own
``uri_parser`` — that is the component that would reject a malformed URI at
connect time, so asserting against it is what makes these tests meaningful
rather than just string comparisons.
"""

from unittest.mock import patch

import pytest
from pymongo import uri_parser

from cloudrift.document import get_mongodb, get_mongodb_sync
from cloudrift.document._firestore_uri import (
    REQUIRED_PARAMS,
    build_access_token_uri,
    build_oidc_uri,
    build_scram_uri,
    ensure_required_params,
    firestore_host,
)

UID = "f116f93a-519c-208a-9a72-3ef6c9a1f081"
LOCATION = "nam5"
DATABASE = "mydb"


def _options(uri: str) -> dict:
    return dict(uri_parser.parse_uri(uri)["options"])


# ---------------------------------------------------------------------------
# The three mandatory options
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "uri",
    [
        build_oidc_uri(UID, LOCATION, DATABASE),
        build_scram_uri(UID, LOCATION, DATABASE, "user", "pass"),
        build_access_token_uri(UID, LOCATION, DATABASE, "ya29.token"),
    ],
)
def test_every_builder_sets_the_mandatory_options(uri):
    """loadBalanced/tls/retryWrites=false are non-negotiable for Firestore.

    Omitting any one surfaces as an opaque server-selection timeout, so every
    auth path must carry all three.
    """
    options = _options(uri)
    assert options["loadBalanced"] is True
    assert options["tls"] is True
    assert options["retryWrites"] is False


def test_host_and_database_are_placed_correctly():
    parsed = uri_parser.parse_uri(build_oidc_uri(UID, LOCATION, DATABASE))
    assert parsed["nodelist"] == [(f"{UID}.{LOCATION}.firestore.goog", 443)]
    assert parsed["database"] == DATABASE


def test_firestore_host_format():
    assert firestore_host(UID, LOCATION) == f"{UID}.{LOCATION}.firestore.goog"


def test_custom_port_is_honored():
    parsed = uri_parser.parse_uri(build_oidc_uri(UID, LOCATION, DATABASE, port=8443))
    assert parsed["nodelist"][0][1] == 8443


# ---------------------------------------------------------------------------
# Per-mechanism wiring
# ---------------------------------------------------------------------------


def test_oidc_uri_selects_gcp_environment():
    options = _options(build_oidc_uri(UID, LOCATION, DATABASE))
    assert options["authMechanism"] == "MONGODB-OIDC"
    # pymongo parses the properties into a dict; ENVIRONMENT:gcp is what makes
    # the driver fetch an identity token from the metadata server.
    assert options["authMechanismProperties"]["ENVIRONMENT"] == "gcp"
    assert options["authMechanismProperties"]["TOKEN_RESOURCE"] == "FIRESTORE"


def test_scram_uri_carries_credentials():
    uri = build_scram_uri(UID, LOCATION, DATABASE, "myuser", "mypass")
    parsed = uri_parser.parse_uri(uri)
    assert parsed["username"] == "myuser"
    assert parsed["password"] == "mypass"
    assert dict(parsed["options"])["authMechanism"] == "SCRAM-SHA-256"


def test_scram_password_with_uri_metacharacters_round_trips():
    """A password containing @ : / ? # % must not corrupt the URI."""
    password = "p@ss:w/rd?#%x"
    uri = build_scram_uri(UID, LOCATION, DATABASE, "us:er", password)
    parsed = uri_parser.parse_uri(uri)
    assert parsed["password"] == password
    assert parsed["username"] == "us:er"
    assert parsed["nodelist"] == [(f"{UID}.{LOCATION}.firestore.goog", 443)]


def test_access_token_uri_uses_plain_against_external():
    uri = build_access_token_uri(UID, LOCATION, DATABASE, "ya29.a0Af_token")
    parsed = uri_parser.parse_uri(uri)
    options = dict(parsed["options"])
    assert options["authMechanism"] == "PLAIN"
    assert options["authSource"] == "$external"
    # PLAIN sends the bearer token as the password.
    assert parsed["password"] == "ya29.a0Af_token"


# ---------------------------------------------------------------------------
# ensure_required_params
# ---------------------------------------------------------------------------


def test_ensure_required_params_fills_in_a_bare_uri():
    uri = ensure_required_params(f"mongodb://{UID}.{LOCATION}.firestore.goog:443/{DATABASE}")
    options = _options(uri)
    assert options["loadBalanced"] is True
    assert options["tls"] is True
    assert options["retryWrites"] is False


def test_ensure_required_params_leaves_a_complete_uri_untouched():
    original = build_oidc_uri(UID, LOCATION, DATABASE)
    assert ensure_required_params(original) == original


def test_ensure_required_params_preserves_existing_query_options():
    uri = ensure_required_params(
        f"mongodb://{UID}.{LOCATION}.firestore.goog:443/{DATABASE}?appName=svc"
    )
    options = _options(uri)
    assert options["appname"] == "svc"
    assert options["loadBalanced"] is True


def test_ensure_required_params_does_not_override_caller_values():
    """A caller who deliberately set retryWrites=true keeps it.

    Firestore will reject it, but silently rewriting an explicit option would be
    worse: the caller would never learn their setting was ignored.
    """
    uri = ensure_required_params(
        f"mongodb://{UID}.{LOCATION}.firestore.goog:443/{DATABASE}?retryWrites=true"
    )
    assert _options(uri)["retryWrites"] is True


def test_ensure_required_params_treats_ssl_as_tls():
    """pymongo rejects a URI setting both tls and ssl, so ssl must suppress tls."""
    uri = ensure_required_params(
        f"mongodb://{UID}.{LOCATION}.firestore.goog:443/{DATABASE}?ssl=true"
    )
    assert "tls=true" not in uri
    # Must still parse — this is the assertion that would have caught adding both.
    assert _options(uri)["tls"] is True


def test_ensure_required_params_matches_option_names_case_insensitively():
    uri = ensure_required_params(
        f"mongodb://{UID}.{LOCATION}.firestore.goog:443/{DATABASE}?loadbalanced=true"
    )
    assert uri.lower().count("loadbalanced") == 1


def test_required_params_are_the_three_documented_options():
    assert REQUIRED_PARAMS == {
        "loadBalanced": "true",
        "tls": "true",
        "retryWrites": "false",
    }


# ---------------------------------------------------------------------------
# Factory routing
# ---------------------------------------------------------------------------


#: (factory, module patched by that factory) — the async and sync factories must
#: route identically, so every routing case runs against both.
_FACTORIES = [
    (get_mongodb, "cloudrift.document.firestore"),
    (get_mongodb_sync, "cloudrift.document.firestore_sync"),
]


@pytest.mark.parametrize("factory,module", _FACTORIES)
@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"uid": UID, "location": LOCATION, "database": DATABASE}, "connect_oidc"),
        (
            {
                "uid": UID,
                "location": LOCATION,
                "database": DATABASE,
                "username": "u",
                "password": "p",
            },
            "connect_scram",
        ),
        (
            {
                "uid": UID,
                "location": LOCATION,
                "database": DATABASE,
                "access_token": "ya29.token",
            },
            "connect_access_token",
        ),
        ({"uri": "mongodb://host:443/db"}, "connect_uri"),
        ({"connection_string": "mongodb://host:443/db"}, "connect_uri"),
    ],
)
def test_factory_routes_by_keys_present(factory, module, kwargs, expected):
    with patch(f"{module}.{expected}") as target:
        factory("firestore", **kwargs)
    target.assert_called_once()


@pytest.mark.parametrize("factory,module", _FACTORIES)
def test_connection_string_alias_is_passed_as_uri(factory, module):
    """`gcloud firestore databases connection-string` output is called a
    connection string, so callers reach for that key as well as `uri` — but the
    underlying function only takes `uri`."""
    with patch(f"{module}.connect_uri") as target:
        factory("firestore", connection_string="mongodb://host:443/db")
    target.assert_called_once_with(uri="mongodb://host:443/db")


@pytest.mark.parametrize("factory", [get_mongodb, get_mongodb_sync])
def test_factory_builds_a_working_client_end_to_end(factory):
    """No patching: the real driver must accept what the factory produces."""
    client = factory("firestore", uid=UID, location=LOCATION, database=DATABASE)
    try:
        assert client.options.load_balanced is True
        assert client.options.retry_writes is False
    finally:
        client.close()


def test_unknown_provider_still_lists_firestore():
    with pytest.raises(ValueError, match="firestore"):
        get_mongodb("nope")
