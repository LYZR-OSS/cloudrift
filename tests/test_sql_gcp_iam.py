"""Tests for Cloud SQL IAM database authentication.

The access token replaces the database password. Unlike AWS RDS IAM (a local
SigV4 presign) or Azure Entra (a per-connect credential), minting a Cloud SQL
token is a real network round trip to the OAuth endpoint — so the credential is
built once and cached on the backend, and cloud_sql_access_token() refreshes it
only when the cached token has actually expired, never on every call. These
tests cover that caching contract plus that the token/scope reach the driver.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cloudrift.core.exceptions import SQLAuthError
from cloudrift.sql import get_sql
from cloudrift.sql._gcp_iam import (
    CLOUD_SQL_LOGIN_SCOPE,
    build_cloud_sql_credentials,
    cloud_sql_access_token,
)
from cloudrift.sql.mysql import MySQLSQLBackend
from cloudrift.sql.postgresql import PostgresSQLBackend

HOST = "10.20.0.5"


def _credentials(token="ya29.token", valid=False):
    credentials = MagicMock()
    credentials.token = token
    credentials.valid = valid
    return credentials


# ---------------------------------------------------------------------------
# Credential building
# ---------------------------------------------------------------------------


def test_credentials_are_built_with_the_cloud_sql_login_scope():
    """cloud-platform is rejected at login — only sqlservice.login works, so a
    regression here would break every IAM connection."""
    with patch("cloudrift.core.gcp_credentials.build_credentials") as build:
        build_cloud_sql_credentials()
    assert build.call_args.kwargs["scopes"] == [CLOUD_SQL_LOGIN_SCOPE]
    assert CLOUD_SQL_LOGIN_SCOPE.endswith("sqlservice.login")


def test_credentials_forward_the_identity_options():
    with patch("cloudrift.core.gcp_credentials.build_credentials") as build:
        build_cloud_sql_credentials(service_account_file="/tmp/sa.json", prefer_metadata=False)
    assert build.call_args.kwargs["service_account_file"] == "/tmp/sa.json"


# ---------------------------------------------------------------------------
# Token access — build once, refresh only when expired
# ---------------------------------------------------------------------------


async def test_access_token_refreshes_an_invalid_credential():
    credentials = _credentials(valid=False)
    assert await cloud_sql_access_token(credentials) == "ya29.token"
    credentials.refresh.assert_called_once()


async def test_access_token_does_not_refresh_a_valid_credential():
    """The whole point of caching: a still-valid token must not trigger another
    network round trip to the OAuth endpoint on every call."""
    credentials = _credentials(valid=True)
    assert await cloud_sql_access_token(credentials) == "ya29.token"
    credentials.refresh.assert_not_called()


async def test_access_token_refresh_failure_is_translated():
    credentials = _credentials(valid=False)
    credentials.refresh.side_effect = RuntimeError("no metadata server")
    with pytest.raises(SQLAuthError, match="Cloud SQL IAM access token"):
        await cloud_sql_access_token(credentials)


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------


def test_postgres_gcp_iam_requires_tls_by_default():
    backend = PostgresSQLBackend.from_gcp_iam_auth(HOST, "sa@p.iam", "app")
    assert backend._connect_kwargs["sslmode"] == "require"
    assert backend._gcp_iam is True
    assert backend._port == 5432


def test_postgres_gcp_iam_sslmode_can_be_overridden():
    backend = PostgresSQLBackend.from_gcp_iam_auth(HOST, "sa@p.iam", "app", sslmode="verify-full")
    assert backend._connect_kwargs["sslmode"] == "verify-full"


async def test_postgres_gcp_iam_resolves_the_token_as_the_password():
    backend = PostgresSQLBackend.from_gcp_iam_auth(HOST, "sa@p.iam", "app")
    with patch(
        "cloudrift.core.gcp_credentials.build_credentials",
        return_value=_credentials(token="ya29.fresh", valid=True),
    ):
        assert await backend._auth_password() == "ya29.fresh"


async def test_postgres_caches_the_credential_across_connects():
    """The credential is built once — not re-resolved (re-parsing a key file or
    re-running ADC) on every connect. Only the token refresh is per-call, and
    only once the cached token has actually expired (covered separately)."""
    backend = PostgresSQLBackend.from_gcp_iam_auth(HOST, "sa@p.iam", "app")
    with patch(
        "cloudrift.core.gcp_credentials.build_credentials",
        return_value=_credentials(token="ya29.fresh", valid=True),
    ) as build:
        await backend._auth_password()
        await backend._auth_password()
    build.assert_called_once()


async def test_postgres_mints_a_fresh_token_once_the_cached_one_expires():
    backend = PostgresSQLBackend.from_gcp_iam_auth(HOST, "sa@p.iam", "app")
    credentials = _credentials(token="token-1", valid=False)

    def _refresh(_request):
        credentials.valid = True
        credentials.token = "token-2"

    credentials.refresh.side_effect = _refresh
    with patch("cloudrift.core.gcp_credentials.build_credentials", return_value=credentials):
        first = await backend._auth_password()
        second = await backend._auth_password()
    assert [first, second] == ["token-2", "token-2"]
    credentials.refresh.assert_called_once()


async def test_postgres_static_credentials_mint_no_token():
    backend = PostgresSQLBackend.from_credentials(HOST, 5432, "u", "pw", "app")
    build = MagicMock()
    with patch("cloudrift.core.gcp_credentials.build_credentials", build):
        assert await backend._auth_password() == "pw"
    build.assert_not_called()


async def test_postgres_gcp_iam_forwards_the_identity_options():
    backend = PostgresSQLBackend.from_gcp_iam_auth(
        HOST, "sa@p.iam", "app", service_account_file="/tmp/sa.json"
    )
    with patch(
        "cloudrift.core.gcp_credentials.build_credentials",
        return_value=_credentials(valid=True),
    ) as build:
        await backend._auth_password()
    assert build.call_args.kwargs["service_account_file"] == "/tmp/sa.json"


async def test_postgres_token_reaches_the_driver():
    """End-to-end through connect(), so the wiring is covered and not just
    _auth_password. Skipped where the driver is not installed — psycopg lives in
    the sql-postgres extra, not dev."""
    psycopg = pytest.importorskip("psycopg")
    backend = PostgresSQLBackend.from_gcp_iam_auth(HOST, "sa@p.iam", "app")
    connect = AsyncMock(return_value="conn")
    with (
        patch(
            "cloudrift.core.gcp_credentials.build_credentials",
            return_value=_credentials(token="ya29.fresh", valid=True),
        ),
        patch.object(psycopg.AsyncConnection, "connect", connect),
    ):
        assert await backend.connect() == "conn"
    assert connect.await_args.kwargs["password"] == "ya29.fresh"
    assert connect.await_args.kwargs["user"] == "sa@p.iam"


def test_postgres_sqlalchemy_url_unavailable_for_gcp_iam():
    """A dynamic token cannot be embedded in a static URL."""
    backend = PostgresSQLBackend.from_gcp_iam_auth(HOST, "sa@p.iam", "app")
    with pytest.raises(SQLAuthError, match="dynamic"):
        backend.sqlalchemy_url()


def test_postgres_sqlalchemy_url_still_works_for_static_credentials():
    backend = PostgresSQLBackend.from_credentials(HOST, 5432, "u", "pw", "app")
    assert backend.sqlalchemy_url().startswith("postgresql+psycopg://u:pw@")


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------


def test_mysql_gcp_iam_defaults():
    backend = MySQLSQLBackend.from_gcp_iam_auth(HOST, "sa-name", "app")
    assert backend._gcp_iam is True
    assert backend._port == 3306


async def test_mysql_gcp_iam_resolves_the_token_as_the_password():
    # MySQL usernames cap at 32 chars, so GCP uses only the SA local part —
    # cloudrift passes `user` through untouched either way.
    backend = MySQLSQLBackend.from_gcp_iam_auth(HOST, "sa-name", "app")
    with patch(
        "cloudrift.core.gcp_credentials.build_credentials",
        return_value=_credentials(token="ya29.fresh", valid=True),
    ):
        assert await backend._auth_password() == "ya29.fresh"
    assert backend._user == "sa-name"


async def test_mysql_caches_the_credential_across_connects():
    backend = MySQLSQLBackend.from_gcp_iam_auth(HOST, "sa-name", "app")
    with patch(
        "cloudrift.core.gcp_credentials.build_credentials",
        return_value=_credentials(valid=True),
    ) as build:
        await backend._auth_password()
        await backend._auth_password()
    build.assert_called_once()


async def test_mysql_static_credentials_mint_no_token():
    backend = MySQLSQLBackend.from_credentials(HOST, 3306, "u", "pw", "app")
    build = MagicMock()
    with patch("cloudrift.core.gcp_credentials.build_credentials", build):
        assert await backend._auth_password() == "pw"
    build.assert_not_called()


def test_mysql_sqlalchemy_url_unavailable_for_gcp_iam():
    backend = MySQLSQLBackend.from_gcp_iam_auth(HOST, "sa-name", "app")
    with pytest.raises(SQLAuthError, match="IAM auth"):
        backend.sqlalchemy_url()


# ---------------------------------------------------------------------------
# Factory reachability
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider", ["postgres", "postgresql"])
def test_get_sql_exposes_gcp_iam_for_postgres(provider):
    backend = get_sql(provider, "from_gcp_iam_auth", host=HOST, user="sa@p.iam", database="app")
    assert isinstance(backend, PostgresSQLBackend)
    assert backend._gcp_iam is True


def test_get_sql_exposes_gcp_iam_for_mysql():
    backend = get_sql("mysql", "from_gcp_iam_auth", host=HOST, user="sa-name", database="app")
    assert isinstance(backend, MySQLSQLBackend)
    assert backend._gcp_iam is True


def test_aws_rds_iam_is_unaffected():
    """The GCP path must not disturb the existing AWS one."""
    backend = PostgresSQLBackend.from_iam_auth(HOST, 5432, "u", "app", "us-east-1")
    assert backend._iam is True
    assert backend._gcp_iam is False
