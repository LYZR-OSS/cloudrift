"""Shared URL helpers for SQL backends.

Parse a SQLAlchemy-/driver-style connection URL into discrete components, and
re-emit a SQLAlchemy URL with proper percent-encoding. Used by the ``from_url``
constructors and the ``sqlalchemy_url()`` helpers.
"""
from urllib.parse import quote, unquote, urlsplit


def parse_sql_url(url: str, default_port: int | None = None) -> dict:
    """Parse ``[scheme://]user:pass@host:port/database`` into a dict.

    The scheme (e.g. ``postgresql+psycopg``) is accepted but ignored — the caller
    already knows the dialect. A bare ``user:pass@host:port/db`` (no scheme) is
    also accepted. Percent-encoded credentials are decoded.

    Returns keys: ``host``, ``port`` (int or ``default_port``), ``user``,
    ``password``, ``database`` (None if absent).
    """
    work = url if "://" in url else f"//{url}"
    parts = urlsplit(work)
    if not parts.hostname:
        raise ValueError(f"Could not parse host from SQL URL: {url!r}")
    database = parts.path.lstrip("/") or None
    return {
        "host": parts.hostname,
        "port": parts.port if parts.port is not None else default_port,
        "user": unquote(parts.username) if parts.username else None,
        "password": unquote(parts.password) if parts.password else None,
        "database": unquote(database) if database else None,
    }


def build_sqlalchemy_url(
    scheme: str,
    *,
    host: str,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
) -> str:
    """Build a percent-encoded SQLAlchemy URL, e.g.
    ``mysql+aiomysql://user:p%40ss@host:3306/db``. ``database`` may be omitted."""
    auth = ""
    if user is not None:
        auth = quote(user, safe="")
        if password is not None:
            auth += f":{quote(password, safe='')}"
        auth += "@"
    netloc = f"{auth}{host}"
    if port is not None:
        netloc += f":{port}"
    path = f"/{quote(database, safe='')}" if database else ""
    return f"{scheme}://{netloc}{path}"
