"""Relational SQL connection + authentication factory.

cloudrift's SQL layer abstracts how you *connect and authenticate* to a
relational database across clouds — static credentials, AWS RDS/Aurora IAM
tokens, and Azure AD / Entra tokens — and hands back a fully-authenticated
**native driver connection**. It deliberately does NOT abstract query execution,
SQL dialects, or schema introspection: those belong to the application, which
uses the returned connection's native cursor/execute API directly.

    from cloudrift.sql import get_sql

    backend = get_sql("postgres", "from_credentials",
                      host="db", port=5432, user="u", password="p", database="app")
    conn = await backend.connect(timeout=10)        # psycopg.AsyncConnection
    async with conn.cursor() as cur:
        await cur.execute("SELECT 1")

    # Azure SQL via Entra service principal — a fresh token per connect():
    backend = get_sql("azuresql", "from_entra_service_principal",
                      server="x.database.windows.net", database="app",
                      tenant_id="...", client_id="...", client_secret="...")

See each backend class for its supported auth methods.
"""
from cloudrift.sql.base import SQLBackend


def get_sql(provider: str, auth_method: str, **kwargs) -> SQLBackend:
    """Factory to instantiate a SQL connection backend.

    Args:
        provider: One of ``"postgres"``, ``"redshift"``, ``"mysql"``,
            ``"mssql"``, ``"azuresql"`` (alias of ``mssql``), ``"oracle"``,
            ``"databricks"``.
        auth_method: The ``from_*`` classmethod to call on the backend class.
            See each backend for supported methods, e.g. ``"from_credentials"``,
            ``"from_iam_auth"``, ``"from_entra_service_principal"``.
        **kwargs: Arguments forwarded to the chosen factory method.

    Returns:
        An :class:`SQLBackend` whose ``connect()`` returns a native async
        connection for the provider.
    """
    p = provider.lower()
    if p == "postgres" or p == "postgresql":
        from cloudrift.sql.postgresql import PostgresSQLBackend as _Backend
    elif p == "redshift":
        from cloudrift.sql.postgresql import RedshiftSQLBackend as _Backend
    elif p == "mysql":
        from cloudrift.sql.mysql import MySQLSQLBackend as _Backend
    elif p in ("mssql", "azuresql", "sqlserver"):
        from cloudrift.sql.mssql import MSSQLSQLBackend as _Backend
    elif p == "oracle":
        from cloudrift.sql.oracle import OracleSQLBackend as _Backend
    elif p == "databricks":
        from cloudrift.sql.databricks import DatabricksSQLBackend as _Backend
    else:
        raise ValueError(
            f"Unknown SQL provider: {provider!r}. Choose 'postgres', 'redshift', "
            "'mysql', 'mssql'/'azuresql', 'oracle', or 'databricks'."
        )

    factory = getattr(_Backend, auth_method, None)
    if factory is None:
        raise ValueError(f"{_Backend.__name__} has no auth method {auth_method!r}.")
    return factory(**kwargs)


__all__ = ["SQLBackend", "get_sql"]
