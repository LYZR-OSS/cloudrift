import asyncio
from contextlib import asynccontextmanager

from cloudrift.core.exceptions import SQLAuthError, SQLConnectionError
from cloudrift.sql.base import SQLBackend

# SQL_COPT_SS_ACCESS_TOKEN — the ODBC pre-connect attribute used to pass an
# Azure AD / Entra access token instead of a username/password.
_SQL_COPT_SS_ACCESS_TOKEN = 1256
_AAD_TOKEN_SCOPE = "https://database.windows.net/.default"
_DEFAULT_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


def _pop_kwarg(kwargs: dict, key: str, default):
    """Case-insensitively pop *key* from *kwargs*, returning *default* if absent.

    Lets callers override ODBC defaults (Encrypt, TrustServerCertificate,
    Connection Timeout) under any casing without the key appearing twice in the
    final connection string.
    """
    for k in list(kwargs.keys()):
        if k.lower() == key.lower():
            return kwargs.pop(k)
    return default


class MSSQLSQLBackend(SQLBackend):
    """Microsoft SQL Server / Azure SQL Database backed by the async ``aioodbc``
    driver and the Microsoft ODBC Driver 18.

    Use one of the class methods to construct:
    - ``from_credentials``            — SQL login (username/password), optional
      self-signed certificate pinning via ``server_certificate`` (PEM)
    - ``from_entra_service_principal``— Azure AD / Entra service principal
      (tenant_id + client_id + client_secret)
    - ``from_entra_managed_identity`` — Azure managed identity (system- or
      user-assigned via ``client_id``)

    For the Entra methods a fresh access token is acquired on every
    :meth:`connect` call, so opening a new connection per query is safe.
    """

    dialect = "mssql"

    def __init__(
        self,
        *,
        server: str,
        database: str,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        server_certificate: str | None = None,
        token_provider=None,
        connection_kwargs: dict | None = None,
        odbc_driver: str = _DEFAULT_ODBC_DRIVER,
        pool: bool = False,
        pool_min_size: int = 1,
        pool_max_size: int = 10,
    ) -> None:
        self._database = database
        self._username = username
        self._password = password
        self._server_certificate = server_certificate
        self._token_provider = token_provider  # callable() -> token str, or None
        self._odbc_driver = odbc_driver
        self._pool_enabled = pool
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._pool = None

        # Host/port kept separate for the cert-pinning pre-check; the ODBC
        # SERVER value folds the port in as "host,port".
        self._cert_host = server
        self._cert_port = port if port is not None else 1433
        self._server = f"{server},{port}" if port is not None else server

        kwargs = dict(connection_kwargs or {})
        # Pinning mode: we validate the fingerprint ourselves before connecting,
        # so ODBC can skip its own chain validation.
        if self._server_certificate is not None:
            encrypt = "yes"
            trust_server_cert = "yes"
            _pop_kwarg(kwargs, "Encrypt", None)
            _pop_kwarg(kwargs, "TrustServerCertificate", None)
        else:
            encrypt = _pop_kwarg(kwargs, "Encrypt", "yes")
            trust_server_cert = _pop_kwarg(kwargs, "TrustServerCertificate", "no")
        self._timeout_default = _pop_kwarg(kwargs, "Connection Timeout", 30)
        self._encrypt = encrypt
        self._trust_server_cert = trust_server_cert
        self._extra_kwargs = kwargs

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_credentials(
        cls,
        server: str,
        database: str,
        username: str,
        password: str,
        port: int | None = None,
        server_certificate: str | None = None,
        connection_kwargs: dict | None = None,
        odbc_driver: str = _DEFAULT_ODBC_DRIVER,
        pool: bool = False,
        pool_min_size: int = 1,
        pool_max_size: int = 10,
    ) -> "MSSQLSQLBackend":
        """Authenticate with a SQL login (username/password).

        ``server_certificate`` (a PEM string) enables certificate pinning for
        self-signed servers — the live certificate's SHA-256 fingerprint is
        verified before connecting.

        Set ``pool=True`` to enable an ``aioodbc`` connection pool used by
        :meth:`acquire` (credential auth only — token auth cannot share a static
        pool); ``connect()`` still opens standalone connections.
        """
        return cls(
            server=server,
            database=database,
            port=port,
            username=username,
            password=password,
            server_certificate=server_certificate,
            connection_kwargs=connection_kwargs,
            odbc_driver=odbc_driver,
            pool=pool,
            pool_min_size=pool_min_size,
            pool_max_size=pool_max_size,
        )

    @classmethod
    def from_entra_service_principal(
        cls,
        server: str,
        database: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        port: int | None = None,
        connection_kwargs: dict | None = None,
        odbc_driver: str = _DEFAULT_ODBC_DRIVER,
    ) -> "MSSQLSQLBackend":
        """Authenticate via an Azure AD / Entra service principal."""

        def _provider():
            from azure.identity import ClientSecretCredential

            cred = ClientSecretCredential(
                tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
            )
            return cred.get_token(_AAD_TOKEN_SCOPE).token

        return cls(
            server=server,
            database=database,
            port=port,
            token_provider=_provider,
            connection_kwargs=connection_kwargs,
            odbc_driver=odbc_driver,
        )

    @classmethod
    def from_entra_managed_identity(
        cls,
        server: str,
        database: str,
        client_id: str | None = None,
        port: int | None = None,
        connection_kwargs: dict | None = None,
        odbc_driver: str = _DEFAULT_ODBC_DRIVER,
        credential_options: dict | None = None,
    ) -> "MSSQLSQLBackend":
        """Authenticate via Azure AD: workload identity → managed identity → az CLI.

        ``client_id`` selects a user-assigned managed identity; omit it for the
        system-assigned one. ``credential_options`` is forwarded to
        ``DefaultAzureCredential`` — see :mod:`cloudrift.core.azure_credentials`.
        """

        def _provider():
            from cloudrift.core.azure_credentials import build_credential

            cred = build_credential(client_id, **(credential_options or {}))
            return cred.get_token(_AAD_TOKEN_SCOPE).token

        return cls(
            server=server,
            database=database,
            port=port,
            token_provider=_provider,
            connection_kwargs=connection_kwargs,
            odbc_driver=odbc_driver,
        )

    # ------------------------------------------------------------------
    # Connection-string assembly
    # ------------------------------------------------------------------

    def build_connection_string(self, timeout: float | None = None) -> str:
        """Assemble the ODBC connection string. Credentials are included only for
        SQL-login auth; token auth omits UID/PWD (the token is passed separately
        via ``attrs_before``)."""
        conn_timeout = int(timeout) if timeout is not None else self._timeout_default
        parts = [
            f"DRIVER={{{self._odbc_driver}}}",
            f"SERVER={self._server}",
            f"DATABASE={self._database}",
        ]
        if self._token_provider is None:
            parts.append(f"UID={self._username}")
            parts.append(f"PWD={self._password}")
        parts.append(f"Connection Timeout={conn_timeout}")
        parts.append(f"Encrypt={self._encrypt}")
        parts.append(f"TrustServerCertificate={self._trust_server_cert}")
        parts.extend(f"{k}={v}" for k, v in self._extra_kwargs.items())
        return ";".join(parts) + ";"

    async def _access_token_struct(self) -> bytes:
        import struct

        try:
            token = await asyncio.to_thread(self._token_provider)
        except Exception as e:
            raise SQLAuthError(f"Failed to acquire Azure AD access token: {e}") from e
        token_bytes = token.encode("UTF-16-LE")
        return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def connect(self, timeout: float | None = None):
        try:
            import aioodbc
        except ImportError as e:  # pragma: no cover - import guard
            raise SQLConnectionError(
                "MS SQL support requires aioodbc. Install cloudrift[sql-mssql]."
            ) from e

        if self._server_certificate is not None:
            from cloudrift.sql._mssql_tls import validate_pinned_certificate

            await validate_pinned_certificate(
                self._cert_host, self._cert_port, self._server_certificate
            )

        dsn = self.build_connection_string(timeout)
        try:
            if self._token_provider is not None:
                token_struct = await self._access_token_struct()
                return await aioodbc.connect(
                    dsn=dsn, attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct}
                )
            return await aioodbc.connect(dsn=dsn)
        except SQLAuthError:
            raise
        except Exception as e:
            raise SQLConnectionError(
                f"Failed to connect to MS SQL / Azure SQL at {self._server}: {e}"
            ) from e

    # ------------------------------------------------------------------
    # Pooling (opt-in via pool=True; credential auth only)
    # ------------------------------------------------------------------

    async def _ensure_pool(self):
        if self._pool is None:
            if self._token_provider is not None:
                raise SQLConnectionError(
                    "Connection pooling is not supported with Azure AD/Entra token "
                    "auth (tokens expire and cannot be shared across a static pool). "
                    "Use connect() per query instead."
                )
            try:
                import aioodbc
            except ImportError as e:  # pragma: no cover - import guard
                raise SQLConnectionError(
                    "Pooling requires aioodbc. Install cloudrift[sql-mssql]."
                ) from e
            if self._server_certificate is not None:
                from cloudrift.sql._mssql_tls import validate_pinned_certificate

                await validate_pinned_certificate(
                    self._cert_host, self._cert_port, self._server_certificate
                )
            dsn = self.build_connection_string()
            self._pool = await aioodbc.create_pool(
                dsn=dsn, minsize=self._pool_min_size, maxsize=self._pool_max_size
            )
        return self._pool

    @asynccontextmanager
    async def acquire(self, timeout: float | None = None):
        if not self._pool_enabled:
            async with super().acquire(timeout) as conn:
                yield conn
            return
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            yield conn

    async def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
