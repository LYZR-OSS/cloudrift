import asyncio
from contextlib import asynccontextmanager

from cloudrift.core.exceptions import SQLAuthError, SQLConnectionError
from cloudrift.sql.base import SQLBackend

# Azure Database for PostgreSQL — Microsoft Entra (AAD) token scope. The access
# token is used in place of a password; the DB user is the Entra principal name.
_AAD_TOKEN_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"


class PostgresSQLBackend(SQLBackend):
    """PostgreSQL (and wire-compatible engines such as Amazon Redshift) backed by
    the async ``psycopg`` (v3) driver.

    Use one of the class methods to construct:
    - ``from_credentials``            — static host/port/user/password
    - ``from_iam_auth``               — AWS RDS/Aurora IAM authentication (token as password)
    - ``from_entra_managed_identity`` — Azure Entra managed-identity token auth
    - ``from_gcp_iam_auth``           — Cloud SQL / AlloyDB IAM authentication (token as password)
    """

    dialect = "postgresql"
    # Default SQLAlchemy scheme for sqlalchemy_url() — async psycopg v3 driver.
    _sa_scheme = "postgresql+psycopg"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str,
        database: str,
        password: str | None = None,
        iam: bool = False,
        region: str | None = None,
        entra: bool = False,
        client_id: str | None = None,
        gcp_iam: bool = False,
        gcp_credentials: dict | None = None,
        connect_kwargs: dict | None = None,
        pool: bool = False,
        pool_min_size: int = 0,
        pool_max_size: int = 10,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._user = user
        self._database = database
        self._password = password
        self._iam = iam
        self._region = region
        self._entra = entra
        self._client_id = client_id
        self._gcp_iam = gcp_iam
        # Raw from_gcp_iam_auth kwargs (service_account_file/info, prefer_metadata).
        # The built google.auth Credentials object is cached separately, lazily,
        # on first _auth_password() call — see that method.
        self._gcp_iam_kwargs = gcp_credentials or {}
        self._gcp_credentials = None
        self._connect_kwargs = connect_kwargs or {}
        self._pool_enabled = pool
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._pool = None

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_credentials(
        cls,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        pool: bool = False,
        pool_min_size: int = 0,
        pool_max_size: int = 10,
        **connect_kwargs,
    ) -> "PostgresSQLBackend":
        """Authenticate with a static username/password.

        Set ``pool=True`` to enable a ``psycopg_pool`` connection pool used by
        :meth:`acquire`; ``connect()`` still opens standalone connections.
        """
        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_kwargs=connect_kwargs,
            pool=pool,
            pool_min_size=pool_min_size,
            pool_max_size=pool_max_size,
        )

    @classmethod
    def from_url(cls, url: str, **connect_kwargs) -> "PostgresSQLBackend":
        """Authenticate from a connection URL, e.g.
        ``postgresql://user:pass@host:5432/db`` (scheme is ignored)."""
        from cloudrift.sql._url import parse_sql_url

        p = parse_sql_url(url, default_port=5432)
        return cls.from_credentials(
            host=p["host"],
            port=p["port"],
            user=p["user"],
            password=p["password"],
            database=p["database"],
            **connect_kwargs,
        )

    def sqlalchemy_url(self, driver: str | None = None) -> str:
        """Return a SQLAlchemy URL for this connection (for SQLAlchemy-based
        consumers). ``driver`` overrides the dialect+driver scheme. Not available
        for IAM auth, whose token cannot be embedded in a static URL."""
        from cloudrift.sql._url import build_sqlalchemy_url

        if self._iam or self._entra or self._gcp_iam:
            from cloudrift.core.exceptions import SQLAuthError

            raise SQLAuthError(
                "sqlalchemy_url() is unavailable for token auth (IAM/Entra/GCP "
                "tokens are dynamic). Use connect()/acquire(), or pass a SQLAlchemy "
                "do_connect hook that calls the backend for a fresh token."
            )
        scheme = driver or self._sa_scheme
        return build_sqlalchemy_url(
            scheme,
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
        )

    @classmethod
    def from_iam_auth(
        cls,
        host: str,
        port: int,
        user: str,
        database: str,
        region: str,
        pool: bool = False,
        pool_min_size: int = 0,
        pool_max_size: int = 10,
        **connect_kwargs,
    ) -> "PostgresSQLBackend":
        """Authenticate to AWS RDS/Aurora PostgreSQL using an IAM auth token.

        A short-lived (15 min) token is generated on every :meth:`connect` call
        and used in place of a password. IAM auth requires TLS, so ``sslmode``
        defaults to ``require`` unless overridden in ``connect_kwargs``.

        Set ``pool=True`` to enable a ``psycopg_pool`` pool used by
        :meth:`acquire`; each pooled connection mints its own fresh token.
        """
        connect_kwargs.setdefault("sslmode", "require")
        return cls(
            host=host,
            port=port,
            user=user,
            database=database,
            iam=True,
            region=region,
            connect_kwargs=connect_kwargs,
            pool=pool,
            pool_min_size=pool_min_size,
            pool_max_size=pool_max_size,
        )

    @classmethod
    def from_entra_managed_identity(
        cls,
        host: str,
        port: int,
        user: str,
        database: str,
        client_id: str | None = None,
        pool: bool = False,
        pool_min_size: int = 0,
        pool_max_size: int = 10,
        **connect_kwargs,
    ) -> "PostgresSQLBackend":
        """Authenticate to Azure Database for PostgreSQL via a Microsoft Entra
        managed identity (system- or user-assigned).

        A short-lived Entra access token is generated on every :meth:`connect`
        call and used in place of a password; ``user`` is the Entra principal
        name configured on the server. ``client_id`` selects a user-assigned
        identity (omit for the system-assigned one). Entra auth requires TLS, so
        ``sslmode`` defaults to ``require`` unless overridden.
        """
        connect_kwargs.setdefault("sslmode", "require")
        return cls(
            host=host,
            port=port,
            user=user,
            database=database,
            entra=True,
            client_id=client_id,
            connect_kwargs=connect_kwargs,
            pool=pool,
            pool_min_size=pool_min_size,
            pool_max_size=pool_max_size,
        )

    @classmethod
    def from_gcp_iam_auth(
        cls,
        host: str,
        user: str,
        database: str,
        port: int = 5432,
        service_account_file: str | None = None,
        service_account_info: dict | None = None,
        prefer_metadata: bool = False,
        **connect_kwargs,
    ) -> "PostgresSQLBackend":
        """Authenticate to Cloud SQL / AlloyDB for PostgreSQL with an IAM token.

        The credential is built on first use and cached; its access token is
        used in place of a password and refreshed automatically only once it
        actually expires — never rebuilt or re-refreshed on every
        :meth:`connect`. No database password is ever stored. Requires IAM
        database authentication enabled on the instance and the principal added
        as a database user with ``roles/cloudsql.instanceUser``.

        ``user`` is the IAM principal's database username, which GCP derives
        differently per engine: for PostgreSQL it is the service-account email
        **with the ``.gserviceaccount.com`` suffix removed**
        (``sa@project.iam``), and for a human it is the full email address.
        cloudrift does not transform it — pass exactly what the instance's user
        list shows.

        IAM auth requires TLS, so ``sslmode`` defaults to ``require``.

        Args:
            host: Instance IP or the Cloud SQL Proxy / Auth Proxy address.
            user: Database username for the IAM principal (see above).
            database: Database name.
            port: Port (default 5432).
            service_account_file: Path to a service-account JSON key file.
            service_account_info: Parsed service-account JSON.
            prefer_metadata: Read the attached service account from the metadata
                server — see :mod:`cloudrift.core.gcp_credentials`.
            **connect_kwargs: Extra psycopg connection arguments.
        """
        connect_kwargs.setdefault("sslmode", "require")
        return cls(
            host=host,
            port=port,
            user=user,
            database=database,
            gcp_iam=True,
            gcp_credentials={
                "service_account_file": service_account_file,
                "service_account_info": service_account_info,
                "prefer_metadata": prefer_metadata,
            },
            connect_kwargs=connect_kwargs,
        )

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def _entra_token(self) -> str:
        try:
            from azure.identity.aio import (
                DefaultAzureCredential,
                ManagedIdentityCredential,
            )
        except ImportError as e:  # pragma: no cover - import guard
            raise SQLAuthError(
                "Entra managed-identity auth requires azure-identity. Install cloudrift[azure]."
            ) from e

        try:
            credential = (
                ManagedIdentityCredential(client_id=self._client_id)
                if self._client_id
                else DefaultAzureCredential()
            )
            async with credential:
                token = await credential.get_token(_AAD_TOKEN_SCOPE)
            return token.token
        except Exception as e:
            raise SQLAuthError(f"Failed to acquire Entra token for PostgreSQL: {e}") from e

    async def _rds_token(self) -> str:
        try:
            import boto3
        except ImportError as e:  # pragma: no cover - import guard
            raise SQLAuthError(
                "RDS IAM auth requires boto3. Install cloudrift[sql-postgres] or boto3."
            ) from e

        def _gen() -> str:
            client = boto3.client("rds", region_name=self._region)
            return client.generate_db_auth_token(
                DBHostname=self._host,
                Port=self._port,
                DBUsername=self._user,
                Region=self._region,
            )

        try:
            return await asyncio.to_thread(_gen)
        except Exception as e:
            raise SQLAuthError(f"Failed to generate RDS IAM auth token: {e}") from e

    async def _auth_password(self) -> str | None:
        """Resolve the password for this connection, minting a token if needed.

        Single resolution point for every auth mode — AWS RDS IAM, Azure Entra,
        GCP Cloud SQL IAM, or a static password — so ``connect()`` and the pooled
        token-connection class stay in sync.
        """
        if self._iam:
            return await self._rds_token()
        if self._entra:
            return await self._entra_token()
        if self._gcp_iam:
            from cloudrift.sql._gcp_iam import (
                build_cloud_sql_credentials,
                cloud_sql_access_token,
            )

            if self._gcp_credentials is None:
                self._gcp_credentials = build_cloud_sql_credentials(**self._gcp_iam_kwargs)
            return await cloud_sql_access_token(self._gcp_credentials)
        return self._password

    async def connect(self, timeout: float | None = None):
        try:
            import psycopg
        except ImportError as e:  # pragma: no cover - import guard
            raise SQLConnectionError(
                "PostgreSQL support requires psycopg. Install cloudrift[sql-postgres]."
            ) from e

        password = await self._auth_password()
        kwargs = dict(self._connect_kwargs)
        if timeout is not None:
            kwargs["connect_timeout"] = int(timeout)
        try:
            return await psycopg.AsyncConnection.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                password=password,
                dbname=self._database,
                **kwargs,
            )
        except Exception as e:
            raise SQLConnectionError(
                f"Failed to connect to PostgreSQL at {self._host}:{self._port}: {e}"
            ) from e

    # ------------------------------------------------------------------
    # Pooling (opt-in via pool=True)
    # ------------------------------------------------------------------

    async def _ensure_pool(self):
        if self._pool is None:
            try:
                from psycopg_pool import AsyncConnectionPool
            except ImportError as e:  # pragma: no cover - import guard
                raise SQLConnectionError(
                    "Pooling requires psycopg_pool. Install cloudrift[sql-postgres]."
                ) from e
            kwargs = {
                "host": self._host,
                "port": self._port,
                "user": self._user,
                "dbname": self._database,
                **self._connect_kwargs,
            }
            pool_kwargs: dict = dict(
                conninfo="",
                kwargs=kwargs,
                min_size=self._pool_min_size,
                max_size=self._pool_max_size,
                open=False,
            )
            if self._iam or self._entra or self._gcp_iam:
                # Token auth: no static password. Each physical connection the
                # pool opens mints its own fresh short-lived token (RDS IAM
                # tokens last ~15 min) via a custom connection class.
                pool_kwargs["connection_class"] = self._token_connection_class()
            else:
                kwargs["password"] = self._password
            pool = AsyncConnectionPool(**pool_kwargs)
            await pool.open()
            self._pool = pool
        return self._pool

    def _token_connection_class(self):
        """Build a ``psycopg.AsyncConnection`` subclass that authenticates each
        new physical pool connection with a freshly minted IAM/Entra token."""
        import psycopg

        backend = self

        class _TokenConnection(psycopg.AsyncConnection):
            @classmethod
            async def connect(cls, conninfo="", **kwargs):
                # Route through the backend's single resolver so a pooled RDS IAM,
                # Entra, or GCP IAM connection all mint their token the same way.
                kwargs["password"] = await backend._auth_password()
                return await super().connect(conninfo, **kwargs)

        return _TokenConnection

    @asynccontextmanager
    async def acquire(self, timeout: float | None = None):
        if not self._pool_enabled:
            async with super().acquire(timeout) as conn:
                yield conn
            return
        pool = await self._ensure_pool()
        async with pool.connection() as conn:
            yield conn

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None


class RedshiftSQLBackend(PostgresSQLBackend):
    """Amazon Redshift backend. Redshift speaks the PostgreSQL wire protocol, so
    this reuses :class:`PostgresSQLBackend` and only differs in ``dialect`` and a
    UTF-8 client-encoding default."""

    dialect = "redshift"

    @classmethod
    def from_credentials(cls, host, port, user, password, database, **connect_kwargs):
        connect_kwargs.setdefault("client_encoding", "utf8")
        return super().from_credentials(host, port, user, password, database, **connect_kwargs)
