import asyncio
from contextlib import asynccontextmanager

from cloudrift.core.exceptions import SQLAuthError, SQLConnectionError
from cloudrift.sql.base import SQLBackend


class PostgresSQLBackend(SQLBackend):
    """PostgreSQL (and wire-compatible engines such as Amazon Redshift) backed by
    the async ``psycopg`` (v3) driver.

    Use one of the class methods to construct:
    - ``from_credentials`` — static host/port/user/password
    - ``from_iam_auth``    — AWS RDS/Aurora IAM authentication (token as password)
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
            host=p["host"], port=p["port"], user=p["user"],
            password=p["password"], database=p["database"], **connect_kwargs,
        )

    def sqlalchemy_url(self, driver: str | None = None) -> str:
        """Return a SQLAlchemy URL for this connection (for SQLAlchemy-based
        consumers). ``driver`` overrides the dialect+driver scheme. Not available
        for IAM auth, whose token cannot be embedded in a static URL."""
        from cloudrift.sql._url import build_sqlalchemy_url

        if self._iam:
            from cloudrift.core.exceptions import SQLAuthError

            raise SQLAuthError(
                "sqlalchemy_url() is unavailable for IAM auth (token is dynamic)."
            )
        scheme = driver or self._sa_scheme
        return build_sqlalchemy_url(
            scheme, host=self._host, port=self._port, user=self._user,
            password=self._password, database=self._database,
        )

    @classmethod
    def from_iam_auth(
        cls,
        host: str,
        port: int,
        user: str,
        database: str,
        region: str,
        **connect_kwargs,
    ) -> "PostgresSQLBackend":
        """Authenticate to AWS RDS/Aurora PostgreSQL using an IAM auth token.

        A short-lived (15 min) token is generated on every :meth:`connect` call
        and used in place of a password. IAM auth requires TLS, so ``sslmode``
        defaults to ``require`` unless overridden in ``connect_kwargs``.
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
        )

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

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

    async def connect(self, timeout: float | None = None):
        try:
            import psycopg
        except ImportError as e:  # pragma: no cover - import guard
            raise SQLConnectionError(
                "PostgreSQL support requires psycopg. Install cloudrift[sql-postgres]."
            ) from e

        password = await self._rds_token() if self._iam else self._password
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
                "password": self._password,
                "dbname": self._database,
                **self._connect_kwargs,
            }
            pool = AsyncConnectionPool(
                conninfo="",
                kwargs=kwargs,
                min_size=self._pool_min_size,
                max_size=self._pool_max_size,
                open=False,
            )
            await pool.open()
            self._pool = pool
        return self._pool

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
