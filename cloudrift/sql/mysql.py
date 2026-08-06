import asyncio

from cloudrift.core.exceptions import SQLAuthError, SQLConnectionError
from cloudrift.sql.base import SQLBackend


class MySQLSQLBackend(SQLBackend):
    """MySQL (and wire-compatible engines such as Amazon Aurora MySQL) backed by
    the async ``mysql.connector.aio`` driver.

    Use one of the class methods to construct:
    - ``from_credentials``   — static host/port/user/password
    - ``from_iam_auth``      — AWS RDS/Aurora IAM authentication (token as password)
    - ``from_gcp_iam_auth``  — Cloud SQL IAM authentication (token as password)
    """

    dialect = "mysql"
    # Default SQLAlchemy scheme for sqlalchemy_url() — async aiomysql driver.
    _sa_scheme = "mysql+aiomysql"

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
        gcp_iam: bool = False,
        gcp_credentials: dict | None = None,
        connect_kwargs: dict | None = None,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._user = user
        self._database = database
        self._password = password
        self._iam = iam
        self._region = region
        self._gcp_iam = gcp_iam
        # Raw from_gcp_iam_auth kwargs (service_account_file/info, prefer_metadata).
        # The built google.auth Credentials object is cached separately, lazily,
        # on first _auth_password() call — see that method.
        self._gcp_iam_kwargs = gcp_credentials or {}
        self._gcp_credentials = None
        self._connect_kwargs = connect_kwargs or {}

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
        **connect_kwargs,
    ) -> "MySQLSQLBackend":
        """Authenticate with a static username/password."""
        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_kwargs=connect_kwargs,
        )

    @classmethod
    def from_url(cls, url: str, **connect_kwargs) -> "MySQLSQLBackend":
        """Authenticate from a connection URL, e.g.
        ``mysql://user:pass@host:3306/db`` (scheme is ignored). ``database`` may
        be omitted."""
        from cloudrift.sql._url import parse_sql_url

        p = parse_sql_url(url, default_port=3306)
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

        if self._iam or self._gcp_iam:
            from cloudrift.core.exceptions import SQLAuthError

            raise SQLAuthError("sqlalchemy_url() is unavailable for IAM auth (token is dynamic).")
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
        **connect_kwargs,
    ) -> "MySQLSQLBackend":
        """Authenticate to AWS RDS/Aurora MySQL using an IAM auth token.

        A short-lived token is generated on every :meth:`connect` call and used
        as the password. IAM auth requires TLS; configure ``ssl_ca`` / ``ssl_*``
        via ``connect_kwargs`` as your deployment requires.
        """
        return cls(
            host=host,
            port=port,
            user=user,
            database=database,
            iam=True,
            region=region,
            connect_kwargs=connect_kwargs,
        )

    @classmethod
    def from_gcp_iam_auth(
        cls,
        host: str,
        user: str,
        database: str,
        port: int = 3306,
        service_account_file: str | None = None,
        service_account_info: dict | None = None,
        prefer_metadata: bool = False,
        **connect_kwargs,
    ) -> "MySQLSQLBackend":
        """Authenticate to Cloud SQL for MySQL with an IAM token.

        The credential is built on first use and cached; its access token is
        used as the password and refreshed automatically only once it actually
        expires — never rebuilt or re-refreshed on every :meth:`connect`. No
        database password is ever stored. Requires IAM database authentication
        enabled on the instance and the principal added as a database user with
        ``roles/cloudsql.instanceUser``.

        ``user`` is the IAM principal's database username. MySQL usernames are
        capped at 32 characters, so GCP uses only the **local part** of a service
        account email (``sa-name`` from ``sa-name@project.iam.gserviceaccount.com``)
        — a different rule from PostgreSQL. cloudrift does not transform it; pass
        exactly what the instance's user list shows.

        IAM auth requires TLS; configure ``ssl_ca`` / ``ssl_*`` via
        ``connect_kwargs`` as your deployment requires.

        Args:
            host: Instance IP or the Cloud SQL Auth Proxy address.
            user: Database username for the IAM principal (see above).
            database: Database name.
            port: Port (default 3306).
            service_account_file: Path to a service-account JSON key file.
            service_account_info: Parsed service-account JSON.
            prefer_metadata: Read the attached service account from the metadata
                server — see :mod:`cloudrift.core.gcp_credentials`.
            **connect_kwargs: Extra mysql-connector arguments.
        """
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

    async def _rds_token(self) -> str:
        try:
            import boto3
        except ImportError as e:  # pragma: no cover - import guard
            raise SQLAuthError(
                "RDS IAM auth requires boto3. Install cloudrift[sql-mysql] or boto3."
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
        """Resolve the password for this connection, minting a token if needed."""
        if self._iam:
            return await self._rds_token()
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
            from mysql.connector.aio import connect as aio_connect
        except ImportError as e:  # pragma: no cover - import guard
            raise SQLConnectionError(
                "MySQL support requires mysql-connector-python. Install cloudrift[sql-mysql]."
            ) from e

        password = await self._auth_password()
        kwargs = dict(self._connect_kwargs)
        if timeout is not None:
            kwargs["connection_timeout"] = int(timeout)
        try:
            return await aio_connect(
                host=self._host,
                port=self._port,
                user=self._user,
                password=password,
                database=self._database,
                **kwargs,
            )
        except Exception as e:
            raise SQLConnectionError(
                f"Failed to connect to MySQL at {self._host}:{self._port}: {e}"
            ) from e
