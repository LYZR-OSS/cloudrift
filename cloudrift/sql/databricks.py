import asyncio

from cloudrift.core.exceptions import SQLConnectionError
from cloudrift.sql.base import SQLBackend


class DatabricksSQLBackend(SQLBackend):
    """Databricks SQL warehouse backed by the ``databricks-sql-connector`` driver.

    The driver is synchronous, so connections are opened in a worker thread.
    Use :meth:`from_token` to construct (personal access token / OAuth token).
    """

    dialect = "databricks"

    def __init__(
        self,
        *,
        host: str,
        http_path: str,
        token: str,
        port: int = 443,
        catalog: str | None = None,
        schema: str | None = None,
        connection_kwargs: dict | None = None,
    ) -> None:
        self._host = host
        self._http_path = http_path
        self._token = token
        self._port = int(port)
        self._catalog = catalog
        self._schema = schema
        self._connection_kwargs = connection_kwargs or {}

    @classmethod
    def from_token(
        cls,
        host: str,
        http_path: str,
        token: str,
        port: int = 443,
        catalog: str | None = None,
        schema: str | None = None,
        connection_kwargs: dict | None = None,
    ) -> "DatabricksSQLBackend":
        """Authenticate with an access token (PAT or OAuth)."""
        return cls(
            host=host,
            http_path=http_path,
            token=token,
            port=port,
            catalog=catalog,
            schema=schema,
            connection_kwargs=connection_kwargs,
        )

    async def connect(self, timeout: float | None = None):
        try:
            import databricks.sql as databricks_sql
        except ImportError as e:  # pragma: no cover - import guard
            raise SQLConnectionError(
                "Databricks support requires databricks-sql-connector. "
                "Install cloudrift[sql-databricks]."
            ) from e

        server_hostname = self._host if self._port == 443 else f"{self._host}:{self._port}"
        connect_coro = asyncio.to_thread(
            databricks_sql.connect,
            server_hostname=server_hostname,
            http_path=self._http_path,
            access_token=self._token,
            catalog=self._catalog,
            schema=self._schema,
            **self._connection_kwargs,
        )
        try:
            if timeout is not None:
                return await asyncio.wait_for(connect_coro, timeout=timeout)
            return await connect_coro
        except asyncio.TimeoutError as e:
            raise SQLConnectionError(
                f"Timed out connecting to Databricks after {timeout} seconds."
            ) from e
        except Exception as e:
            raise SQLConnectionError(f"Failed to connect to Databricks: {e}") from e
