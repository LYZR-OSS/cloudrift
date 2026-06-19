import asyncio
import inspect
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any


class SQLBackend(ABC):
    """Abstract base class for relational-SQL connection backends.

    cloudrift's SQL layer abstracts **connection construction and cloud
    authentication**, not query execution. Unlike a Mongo/Redis backend, there
    is no single wire protocol across SQL engines, so this layer does *not* wrap
    queries — it hands the caller a fully-authenticated **native driver
    connection** and the caller uses that driver's own cursor/execute API.

    Why ``connect()`` returns a *fresh* connection each call:
      - For static-credential engines (plain user/password) the caller typically
        opens one connection and reuses it.
      - For token-auth engines (Azure Entra / AAD, AWS RDS IAM) the access token
        is short-lived, so a fresh token must be acquired per connection. Calling
        ``connect()`` again transparently mints a new token. This makes the
        "open a new connection per query" pattern safe and is why token freshness
        is the backend's responsibility, not the caller's.

    The concrete native type returned by ``connect()`` depends on ``dialect``:
      - ``postgresql`` / ``redshift`` → ``psycopg.AsyncConnection``
      - ``mysql``                     → ``mysql.connector.aio`` connection
      - ``mssql``                     → ``aioodbc`` connection
      - ``oracle``                    → ``oracledb.Connection`` (thread-backed)
      - ``databricks``                → ``databricks.sql`` connection (thread-backed)

    Construct via the ``from_*`` classmethods (or the :func:`cloudrift.sql.get_sql`
    factory), never the initializer directly.
    """

    #: Engine family identifier — see class docstring for the mapping.
    dialect: str = "sql"

    @abstractmethod
    async def connect(self, timeout: float | None = None) -> Any:
        """Open and return a fresh, authenticated native async connection.

        Args:
            timeout: Optional connection timeout in seconds. Applied using the
                native driver's own timeout mechanism.

        Raises:
            SQLConnectionError: The connection could not be established.
            SQLAuthError: A required credential/token could not be acquired.
        """

    @asynccontextmanager
    async def acquire(self, timeout: float | None = None):
        """Lease a connection for the duration of the ``async with`` block.

        Uniform across backends: when the backend has pooling enabled (see the
        Postgres/MSSQL ``pool=True`` options) a pooled connection is leased and
        returned to the pool on exit; otherwise a fresh connection is opened and
        closed on exit. Use this instead of ``connect()`` when you want the
        connection lifecycle managed for you::

            async with backend.acquire(timeout=10) as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
        """
        conn = await self.connect(timeout)
        try:
            yield conn
        finally:
            await self._aclose_connection(conn)

    @staticmethod
    async def _aclose_connection(conn: Any) -> None:
        """Close a native connection, awaiting async ``close()`` and offloading a
        blocking (sync-driver) ``close()`` to a worker thread."""
        close = getattr(conn, "close", None)
        if close is None:
            return
        if inspect.iscoroutinefunction(close):
            await close()
        else:
            await asyncio.to_thread(close)

    async def close(self) -> None:
        """Release backend-held resources (credential clients, token caches,
        connection pools).

        Does NOT close connections handed out by :meth:`connect` — the caller
        owns those. Default is a no-op; backends that hold a credential client or
        pool override this.
        """

    async def __aenter__(self) -> "SQLBackend":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()
