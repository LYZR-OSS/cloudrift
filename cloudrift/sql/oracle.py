import asyncio

from cloudrift.core.exceptions import SQLConnectionError
from cloudrift.sql.base import SQLBackend


class OracleSQLBackend(SQLBackend):
    """Oracle Database backed by the ``oracledb`` driver (thin mode).

    ``oracledb`` is synchronous, so connections are opened in a worker thread to
    keep the API non-blocking. Use :meth:`from_credentials` to construct.
    """

    dialect = "oracle"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str,
        password: str,
        service_name: str | None = None,
        sid: str | None = None,
        protocol: str = "tcp",
        wallet_path: str | None = None,
        wallet_password: str | None = None,
        connection_kwargs: dict | None = None,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._username = username
        self._password = password
        self._service_name = service_name
        self._sid = sid
        self._protocol = protocol
        self._wallet_path = wallet_path
        self._wallet_password = wallet_password
        self._connection_kwargs = connection_kwargs or {}

    @classmethod
    def from_credentials(
        cls,
        host: str,
        username: str,
        password: str,
        port: int = 1521,
        service_name: str | None = None,
        sid: str | None = None,
        protocol: str = "tcp",
        wallet_path: str | None = None,
        wallet_password: str | None = None,
        connection_kwargs: dict | None = None,
    ) -> "OracleSQLBackend":
        """Authenticate with username/password. Provide exactly one of
        ``service_name`` or ``sid``. ``wallet_path`` enables thin-mode mTLS
        wallets (TCPS)."""
        return cls(
            host=host,
            port=port,
            username=username,
            password=password,
            service_name=service_name,
            sid=sid,
            protocol=protocol,
            wallet_path=wallet_path,
            wallet_password=wallet_password,
            connection_kwargs=connection_kwargs,
        )

    def _build_params(self):
        import oracledb

        params = oracledb.ConnectParams(
            host=self._host,
            port=self._port,
            user=self._username,
            password=self._password,
            service_name=self._service_name,
            sid=self._sid,
            protocol=self._protocol,
        )
        if self._wallet_path:
            params.wallet_location = self._wallet_path
            params.config_dir = self._wallet_path
            if self._wallet_password:
                params.wallet_password = self._wallet_password
        for key, value in self._connection_kwargs.items():
            setattr(params, key, value)
        return params

    async def connect(self, timeout: float | None = None):
        try:
            import oracledb
        except ImportError as e:  # pragma: no cover - import guard
            raise SQLConnectionError(
                "Oracle support requires oracledb. Install cloudrift[sql-oracle]."
            ) from e

        params = self._build_params()
        connect_coro = asyncio.to_thread(
            oracledb.connect, params=params, mode=oracledb.AUTH_MODE_DEFAULT
        )
        try:
            if timeout is not None:
                return await asyncio.wait_for(connect_coro, timeout=timeout)
            return await connect_coro
        except asyncio.TimeoutError as e:
            raise SQLConnectionError(
                f"Timed out connecting to Oracle after {timeout} seconds."
            ) from e
        except Exception as e:
            raise SQLConnectionError(f"Failed to connect to Oracle: {e}") from e
