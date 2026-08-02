import pytest

psycopg = pytest.importorskip("psycopg")
pytest.importorskip("psycopg_pool")

from cloudrift.sql.postgresql import PostgresSQLBackend  # noqa: E402


class _RecordingPool:
    """Stand-in for psycopg_pool.AsyncConnectionPool that records constructor args."""

    instances: list["_RecordingPool"] = []

    def __init__(
        self,
        conninfo="",
        *,
        kwargs=None,
        min_size=0,
        max_size=10,
        open=False,
        connection_class=None,
    ):
        self.conninfo = conninfo
        self.kwargs = kwargs
        self.min_size = min_size
        self.max_size = max_size
        self.connection_class = connection_class
        _RecordingPool.instances.append(self)

    async def open(self):
        pass

    async def close(self):
        pass


@pytest.fixture
def pool_recorder(monkeypatch):
    _RecordingPool.instances.clear()
    import psycopg_pool

    monkeypatch.setattr(psycopg_pool, "AsyncConnectionPool", _RecordingPool)
    return _RecordingPool


async def test_iam_pool_mints_fresh_token_per_connection(pool_recorder, monkeypatch):
    backend = PostgresSQLBackend.from_iam_auth(
        host="db.cluster.us-east-1.rds.amazonaws.com",
        port=5432,
        user="svc",
        database="app",
        region="us-east-1",
        pool=True,
        pool_min_size=1,
        pool_max_size=5,
    )
    tokens = iter(["tok-1", "tok-2"])

    async def fake_token(self):
        return next(tokens)

    monkeypatch.setattr(PostgresSQLBackend, "_rds_token", fake_token)

    await backend._ensure_pool()
    rec = pool_recorder.instances[-1]
    # no static password — each connection authenticates with its own token
    assert "password" not in rec.kwargs
    assert rec.kwargs["host"] == "db.cluster.us-east-1.rds.amazonaws.com"
    assert rec.kwargs["user"] == "svc"
    assert rec.kwargs["sslmode"] == "require"
    assert rec.min_size == 1
    assert rec.max_size == 5
    conn_cls = rec.connection_class
    assert conn_cls is not None
    assert issubclass(conn_cls, psycopg.AsyncConnection)

    captured: list[dict] = []

    async def fake_connect(cls, conninfo="", **kw):
        captured.append(kw)
        return "conn"

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", classmethod(fake_connect))
    assert await conn_cls.connect(host="h") == "conn"
    assert await conn_cls.connect(host="h") == "conn"
    assert captured[0]["password"] == "tok-1"
    assert captured[1]["password"] == "tok-2"


async def test_non_iam_pool_passes_static_password(pool_recorder):
    backend = PostgresSQLBackend.from_credentials(
        host="h", port=5432, user="u", password="pw", database="d", pool=True
    )
    await backend._ensure_pool()
    rec = pool_recorder.instances[-1]
    assert rec.kwargs["password"] == "pw"
    assert rec.connection_class is None


async def test_from_iam_auth_without_pool_unchanged():
    backend = PostgresSQLBackend.from_iam_auth(
        host="h", port=5432, user="u", database="d", region="us-east-1"
    )
    assert backend._pool_enabled is False
    assert backend._iam is True
