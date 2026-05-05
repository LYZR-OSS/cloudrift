import pytest
from motor.motor_asyncio import AsyncIOMotorClient

from cloudrift.core.exceptions import DocumentConnectionError
from cloudrift.document import get_mongodb


class _RecordingClient:
    """Stand-in for AsyncIOMotorClient that records constructor args."""

    instances: list["_RecordingClient"] = []

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        _RecordingClient.instances.append(self)

    def close(self):
        pass


@pytest.fixture
def recorder(monkeypatch):
    _RecordingClient.instances.clear()
    import cloudrift.document.cosmos as cosmos_mod
    import cloudrift.document.documentdb as docdb_mod

    monkeypatch.setattr(docdb_mod, "AsyncIOMotorClient", _RecordingClient)
    monkeypatch.setattr(cosmos_mod, "AsyncIOMotorClient", _RecordingClient)
    return _RecordingClient


def test_documentdb_uri_returns_motor_client():
    # Real Motor client (no recorder) — verifies the dispatch returns the right type.
    client = get_mongodb(
        "documentdb",
        uri="mongodb://localhost:27017/",
        max_pool_size=50,
        min_pool_size=5,
    )
    assert isinstance(client, AsyncIOMotorClient)
    client.close()


def test_documentdb_uri_passes_pool_kwargs(recorder):
    get_mongodb(
        "documentdb",
        uri="mongodb://h:27017/",
        max_pool_size=200,
        min_pool_size=10,
        tls_ca_file="/etc/ssl/ca.pem",
    )
    inst = recorder.instances[-1]
    assert inst.args == ("mongodb://h:27017/",)
    assert inst.kwargs["maxPoolSize"] == 200
    assert inst.kwargs["minPoolSize"] == 10
    assert inst.kwargs["tlsCAFile"] == "/etc/ssl/ca.pem"


def test_documentdb_credentials_url_encodes_password(recorder):
    get_mongodb(
        "documentdb",
        host="cluster.docdb.amazonaws.com",
        port=27017,
        username="admin",
        password="p@ss/word",
        tls=True,
    )
    inst = recorder.instances[-1]
    uri = inst.args[0]
    # raw '@' / '/' in the password would corrupt the URI; quote_plus encodes them
    assert "p%40ss%2Fword" in uri
    assert uri.startswith("mongodb://admin:")
    assert "cluster.docdb.amazonaws.com:27017" in uri
    assert inst.kwargs["tls"] is True


def test_documentdb_tls_cert_passes_cert_path(recorder):
    get_mongodb(
        "documentdb",
        host="cluster.docdb.amazonaws.com",
        port=27017,
        username="admin",
        password="pw",
        tls_cert_key_file="/secrets/client.pem",
        tls_ca_file="/secrets/ca.pem",
    )
    inst = recorder.instances[-1]
    assert inst.kwargs["tls"] is True
    assert inst.kwargs["tlsCertificateKeyFile"] == "/secrets/client.pem"
    assert inst.kwargs["tlsCAFile"] == "/secrets/ca.pem"


def test_cosmos_account_key_builds_mongo_uri(recorder):
    get_mongodb("cosmos", account="myacct", account_key="raw+key/with=special")
    inst = recorder.instances[-1]
    uri = inst.args[0]
    assert uri.startswith("mongodb://myacct:")
    assert "myacct.mongo.cosmos.azure.com:10255" in uri
    assert "ssl=true" in uri
    assert "replicaSet=globaldb" in uri
    assert "retryWrites=false" in uri
    # the '+' / '/' / '=' in the key must be URL-encoded
    assert "raw%2Bkey%2Fwith%3Dspecial" in uri


def test_cosmos_connection_string_passed_through(recorder):
    cs = "mongodb://acct:key@acct.mongo.cosmos.azure.com:10255/?ssl=true"
    get_mongodb("cosmos", connection_string=cs)
    inst = recorder.instances[-1]
    assert inst.args == (cs,)


@pytest.mark.parametrize(
    "kwargs,expected_max,expected_min",
    [
        # defaults
        ({"uri": "mongodb://h/"}, 100, 0),
        ({"host": "h", "port": 27017, "username": "u", "password": "p"}, 100, 0),
        (
            {
                "host": "h", "port": 27017, "username": "u", "password": "p",
                "tls_cert_key_file": "/c.pem",
            },
            100, 0,
        ),
        # explicit
        ({"uri": "mongodb://h/", "max_pool_size": 250, "min_pool_size": 25}, 250, 25),
        (
            {"host": "h", "port": 27017, "username": "u", "password": "p",
             "max_pool_size": 250, "min_pool_size": 25},
            250, 25,
        ),
        (
            {"host": "h", "port": 27017, "username": "u", "password": "p",
             "tls_cert_key_file": "/c.pem",
             "max_pool_size": 250, "min_pool_size": 25},
            250, 25,
        ),
    ],
)
def test_documentdb_pool_kwargs_standardized(recorder, kwargs, expected_max, expected_min):
    get_mongodb("documentdb", **kwargs)
    inst = recorder.instances[-1]
    assert inst.kwargs["maxPoolSize"] == expected_max
    assert inst.kwargs["minPoolSize"] == expected_min


@pytest.mark.parametrize(
    "kwargs,expected_max,expected_min",
    [
        ({"connection_string": "mongodb://h/"}, 100, 0),
        ({"account": "a", "account_key": "k"}, 100, 0),
        (
            {"connection_string": "mongodb://h/",
             "max_pool_size": 250, "min_pool_size": 25},
            250, 25,
        ),
        (
            {"account": "a", "account_key": "k",
             "max_pool_size": 250, "min_pool_size": 25},
            250, 25,
        ),
    ],
)
def test_cosmos_pool_kwargs_standardized(recorder, kwargs, expected_max, expected_min):
    get_mongodb("cosmos", **kwargs)
    inst = recorder.instances[-1]
    assert inst.kwargs["maxPoolSize"] == expected_max
    assert inst.kwargs["minPoolSize"] == expected_min


def test_invalid_provider():
    with pytest.raises(ValueError, match="Unknown document DB provider"):
        get_mongodb("dynamodb", uri="x")


def test_documentdb_connect_failure_wrapped(monkeypatch):
    import cloudrift.document.documentdb as mod

    def boom(*args, **kwargs):
        raise RuntimeError("bad uri")

    monkeypatch.setattr(mod, "AsyncIOMotorClient", boom)
    with pytest.raises(DocumentConnectionError, match="Failed to connect to DocumentDB"):
        get_mongodb("documentdb", uri="mongodb://broken")


def test_cosmos_connect_failure_wrapped(monkeypatch):
    import cloudrift.document.cosmos as mod

    def boom(*args, **kwargs):
        raise RuntimeError("bad key")

    monkeypatch.setattr(mod, "AsyncIOMotorClient", boom)
    with pytest.raises(DocumentConnectionError, match="Failed to connect to Cosmos DB"):
        get_mongodb("cosmos", account="a", account_key="k")
