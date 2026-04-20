import pytest
from mongomock_motor import AsyncMongoMockClient

from cloudrift.document import get_mongodb
from cloudrift.document.documentdb import AWSDocumentDBBackend


@pytest.fixture
def docdb(monkeypatch):
    """Patch AsyncIOMotorClient with mongomock_motor for unit tests."""
    import cloudrift.document.documentdb as mod

    monkeypatch.setattr(mod, "AsyncIOMotorClient", AsyncMongoMockClient)
    backend = get_mongodb("documentdb", uri="mongodb://localhost:27017", database="testdb")
    return backend


@pytest.mark.asyncio
async def test_insert_and_find_one(docdb):
    doc_id = await docdb.insert_one("users", {"name": "Alice", "age": 30})
    assert isinstance(doc_id, str)

    found = await docdb.find_one("users", {"name": "Alice"})
    assert found is not None
    assert found["name"] == "Alice"
    assert found["age"] == 30


@pytest.mark.asyncio
async def test_insert_many(docdb):
    ids = await docdb.insert_many("items", [{"v": 1}, {"v": 2}, {"v": 3}])
    assert len(ids) == 3


@pytest.mark.asyncio
async def test_find_with_limit(docdb):
    for i in range(5):
        await docdb.insert_one("logs", {"i": i})
    results = await docdb.find("logs", {}, limit=3)
    assert len(results) == 3


@pytest.mark.asyncio
async def test_update_one(docdb):
    await docdb.insert_one("products", {"sku": "ABC", "price": 10})
    modified = await docdb.update_one("products", {"sku": "ABC"}, {"$set": {"price": 20}})
    assert modified == 1
    doc = await docdb.find_one("products", {"sku": "ABC"})
    assert doc["price"] == 20


@pytest.mark.asyncio
async def test_delete_one(docdb):
    await docdb.insert_one("temp", {"key": "to_delete"})
    deleted = await docdb.delete_one("temp", {"key": "to_delete"})
    assert deleted == 1
    result = await docdb.find_one("temp", {"key": "to_delete"})
    assert result is None


@pytest.mark.asyncio
async def test_count(docdb):
    for _ in range(4):
        await docdb.insert_one("counters", {"x": 1})
    total = await docdb.count("counters", {"x": 1})
    assert total == 4


@pytest.mark.asyncio
async def test_find_one_missing_returns_none(docdb):
    result = await docdb.find_one("empty_col", {"id": "does-not-exist"})
    assert result is None


def test_invalid_provider():
    with pytest.raises(ValueError, match="Unknown document DB provider"):
        get_mongodb("dynamodb", uri="x", database="y")


# --- New tests for P0/P1 features ---


@pytest.mark.asyncio
async def test_find_iter(docdb):
    for i in range(5):
        await docdb.insert_one("stream_col", {"val": i})
    docs = []
    async for doc in docdb.find_iter("stream_col", {}):
        docs.append(doc)
    assert len(docs) == 5


@pytest.mark.asyncio
async def test_create_index(docdb):
    index_name = await docdb.create_index("indexed_col", [("name", 1)], unique=True)
    assert isinstance(index_name, str)


@pytest.mark.asyncio
async def test_aggregate(docdb):
    await docdb.insert_many("agg_col", [{"status": "a"}, {"status": "b"}, {"status": "a"}])
    results = await docdb.aggregate("agg_col", [{"$match": {"status": "a"}}])
    assert len(results) == 2
    assert all(r["status"] == "a" for r in results)


@pytest.mark.asyncio
async def test_upsert_one_insert(docdb):
    doc_id = await docdb.upsert_one("upsert_col", {"key": "new"}, {"$set": {"value": 42}})
    assert isinstance(doc_id, str)
    found = await docdb.find_one("upsert_col", {"key": "new"})
    assert found is not None
    assert found["value"] == 42


@pytest.mark.asyncio
async def test_upsert_one_update(docdb):
    await docdb.insert_one("upsert_col2", {"key": "existing", "value": 1})
    doc_id = await docdb.upsert_one("upsert_col2", {"key": "existing"}, {"$set": {"value": 99}})
    assert isinstance(doc_id, str)
    found = await docdb.find_one("upsert_col2", {"key": "existing"})
    assert found["value"] == 99
