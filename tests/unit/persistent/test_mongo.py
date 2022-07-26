"""
Test MongoDB persistent backend
"""
from __future__ import annotations

from typing import Tuple

from unittest.mock import patch

import pytest
from bson import ObjectId
from mongomock_motor import AsyncMongoMockClient
from pymongo.errors import DuplicateKeyError

from featurebyte.persistent import DuplicateDocumentError
from featurebyte.persistent.mongo import MongoDB


@pytest.fixture(name="mongo_persistent")
def mongo_persistent_fixture() -> Tuple[MongoDB, AsyncMongoMockClient]:
    """
    Patched MongoDB fixture for testing

    Returns
    -------
    Tuple[MongoDB, AsyncMongoMockClient]
        Patched MongoDB object and MongoClient
    """
    with patch("motor.motor_asyncio.AsyncIOMotorClient.__new__") as mock_new:
        mongo_client = AsyncMongoMockClient()
        mock_new.return_value = mongo_client
        persistent = MongoDB(uri="mongodb://server.example.com:27017", database="test")
        return persistent, mongo_client


@pytest.mark.asyncio
async def test_insert_one(mongo_persistent, test_document):
    """
    Test inserting one document
    """
    persistent, client = mongo_persistent
    inserted_id = await persistent.insert_one(collection_name="data", document=test_document)
    # check document is inserted
    results = await client["test"]["data"].find({}).to_list()
    assert results[0] == test_document
    assert results[0]["_id"] == inserted_id


@pytest.mark.asyncio
async def test_insert_one__duplicate_key__(mongo_persistent, test_document):
    """
    Test inserting one document
    """
    persistent, _ = mongo_persistent
    with pytest.raises(DuplicateDocumentError):
        with patch("mongomock_motor.AsyncMongoMockCollection.insert_one") as mock_insert:
            mock_insert.side_effect = DuplicateKeyError("Document exists")
            await persistent.insert_one(collection_name="data", document=test_document)


@pytest.mark.asyncio
async def test_insert_many(mongo_persistent, test_documents):
    """
    Test inserting many documents
    """
    persistent, client = mongo_persistent
    inserted_ids = await persistent.insert_many(collection_name="data", documents=test_documents)
    # check documents are inserted
    assert await client["test"]["data"].find({}).to_list() == test_documents
    assert [doc["_id"] for doc in test_documents] == inserted_ids


@pytest.mark.asyncio
async def test_insert_many__duplicate_key__(mongo_persistent, test_documents):
    """
    Test inserting many documents
    """
    persistent, _ = mongo_persistent
    with pytest.raises(DuplicateDocumentError):
        with patch("mongomock_motor.AsyncMongoMockCollection.insert_many") as mock_insert:
            mock_insert.side_effect = DuplicateKeyError("Document exists")
            await persistent.insert_many(collection_name="data", documents=test_documents)


@pytest.mark.asyncio
async def test_find_one(mongo_persistent, test_documents):
    """
    Test finding one document
    """
    persistent, client = mongo_persistent
    await client["test"]["data"].insert_many(test_documents)
    doc = await persistent.find_one(collection_name="data", query_filter={})
    assert doc == test_documents[0]


@pytest.mark.asyncio
async def test_find_many(mongo_persistent, test_documents):
    """
    Test finding many documents
    """
    persistent, client = mongo_persistent
    await client["test"]["data"].insert_many(test_documents)
    docs, total = await persistent.find(collection_name="data", query_filter={})
    assert docs == test_documents
    assert total == 3

    # test pagination
    docs, total = await persistent.find(
        collection_name="data", query_filter={}, page_size=2, page=1
    )
    assert docs == test_documents[:2]
    assert total == 3
    docs, total = await persistent.find(
        collection_name="data", query_filter={}, page_size=2, page=2
    )
    assert docs == test_documents[2:]
    assert total == 3
    docs, total = await persistent.find(
        collection_name="data", query_filter={}, page_size=0, page=2
    )
    assert docs == test_documents
    assert total == 3

    # test sort
    docs, total = await persistent.find(
        collection_name="data", query_filter={}, sort_by="_id", sort_dir="desc"
    )
    assert docs == test_documents[-1::-1]
    assert total == 3


@pytest.mark.asyncio
async def test_update_one(mongo_persistent, test_document, test_documents):
    """
    Test updating one document
    """
    persistent, client = mongo_persistent
    test_documents = [{**test_document, **{"_id": ObjectId()}} for _ in range(3)]
    await client["test"]["data"].insert_many(test_documents)
    result = await persistent.update_one(
        collection_name="data", query_filter={}, update={"$set": {"value": 1}}
    )

    assert result == 1
    results = await client["test"]["data"].find({}).to_list()

    # only first document should be updated
    assert results[0]["value"] == 1
    assert results[1] == test_documents[1]
    assert results[2] == test_documents[2]


@pytest.mark.asyncio
async def test_update_many(mongo_persistent, test_documents):
    """
    Test updating one document
    """
    persistent, client = mongo_persistent
    await client["test"]["data"].insert_many(test_documents)
    result = await persistent.update_many(
        collection_name="data", query_filter={}, update={"$set": {"value": 1}}
    )
    # expect all documents to be updated
    assert result == 3
    results = client["test"]["data"].find({})
    async for result in results:
        assert result["value"] == 1


@pytest.mark.asyncio
async def test_replace_one(mongo_persistent, test_document, test_documents):
    """
    Test replacing one document
    """
    persistent, client = mongo_persistent
    test_documents = [{**test_document, **{"_id": ObjectId()}} for _ in range(3)]
    await client["test"]["data"].insert_many(test_documents)
    result = await persistent.replace_one(
        collection_name="data", query_filter={}, replacement={"value": 1}
    )

    assert result == 1
    results = await client["test"]["data"].find({}).to_list()

    # only first document should be updated
    assert results[0] == {"_id": test_documents[0]["_id"], "value": 1}
    assert results[1] == test_documents[1]
    assert results[2] == test_documents[2]


@pytest.mark.asyncio
async def test_delete_one(mongo_persistent, test_documents):
    """
    Test deleting one document
    """
    persistent, client = mongo_persistent
    await client["test"]["data"].insert_many(test_documents)
    result = await persistent.delete_one(collection_name="data", query_filter={})
    # expect only one document to be deleted
    assert result == 1
    results = await client["test"]["data"].find({}).to_list()
    assert len(results) == 2


@pytest.mark.asyncio
async def test_delete_many(mongo_persistent, test_documents):
    """
    Test deleting many documents
    """
    persistent, client = mongo_persistent
    await client["test"]["data"].insert_many(test_documents)
    result = await persistent.delete_many(collection_name="data", query_filter={})
    # expect all documents to be deleted
    assert result == 3
    results = await client["test"]["data"].find({}).to_list()
    assert len(results) == 0
