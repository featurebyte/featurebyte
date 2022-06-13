"""
Test MongoDB storage backend
"""
from typing import Any, Dict, List, Tuple

import mongomock
import pymongo
import pytest
from bson import ObjectId

from featurebyte.storage.mongo import MongoStorage


@pytest.fixture(name="mongo_storage")
def mongo_storage_fixture() -> Tuple[MongoStorage, pymongo.MongoClient]:
    """
    Patched MongoStorage fixture for testing

    Returns
    -------
    Tuple[MongoStorage, pymongo.MongoClient]
        Patched MongoStorage object and MongoClient
    """
    with mongomock.patch(servers=(("server.example.com", 27017),)):
        storage = MongoStorage(uri="mongodb://server.example.com:27017", database="test")
        mongo_client = pymongo.MongoClient("mongodb://server.example.com:27017")
        return storage, mongo_client


@pytest.fixture(name="test_document")
def test_document_fixture() -> Dict[str, Any]:
    """
    Test document to be used for testing

    Returns
    -------
    Dict[str, Any]
        Document for testing
    """
    return {
        "id": ObjectId(),
        "name": "Generic Document",
        "value": [
            {
                "key1": "value1",
                "key2": "value2",
            }
        ],
    }


@pytest.fixture(name="test_documents")
def test_documents_fixture(test_document) -> List[Dict[str, Any]]:
    """
    Test documents to be used for testing

    Returns
    -------
    List[Dict[str, Any]]
        Document for testing
    """
    return [{**test_document, **{"id": ObjectId()}} for _ in range(3)]


def test_insert_one(mongo_storage, test_document):
    """
    Test inserting one document
    """
    storage, client = mongo_storage
    storage.insert_one(collection_name="data", document=test_document)
    # check document is inserted
    results = list(client["test"]["data"].find({}))
    assert results[0] == test_document


def test_insert_many(mongo_storage, test_documents):
    """
    Test inserting many documents
    """
    storage, client = mongo_storage
    storage.insert_many(collection_name="data", documents=test_documents)
    # check documents are inserted
    assert list(client["test"]["data"].find({})) == test_documents


def test_find_one(mongo_storage, test_documents):
    """
    Test finding one document
    """
    storage, client = mongo_storage
    client["test"]["data"].insert_many(test_documents)
    doc = storage.find_one(collection_name="data", filter={})
    assert doc == test_documents[0]


def test_find_many(mongo_storage, test_documents):
    """
    Test finding many documents
    """
    storage, client = mongo_storage
    client["test"]["data"].insert_many(test_documents)
    docs = list(storage.find(collection_name="data", filter={}))
    assert docs == test_documents


def test_update_one(mongo_storage, test_document, test_documents):
    """
    Test updating one document
    """
    storage, client = mongo_storage
    test_documents = [{**test_document, **{"id": ObjectId()}} for _ in range(3)]
    client["test"]["data"].insert_many(test_documents)
    result = storage.update_one(collection_name="data", filter={}, update={"$set": {"value": 1}})

    assert result == 1
    results = list(client["test"]["data"].find({}))

    # only first document should be updated
    assert results[0]["value"] == 1
    assert results[1]["value"] == test_document["value"]
    assert results[2]["value"] == test_document["value"]


def test_update_many(mongo_storage, test_documents):
    """
    Test updating one document
    """
    storage, client = mongo_storage
    client["test"]["data"].insert_many(test_documents)
    result = storage.update_many(collection_name="data", filter={}, update={"$set": {"value": 1}})
    # expect all documents to be updated
    assert result == 3
    results = client["test"]["data"].find({})
    for result in results:
        assert result["value"] == 1


def test_delete_one(mongo_storage, test_documents):
    """
    Test deleting one document
    """
    storage, client = mongo_storage
    client["test"]["data"].insert_many(test_documents)
    result = storage.delete_one(collection_name="data", filter={})
    # expect only one document to be deleted
    assert result == 1
    results = list(client["test"]["data"].find({}))
    assert len(results) == 2


def test_delete_many(mongo_storage, test_documents):
    """
    Test deleting many documents
    """
    storage, client = mongo_storage
    client["test"]["data"].insert_many(test_documents)
    result = storage.delete_many(collection_name="data", filter={})
    # expect all documents to be deleted
    assert result == 3
    results = list(client["test"]["data"].find({}))
    assert len(results) == 0
