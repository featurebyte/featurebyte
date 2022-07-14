"""
Test MongoDB persistent backend
"""
import os

import pymongo
import pytest
from bson.objectid import ObjectId

from featurebyte.persistent.mongo import MongoDB


@pytest.fixture()
def mongo_persistent():
    mongo_connection = os.getenv("MONGO_CONNECTION")
    print(mongo_connection)
    database_name = f"test_{ObjectId()}"
    client = pymongo.MongoClient(mongo_connection)
    persistent = MongoDB(uri=mongo_connection, database=database_name)
    yield persistent, client, client[database_name]
    client.drop_database(database_name)


@pytest.mark.skipif(
    os.getenv("MONGO_CONNECTION") is None, reason="MongoDB connection is not available."
)
def test_start_transaction__success(mongo_persistent):
    """
    Test start_transaction context manager
    """
    persistent, client, db = mongo_persistent
    col = "test_col"

    with persistent.start_transaction() as session:
        session.insert_one(collection_name=col, document={"key1": "value1"})
        session.insert_one(collection_name=col, document={"key2": "value2"})

    # check both records written to the mongodb
    output = sorted(db[col].find({}, {"_id": False}), key=lambda d: list(d.keys()))
    assert output == [{"key1": "value1"}, {"key2": "value2"}]


@pytest.mark.skipif(
    os.getenv("MONGO_CONNECTION") is None, reason="MongoDB connection is not available."
)
def test_start_transaction__with_exception_within_transaction(mongo_persistent):
    """
    Test start_transaction context manager
    """
    persistent, client, db = mongo_persistent
    col = "test_col"

    try:
        with persistent.start_transaction() as session:
            session.insert_one(collection_name=col, document={"key1": "value1"})
            session.insert_one(collection_name=col, document={"key2": "value2"})
            assert False
    except AssertionError:
        pass

    # check no record written to the mongodb
    output = list(db[col].find({}, {"_id": False}))
    assert output == []
