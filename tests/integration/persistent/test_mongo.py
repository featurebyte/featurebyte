"""
Test MongoDB persistent backend
"""
import os

import pymongo
import pytest
from bson.objectid import ObjectId

from featurebyte.persistent.mongo import MongoDB


@pytest.fixture(name="mongo_persistent")
def mongo_persistent_fixture():
    """
    Mongo persistent fixture
    """
    mongo_connection = os.getenv("MONGO_CONNECTION")
    database_name = f"test_{ObjectId()}"
    client = pymongo.MongoClient(mongo_connection)
    persistent = MongoDB(uri=mongo_connection, database=database_name)
    yield persistent, client[database_name]
    client.drop_database(database_name)


@pytest.mark.skipif(
    os.getenv("MONGO_CONNECTION") is None, reason="MongoDB connection is not available."
)
@pytest.mark.asyncio
async def test_start_transaction__success(mongo_persistent):
    """
    Test start_transaction context manager
    """
    persistent, database = mongo_persistent
    col = "test_col"

    async with persistent.start_transaction() as session:
        await session.insert_one(collection_name=col, document={"key1": "value1"})
        await session.insert_one(collection_name=col, document={"key2": "value2"})

    # check both records written to the mongodb
    output = sorted(database[col].find({}, {"_id": False}), key=lambda d: list(d.keys()))
    assert output == [{"key1": "value1"}, {"key2": "value2"}]


@pytest.mark.skipif(
    os.getenv("MONGO_CONNECTION") is None, reason="MongoDB connection is not available."
)
@pytest.mark.asyncio
async def test_start_transaction__exception_within_transaction(mongo_persistent):
    """
    Test start_transaction context manager
    """
    persistent, database = mongo_persistent
    col = "test_col"

    with pytest.raises(AssertionError):
        async with persistent.start_transaction() as session:
            await session.insert_one(collection_name=col, document={"key1": "value1"})
            await session.insert_one(collection_name=col, document={"key2": "value2"})
            assert False

    # check no record written to the mongodb
    output = list(database[col].find({}, {"_id": False}))
    assert not output
