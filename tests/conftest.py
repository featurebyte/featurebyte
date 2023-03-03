"""
Common fixture for both unit and integration tests
"""
from typing import AsyncIterator, Tuple

import os
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest
import pytest_asyncio
from mongomock_motor import AsyncMongoMockClient

from featurebyte.persistent.mongo import MongoDB


def pytest_addoption(parser):
    """Set up additional pytest options"""
    parser.addoption("--update-fixtures", action="store_true", default=False)
    parser.addoption("--source-types", type=str, default=None)


@pytest.fixture(scope="session")
def update_fixtures(pytestconfig):
    """Fixture corresponding to pytest --update-fixtures option"""
    return pytestconfig.getoption("update_fixtures")


@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    """Mask default config path to avoid unintentionally using a real configuration file"""
    with patch.dict(os.environ, {}):
        yield


@pytest.fixture(name="test_dir")
def test_directory_fixture():
    """Test directory"""
    path = os.path.dirname(os.path.abspath(__file__))
    return path


@pytest_asyncio.fixture(name="mongo_persistent")
async def mongo_persistent_fixture() -> Tuple[MongoDB, AsyncMongoMockClient]:
    """
    Patched MongoDB fixture for testing

    Returns
    -------
    Tuple[MongoDB, AsyncMongoMockClient]
        Patched MongoDB object and MongoClient
    """
    mongo_client = AsyncMongoMockClient()
    persistent = MongoDB(
        uri="mongodb://server.example.com:27017", database="test", client=mongo_client
    )

    # skip session in unit tests
    @asynccontextmanager
    async def start_transaction() -> AsyncIterator[MongoDB]:
        yield persistent

    with patch.object(persistent, "start_transaction", start_transaction):
        yield persistent, mongo_client


@pytest_asyncio.fixture(name="persistent")
async def persistent_fixture(mongo_persistent) -> MongoDB:
    """
    Patched persistent fixture for testing

    Returns
    -------
    MongoDB
    """
    persistent, _ = mongo_persistent
    yield persistent


@pytest.fixture(
    params=[
        [15, 25, 1, "2022-06-13T08:51:50.000Z", 27585172],
        [15, 25, 1, "2022-06-13T08:52:49.000Z", 27585172],
        [15, 25, 1, "2022-06-13T08:52:50.000Z", 27585173],
        [15, 25, 1, "2022-06-13 08:51:50", 27585172],
        [15, 25, 1, "2022-06-13 08:52:49", 27585172],
        [15, 25, 1, "2022-06-13 08:52:50", 27585173],
        [15, 100, 2, "2022-06-13T09:24:35.000Z", 13792603],
        [15, 100, 2, "2022-06-13T09:25:35.000Z", 13792603],
        [15, 100, 2, "2022-06-13T09:26:34.000Z", 13792603],
        [15, 100, 2, "2022-06-13T09:26:35.000Z", 13792604],
    ],
)
def timestamp_to_index_fixture(request):
    """
    Parameterized fixture for timestamp to index conversion
    """
    return request.param


@pytest.fixture(
    params=[
        [27585172, 15, 25, 1, "2022-06-13T08:51:50.000Z"],
        [27585173, 15, 25, 1, "2022-06-13T08:52:50.000Z"],
        [13792603, 15, 100, 2, "2022-06-13T09:24:35.000Z"],
        [13792604, 15, 100, 2, "2022-06-13T09:26:35.000Z"],
    ],
)
def index_to_timestamp_fixture(request):
    """
    Parameterized fixture for index to timestamp conversion
    """
    return request.param
