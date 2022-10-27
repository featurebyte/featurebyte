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


def pytest_configure(config):
    """Set up additional pytest markers"""
    # register an additional marker
    config.addinivalue_line("markers", "no_mock_process_store: mark test to not mock process store")


def pytest_addoption(parser):
    """Set up additional pytest options"""
    parser.addoption("--update-fixtures", action="store_true", default=False)


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
    with patch("motor.motor_asyncio.AsyncIOMotorClient.__new__") as mock_new:
        mongo_client = AsyncMongoMockClient()
        mock_new.return_value = mongo_client
        persistent = MongoDB(uri="mongodb://server.example.com:27017", database="test")

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
