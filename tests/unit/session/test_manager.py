"""
Tests for SessionManager class
"""
import os
import tempfile
from unittest.mock import Mock, patch

import pytest
from loguru import logger
from pytest import LogCaptureFixture

from featurebyte.api.feature_store import FeatureStore
from featurebyte.config import Configurations
from featurebyte.query_graph.node.schema import SQLiteDetails
from featurebyte.session.manager import SessionManager, session_cache


@pytest.fixture(autouse=True, name="caplog_handle")
def caplog_handle_fixture(caplog: LogCaptureFixture):
    """
    Log captured emitted output
    """
    handler_id = logger.add(caplog.handler, level="DEBUG", format="{message}")
    yield caplog
    try:
        logger.remove(handler_id)
    except ValueError:
        pass


@pytest.fixture(name="session_manager")
def session_manager_fixture(config, snowflake_connector):
    """
    Session manager fixture
    """
    # pylint: disable=no-member
    _ = snowflake_connector
    session_cache.clear()
    yield SessionManager(credentials=config.credentials)


@pytest.fixture(name="sqlite_feature_store")
def sqlite_feature_store_fixture(config):
    """
    SQLite database source fixture
    """
    return FeatureStore(
        name="sq_featurestore",
        type="sqlite",
        details=SQLiteDetails(filename="some_filename.sqlite"),
    )


@patch("featurebyte.session.sqlite.os", Mock())
@patch("featurebyte.session.sqlite.sqlite3", Mock())
@pytest.mark.asyncio
async def test_session_manager__get_cached_properly(
    snowflake_feature_store_params,
    sqlite_feature_store,
    snowflake_execute_query,
    session_manager,
    caplog_handle,
):
    """
    Test session manager get cached properly
    """
    _ = snowflake_execute_query
    # check no record emit
    assert caplog_handle.records == []

    def count_create_session_logs():
        total = 0
        latest_message = None
        for record in caplog_handle.records:
            if record.msg.startswith("Create a new session for"):
                total += 1
                latest_message = record.msg
        return total, latest_message

    # retrieve data source session for the first time
    snowflake_feature_store = FeatureStore(**snowflake_feature_store_params, type="snowflake")
    _ = await session_manager.get_session(snowflake_feature_store)
    count, msg = count_create_session_logs()
    assert count == 1
    assert msg == "Create a new session for snowflake"

    # retrieve same data source for the second time, check that cached is used
    _ = await session_manager.get_session(snowflake_feature_store)
    count, msg = count_create_session_logs()
    assert count == 1

    # retrieve different data source
    _ = await session_manager.get_session(sqlite_feature_store)
    count, msg = count_create_session_logs()
    assert count == 2
    assert msg == "Create a new session for sqlite"

    # clear the cache & call again
    session_cache.clear()
    _ = await session_manager.get_session(snowflake_feature_store)
    count, msg = count_create_session_logs()
    assert count == 3
    assert msg == "Create a new session for snowflake"


@pytest.mark.asyncio
async def test_session_manager__wrong_configuration_file(snowflake_feature_store):
    """
    Test exception raised when wrong configuration file is used
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        non_existent_path = os.path.join(temp_dir, "non", "existent", "path")
        config = Configurations(non_existent_path)
        session_manager = SessionManager(credentials=config.credentials)
        with pytest.raises(ValueError) as exc:
            _ = await session_manager.get_session(snowflake_feature_store)
        assert 'Credentials do not contain info for the feature store "sf_featurestore"' in str(
            exc.value
        )
