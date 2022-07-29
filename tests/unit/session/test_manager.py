"""
Tests for SessionManager class
"""
from unittest.mock import Mock, patch

import pytest
from loguru import logger
from pytest import LogCaptureFixture

from featurebyte.api.feature_store import FeatureStore
from featurebyte.config import Configurations
from featurebyte.session.manager import SessionManager


@pytest.fixture(autouse=True, name="caplog_handle")
def caplog_handle_fixture(caplog: LogCaptureFixture):
    """
    Log captured emitted output
    """
    handler_id = logger.add(caplog.handler, level="DEBUG", format="{message}")
    yield caplog
    logger.remove(handler_id)


@pytest.fixture(name="session_manager")
def session_manager_fixture(config, snowflake_connector):
    """
    Session manager fixture
    """
    # pylint: disable=no-member
    _ = snowflake_connector
    SessionManager.__getitem__.cache_clear()
    yield SessionManager(credentials=config.credentials)


@pytest.fixture(name="sqlite_feature_store")
def sqlite_feature_store_fixture(config, graph):
    """
    SQLite database source fixture
    """
    return FeatureStore(**config.feature_stores["sq_featurestore"].dict())


@patch("featurebyte.session.sqlite.os", Mock())
@patch("featurebyte.session.sqlite.sqlite3", Mock())
def test_session_manager__get_cached_properly(
    snowflake_feature_store,
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
    _ = session_manager[snowflake_feature_store]
    count, msg = count_create_session_logs()
    assert count == 1
    assert msg == "Create a new session for snowflake"

    # retrieve same data source for the second time, check that cached is used
    _ = session_manager[snowflake_feature_store]
    count, msg = count_create_session_logs()
    assert count == 1

    # retrieve different data source
    _ = session_manager[sqlite_feature_store]
    count, msg = count_create_session_logs()
    assert count == 2
    assert msg == "Create a new session for sqlite"

    # clear the cache & call again
    session_manager.__getitem__.cache_clear()
    _ = session_manager[snowflake_feature_store]
    count, msg = count_create_session_logs()
    assert count == 3
    assert msg == "Create a new session for snowflake"


def test_session_manager__wrong_configuration_file(snowflake_feature_store):
    """
    Test exception raised when wrong configuration file is used
    """
    config = Configurations("non/existent/path")
    session_manager = SessionManager(credentials=config.credentials)
    with pytest.raises(ValueError) as exc:
        _ = session_manager[snowflake_feature_store]
    assert "Credentials do not contain info for the database source" in str(exc.value)
