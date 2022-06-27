"""
Tests for SessionManager class
"""
from unittest.mock import Mock, patch

import pytest
from loguru import logger
from pytest import LogCaptureFixture

from featurebyte.api.database_source import DatabaseSource
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
    # pylint: disable=E1101
    _ = snowflake_connector
    SessionManager.__getitem__.cache_clear()
    yield SessionManager(credentials=config.credentials)


@pytest.fixture(name="sqlite_database_source")
def sqlite_database_source_fixture(config, graph):
    """
    SQLite database source fixture
    """
    _ = graph
    return DatabaseSource(**config.db_sources["sq_datasource"].dict())


@patch("featurebyte.session.sqlite.os", Mock())
@patch("featurebyte.session.sqlite.sqlite3", Mock())
def test_session_manager__get_cached_properly(
    snowflake_database_source, sqlite_database_source, session_manager, caplog_handle
):
    """
    Test session manager get cached properly
    """
    # check no record emit
    assert caplog_handle.records == []

    # retrieve data source session for the first time
    _ = session_manager[snowflake_database_source]
    assert len(caplog_handle.records) == 1
    assert caplog_handle.records[0].msg == "Create a new session for snowflake"

    # retrieve same data source for the second time, check that cached is used
    _ = session_manager[snowflake_database_source]
    assert len(caplog_handle.records) == 1

    # retrieve different data source
    _ = session_manager[sqlite_database_source]
    assert len(caplog_handle.records) == 2
    assert caplog_handle.records[1].msg == "Create a new session for sqlite"

    # clear the cache & call again
    session_manager.__getitem__.cache_clear()
    _ = session_manager[snowflake_database_source]
    assert len(caplog_handle.records) == 3
    assert caplog_handle.records[2].msg == "Create a new session for snowflake"


def test_session_manager__wrong_configuration_file(snowflake_database_source):
    """
    Test exception raised when wrong configuration file is used
    """
    config = Configurations()
    session_manager = SessionManager(credentials=config.credentials)
    with pytest.raises(ValueError) as exc:
        _ = session_manager[snowflake_database_source]
    assert "Credentials do not contain info for the database source" in str(exc.value)
