"""
Tests for SessionManager class
"""
from unittest.mock import Mock, patch

import pytest
from loguru import logger
from pytest import LogCaptureFixture

from featurebyte.session.manager import SessionManager


@pytest.fixture(autouse=True)
def caplog(caplog: LogCaptureFixture):
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
    _ = snowflake_connector
    SessionManager.__getitem__.cache_clear()
    yield SessionManager(credentials=config.credentials)


@patch("featurebyte.session.sqlite.os", Mock())
@patch("featurebyte.session.sqlite.sqlite3", Mock())
def test_session_manager__get_cached_properly(
    snowflake_datasource, sqlite_datasource, session_manager, caplog
):
    """
    Test session manager get cached properly
    """
    # check no record emit
    assert caplog.records == []

    # retrieve data source session for the first time
    _ = session_manager[snowflake_datasource]
    assert len(caplog.records) == 1
    assert caplog.records[0].msg == "Create a new session for snowflake"

    # retrieve same data source for the second time, check that cached is used
    _ = session_manager[snowflake_datasource]
    assert len(caplog.records) == 1

    # retrieve different data source
    _ = session_manager[sqlite_datasource]
    assert len(caplog.records) == 2
    assert caplog.records[1].msg == "Create a new session for sqlite"

    # clear the cache & call again
    session_manager.__getitem__.cache_clear()
    _ = session_manager[snowflake_datasource]
    assert len(caplog.records) == 3
    assert caplog.records[2].msg == "Create a new session for snowflake"
