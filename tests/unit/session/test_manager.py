"""
Tests for SessionManager class
"""

import logging
import time
from typing import cast
from unittest import mock
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import pytest_asyncio
from pytest import LogCaptureFixture

from featurebyte.api.feature_store import FeatureStore
from featurebyte.exception import SessionInitializationTimeOut
from featurebyte.query_graph.node.schema import SQLiteDetails
from featurebyte.session.base import (
    DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS,
    BaseSession,
    session_cache,
)


@pytest.fixture(autouse=True, name="caplog_handle")
def caplog_handle_fixture(caplog: LogCaptureFixture):
    """
    Log captured emitted output
    """
    logger_name = "featurebyte.session.manager"
    caplog.set_level(logging.DEBUG, logger_name)
    yield caplog
    caplog.set_level(logging.NOTSET, logger_name)


@pytest.fixture(name="sqlite_feature_store")
def sqlite_feature_store_fixture(config):
    """
    SQLite database source fixture
    """
    _ = config
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
    snowflake_credentials,
    sqlite_feature_store,
    snowflake_execute_query,
    session_manager_service,
    caplog_handle,
):
    """
    Test session manager get cached properly
    """
    _ = snowflake_execute_query
    session_cache.clear()  # Clear any previous session_cache entries from previous tests
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
    _ = await session_manager_service.get_session(snowflake_feature_store, snowflake_credentials)
    count, msg = count_create_session_logs()
    assert count == 1
    assert msg == "Create a new session for snowflake"

    # retrieve same data source for the second time, check that cached is used
    _ = await session_manager_service.get_session(snowflake_feature_store, snowflake_credentials)
    count, msg = count_create_session_logs()
    assert count == 1

    # retrieve different data source
    _ = await session_manager_service.get_session(sqlite_feature_store)
    count, msg = count_create_session_logs()
    assert count == 2
    assert msg == "Create a new session for sqlite"

    # clear the cache & call again
    session_cache.clear()
    _ = await session_manager_service.get_session(snowflake_feature_store, snowflake_credentials)
    count, msg = count_create_session_logs()
    assert count == 3
    assert msg == "Create a new session for snowflake"


@pytest.mark.asyncio
async def test_session_manager_get_new_session_timeout(
    snowflake_feature_store_params,
    snowflake_credentials,
    session_manager_service,
):
    """
    Test timeout exception raise during get new session
    """
    snowflake_feature_store = FeatureStore(**snowflake_feature_store_params, type="snowflake")
    session_cache.clear()  # Clear any previous session_cache entries from previous tests

    with patch("featurebyte.service.session_manager.SnowflakeSession.__init__") as mock_init:

        def slow_init(*args, **kwargs):
            time.sleep(1)

        mock_init.side_effect = slow_init
        with pytest.raises(SessionInitializationTimeOut) as exc:
            await session_manager_service.get_session(
                snowflake_feature_store, snowflake_credentials, timeout=0.5
            )
        assert "Session creation timed out after" in str(exc.value)


@pytest_asyncio.fixture(name="cached_session_and_cache_key")
async def cached_session_setup(
    snowflake_feature_store_params,
    snowflake_connector,
    snowflake_query_map,
    snowflake_credentials,
    session_manager_service,
):
    """Setup for cached session tests"""
    _ = snowflake_connector

    def _side_effect(query, timeout=DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS, to_log_error=True):
        _ = timeout, to_log_error
        res = snowflake_query_map.get(query)
        if res is not None:
            return pd.DataFrame(res)
        return None

    session_cache.clear()
    assert len(session_cache) == 0
    snowflake_feature_store = FeatureStore(**snowflake_feature_store_params, type="snowflake")
    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = _side_effect
        session = await session_manager_service.get_session(
            snowflake_feature_store, snowflake_credentials, timeout=0.5
        )

    # check the session is cached
    session_cache_key = session._cache_key
    assert session_cache_key is not None
    assert session_cache_key in session_cache
    yield session, session_cache_key


@pytest.mark.asyncio
async def test_session_manager__invalidate_cache_when_execute_query_failed(
    cached_session_and_cache_key,
):
    """Test session cache is invalidated when execute_query fails"""
    session, session_cache_key = cached_session_and_cache_key

    # simulate exception during query execution
    assert session_cache_key in session_cache
    with mock.patch(
        "featurebyte.session.base.BaseSession.get_async_query_generator"
    ) as mock_get_async_query_generator:
        exc_message = "Some error"
        mock_get_async_query_generator.side_effect = Exception(exc_message)
        with pytest.raises(Exception, match=exc_message):
            base_session = cast(BaseSession, session)
            await base_session.execute_query("SELECT * FROM some_table")

    # check the session is invalidated
    assert session_cache_key not in session_cache


@pytest.mark.asyncio
async def test_session_manager__invalidate_cache_when_execute_query_blocking_failed(
    cached_session_and_cache_key, snowflake_connector_patches
):
    """Test session cache is invalidated when execute_query fails"""
    session, session_cache_key = cached_session_and_cache_key

    # simulate exception during query execution
    cursor = snowflake_connector_patches["cursor"]
    exc_message = "Some error"
    cursor.execute.side_effect = Exception(exc_message)

    assert session_cache_key in session_cache
    with pytest.raises(Exception, match=exc_message):
        session.execute_query_blocking("SELECT * FROM some_table")

    # check the session is invalidated
    assert session_cache_key not in session_cache
