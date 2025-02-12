"""
Tests for featurebyte/session/session_helper.py
"""

import textwrap
from unittest.mock import AsyncMock, Mock, call

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.exception import InvalidOutputRowIndexError
from featurebyte.models.feature_query_set import FeatureQuery, FeatureQuerySet
from featurebyte.session.session_helper import (
    SessionHandler,
    execute_feature_query_set,
    run_coroutines,
    validate_output_row_index,
)
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(name="is_output_row_index_valid")
def is_output_row_index_valid_fixture():
    """
    Fixture to determine whether output row index should be valid in a test
    """
    return True


@pytest.fixture(name="mock_snowflake_session")
def mock_snowflake_session_fixture(mock_snowflake_session, is_output_row_index_valid):
    """
    Mock query result
    """
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "is_row_index_valid": [is_output_row_index_valid]
    })
    yield mock_snowflake_session


@pytest.fixture(name="mock_redis")
def mock_redis_fixture(is_output_row_index_valid):
    """
    Mock redis
    """
    mock_redis = Mock()
    pipeline = mock_redis.pipeline.return_value
    pipeline.incr.return_value = pipeline
    pipeline.zadd.return_value = pipeline
    pipeline.zrem.return_value = pipeline
    pipeline.execute.return_value = [0]
    yield mock_redis


@pytest.fixture(name="session_handler")
def session_handler_fixture(mock_snowflake_session, mock_redis):
    """
    Fixture for a mock SessionHandler
    """
    return SessionHandler(
        session=mock_snowflake_session,
        redis=mock_redis,
        feature_store=Mock(id=ObjectId(), max_query_concurrency=None),
    )


@pytest.mark.asyncio
async def test_validate_row_index__valid(mock_snowflake_session):
    """
    Test validate_output_row_index expected query
    """
    await validate_output_row_index(session=mock_snowflake_session, output_table_name="my_table")
    query = mock_snowflake_session.execute_query_long_running.call_args[0][0]
    expected = textwrap.dedent(
        """
        SELECT
          COUNT(DISTINCT "__FB_TABLE_ROW_INDEX") = COUNT(*) AS "is_row_index_valid"
        FROM "my_table"
        """
    ).strip()
    assert query == expected


@pytest.mark.parametrize("is_output_row_index_valid", [False])
@pytest.mark.asyncio
async def test_validate_row_index__invalid(mock_snowflake_session):
    """
    Test validate_output_row_index on error case
    """
    with pytest.raises(InvalidOutputRowIndexError) as exc_info:
        await validate_output_row_index(
            session=mock_snowflake_session, output_table_name="my_table"
        )
    assert str(exc_info.value) == "Row index column is invalid in the output table"


@pytest.mark.asyncio
async def test_execute_feature_query_set(session_handler, update_fixtures):
    """
    Test execute_feature_query_set
    """
    progress_message = "My custom progress message"
    feature_query_set = FeatureQuerySet(
        feature_queries=[
            FeatureQuery(
                sql="CREATE TABLE my_table AS SELECT * FROM another_table",
                table_name="my_table",
                feature_names=["a"],
            ),
        ],
        output_query="CREATE TABLE output_table AS SELECT * FROM my_table",
        output_table_name="output_table",
        progress_message=progress_message,
        validate_output_row_index=True,
    )
    progress_callback = AsyncMock(name="mock_progress_callback")

    await execute_feature_query_set(
        session_handler=session_handler,
        feature_query_set=feature_query_set,
        progress_callback=progress_callback,
    )

    session = session_handler.session
    queries = extract_session_executed_queries(session)

    # Check executed queries
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/expected_feature_query_set.sql",
        update_fixture=update_fixtures,
    )

    # Check intermediate tables are dropped
    assert session.drop_table.call_args_list == [
        call(database_name="sf_db", schema_name="sf_schema", table_name="my_table", if_exists=True),
    ]

    # Check progress update calls
    assert progress_callback.call_args_list == [
        call(50, progress_message),
        call(100, progress_message),
    ]


@pytest.mark.parametrize("is_output_row_index_valid", [False])
@pytest.mark.asyncio
async def test_execute_feature_query_set__invalid_row_index(session_handler):
    """
    Test execute_feature_query_set
    """
    progress_message = "My custom progress message"
    feature_query_set = FeatureQuerySet(
        feature_queries=[
            FeatureQuery(
                sql="CREATE TABLE my_table AS SELECT * FROM another_table",
                table_name="my_table",
                feature_names=["a", "b", "c"],
            ),
        ],
        output_query="CREATE TABLE output_table AS SELECT * FROM my_table",
        output_table_name="output_table",
        progress_message=progress_message,
        validate_output_row_index=True,
    )
    progress_callback = AsyncMock(name="mock_progress_callback")

    with pytest.raises(InvalidOutputRowIndexError) as exc_info:
        await execute_feature_query_set(
            session_handler=session_handler,
            feature_query_set=feature_query_set,
            progress_callback=progress_callback,
        )

    assert (
        str(exc_info.value)
        == "Row index column is invalid in the intermediate feature table: my_table. Feature names: a, b, c"
    )


@pytest.mark.asyncio
async def test_run_coroutines_return_exceptions(mock_redis):
    """
    Test run_coroutines with return_exceptions=True
    """

    async def _ok(i):
        return f"OK {i}"

    async def _error(i):
        raise ValueError(f"Error {i}")

    coroutines = [
        _ok(1),
        _error(2),
        _error(3),
        _error(4),
        _ok(5),
    ]
    result = await run_coroutines(
        coroutines=coroutines,
        redis=mock_redis,
        concurrency_key="my_key",
        max_concurrency=10,
        return_exceptions=True,
    )
    assert result[0] == "OK 1"
    assert isinstance(result[1], ValueError) and str(result[1]) == "Error 2"
    assert isinstance(result[2], ValueError) and str(result[2]) == "Error 3"
    assert isinstance(result[3], ValueError) and str(result[3]) == "Error 4"
    assert result[4] == "OK 5"
