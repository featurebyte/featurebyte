"""
Tests for featurebyte/session/session_helper.py
"""

import textwrap
from unittest.mock import AsyncMock, Mock, call

import pandas as pd
import pytest

from featurebyte.models.feature_query_set import FeatureQuery, FeatureQuerySet
from featurebyte.session.session_helper import execute_feature_query_set, validate_output_row_index
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
    pipeline.execute.side_effect = [
        [1],  # return semaphone counter
        [2],  # return semaphone zset score
        [],
    ]
    yield mock_redis


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
    with pytest.raises(ValueError) as exc_info:
        await validate_output_row_index(
            session=mock_snowflake_session, output_table_name="my_table"
        )
    assert str(exc_info.value) == "Row index column is invalid in the output table"


@pytest.mark.asyncio
async def test_execute_feature_query_set(mock_snowflake_session, mock_redis, update_fixtures):
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
        mock_snowflake_session,
        mock_redis,
        feature_query_set,
        progress_callback=progress_callback,
    )

    queries = extract_session_executed_queries(mock_snowflake_session)

    # Check executed queries
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/expected_feature_query_set.sql",
        update_fixture=update_fixtures,
    )

    # Check intermediate tables are dropped
    assert mock_snowflake_session.drop_table.call_args_list == [
        call(database_name="sf_db", schema_name="sf_schema", table_name="my_table", if_exists=True),
    ]

    # Check progress update calls
    assert progress_callback.call_args_list == [
        call(50, progress_message),
        call(100, progress_message),
    ]
