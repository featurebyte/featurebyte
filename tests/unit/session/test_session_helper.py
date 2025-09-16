"""
Tests for featurebyte/session/session_helper.py
"""

import textwrap
from unittest.mock import AsyncMock, Mock, call, patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.exception import FeatureQueryExecutionError, InvalidOutputRowIndexError
from featurebyte.models.feature_query_set import FeatureQuerySet
from featurebyte.query_graph.node.schema import TableDetails
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
def session_handler_fixture(mock_snowflake_session, mock_redis, app_container):
    """
    Fixture for a mock SessionHandler
    """
    return SessionHandler(
        session=mock_snowflake_session,
        redis=mock_redis,
        feature_store=Mock(id=ObjectId(), max_query_concurrency=None),
        system_metrics_service=app_container.system_metrics_service,
    )


@pytest.fixture(name="progress_message")
def progress_message_fixture():
    """
    Fixture for a progress message
    """
    return "My custom progress message"


@pytest.fixture(name="feature_query_set")
def feature_query_set_fixture(feature_query_generator, saved_features_set, progress_message):
    """
    Fixture for a FeatureQuerySet
    """
    _, _, feature_names = saved_features_set
    return FeatureQuerySet(
        feature_query_generator=feature_query_generator,
        request_table_name="request_table",
        request_table_columns=["a", "b", "c"],
        output_table_details=TableDetails(table_name="output_table"),
        output_feature_names=feature_names,
        output_include_row_index=True,
        progress_message=progress_message,
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
async def test_execute_feature_query_set(
    session_handler,
    feature_query_set,
    update_fixtures,
    progress_message,
):
    """
    Test execute_feature_query_set
    """
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
        call(
            database_name="sf_db",
            schema_name="sf_schema",
            table_name="__TEMP_000000000000000000000000_0",
            if_exists=True,
            timeout=86400,
        ),
    ]

    # Check progress update calls
    assert progress_callback.call_args_list == [
        call(90, progress_message, metadata={"num_features_materialized": 2}),
        call(100, progress_message),
    ]


@pytest.mark.parametrize("is_output_row_index_valid", [False])
@pytest.mark.asyncio
async def test_execute_feature_query_set__invalid_row_index(
    session_handler, feature_query_set, progress_message
):
    """
    Test execute_feature_query_set
    """
    progress_callback = AsyncMock(name="mock_progress_callback")

    with pytest.raises(InvalidOutputRowIndexError) as exc_info:
        await execute_feature_query_set(
            session_handler=session_handler,
            feature_query_set=feature_query_set,
            progress_callback=progress_callback,
        )

    assert (
        str(exc_info.value)
        == "Row index column is invalid in the intermediate feature table: __TEMP_000000000000000000000000_0. Feature names: another_feature, sum_1d"
    )


@pytest.mark.asyncio
async def test_dynamic_batching__success(session_handler, feature_query_set, update_fixtures):
    """
    Test dynamic batching (success)
    """
    from featurebyte.session.session_helper import execute_feature_query

    progress_callback = AsyncMock(name="mock_progress_callback")
    call_count = {"count": 0}

    async def patched_func(*args, **kwargs):
        """
        Patched function to simulate a successful query after a retry
        """
        should_error = call_count["count"] == 0
        call_count["count"] += 1
        result = await execute_feature_query(*args, **kwargs)
        if should_error:
            raise ValueError("Fail query on purpose")
        return result

    with patch(
        "featurebyte.session.session_helper.execute_feature_query", side_effect=patched_func
    ):
        await execute_feature_query_set(
            session_handler=session_handler,
            feature_query_set=feature_query_set,
            progress_callback=progress_callback,
        )

    # Check executed queries
    queries = extract_session_executed_queries(session_handler.session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/expected_feature_query_set_dynamic_batching.sql",
        update_fixture=update_fixtures,
    )


@pytest.mark.asyncio
async def test_dynamic_batching__failure(session_handler, feature_query_set, update_fixtures):
    """
    Test dynamic batching (failure)
    """
    progress_callback = AsyncMock(name="mock_progress_callback")

    async def patched_func(*args, **kwargs):
        """
        Patched function to simulate feature query failure at all times
        """
        _ = args
        _ = kwargs
        raise ValueError("Fail query on purpose")

    with patch(
        "featurebyte.session.session_helper.execute_feature_query", side_effect=patched_func
    ):
        with pytest.raises(FeatureQueryExecutionError) as exc_info:
            await execute_feature_query_set(
                session_handler=session_handler,
                feature_query_set=feature_query_set,
                progress_callback=progress_callback,
            )

    assert str(exc_info.value) == "Failed to materialize 2 features: another_feature, sum_1d"


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
