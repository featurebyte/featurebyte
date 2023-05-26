"""
Tests for featurebyte.query_graph.feature_historical.py
"""
from unittest.mock import AsyncMock, Mock, call, patch

import pandas as pd
import pytest
from bson import ObjectId
from freezegun import freeze_time
from pandas.testing import assert_frame_equal

from featurebyte import exception
from featurebyte.enum import SourceType
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME, sql_to_string
from featurebyte.query_graph.sql.feature_historical import (
    FeatureQuery,
    HistoricalFeatureQuerySet,
    convert_point_in_time_dtype_if_needed,
    get_historical_features,
    get_historical_features_expr,
    get_historical_features_query_set,
    get_internal_observation_set,
    split_nodes,
    validate_historical_requests_point_in_time,
)
from featurebyte.session.base import BaseSession
from tests.util.helper import assert_equal_with_expected_fixture


def get_historical_features_sql(**kwargs):
    """Get historical features SQL"""
    expr, _ = get_historical_features_expr(**kwargs)
    source_type = kwargs["source_type"]
    return sql_to_string(expr, source_type=source_type)


@pytest.fixture(name="mocked_session")
def mocked_session_fixture():
    """Fixture for a mocked session object"""
    with patch("featurebyte.service.session_manager.SessionManager") as session_manager_cls:
        session_manager = AsyncMock(name="MockedSessionManager")
        mocked_session = Mock(
            name="MockedSession",
            spec=BaseSession,
            database_name="sf_database",
            schema_name="sf_schema",
            source_type=SourceType.SNOWFLAKE,
        )
        session_manager_cls.return_value = session_manager
        yield mocked_session


@pytest.fixture(name="output_table_details")
def output_table_details_fixture():
    """Fixture for a TableDetails for the output location"""
    return TableDetails(table_name="SOME_HISTORICAL_FEATURE_TABLE")


@pytest.fixture(name="fixed_object_id")
def fixed_object_id_fixture():
    """Fixture to for a fixed ObjectId in featurebyte.query_graph.sql.feature_historical"""
    oid = ObjectId("646f1b781d1e7970788b32ec")
    with patch(
        "featurebyte.query_graph.sql.feature_historical.ObjectId",
        return_value=oid,
    ) as mocked:
        yield mocked


@pytest.mark.asyncio
async def test_get_historical_features__missing_point_in_time(
    mock_snowflake_feature,
    mocked_session,
    output_table_details,
):
    """Test validation of missing point in time for historical features"""
    observation_set = pd.DataFrame(
        {
            "cust_id": ["C1", "C2", "C3"],
        }
    )
    with pytest.raises(exception.MissingPointInTimeColumnError) as exc_info:
        await get_historical_features(
            session=mocked_session,
            graph=mock_snowflake_feature.graph,
            nodes=[mock_snowflake_feature.node],
            observation_set=observation_set,
            source_type=SourceType.SNOWFLAKE,
            output_table_details=output_table_details,
        )
    assert str(exc_info.value) == "POINT_IN_TIME column is required"


@freeze_time("2022-05-01")
@pytest.mark.parametrize("point_in_time_is_datetime_dtype", [True, False])
@pytest.mark.asyncio
async def test_get_historical_features__too_recent_point_in_time(
    mock_snowflake_feature, mocked_session, point_in_time_is_datetime_dtype, output_table_details
):
    """Test validation of too recent point in time for historical features"""
    point_in_time_vals = ["2022-04-15", "2022-04-30"]
    if point_in_time_is_datetime_dtype:
        point_in_time_vals = pd.to_datetime(point_in_time_vals)
    observation_set = pd.DataFrame(
        {
            "POINT_IN_TIME": point_in_time_vals,
            "cust_id": ["C1", "C2"],
        }
    )
    with pytest.raises(exception.TooRecentPointInTimeError) as exc_info:
        await get_historical_features(
            session=mocked_session,
            graph=mock_snowflake_feature.graph,
            nodes=[mock_snowflake_feature.node],
            observation_set=observation_set,
            source_type=SourceType.SNOWFLAKE,
            output_table_details=output_table_details,
        )
    assert str(exc_info.value) == (
        "The latest point in time (2022-04-30 00:00:00) should not be more recent than 48 hours "
        "from now"
    )


@pytest.mark.asyncio
async def test_get_historical_features__point_in_time_dtype_conversion(
    float_feature,
    config,
    mocked_session,
    mocked_compute_tiles_on_demand,
    output_table_details,
):
    """
    Test that if point in time column is provided as string, it is converted to datetime before
    being registered as a temp table in session
    """
    # Input POINT_IN_TIME is string
    df_request = pd.DataFrame(
        {
            "POINT_IN_TIME": ["2022-01-01", "2022-02-01"],
            "cust_id": ["C1", "C2"],
        }
    )
    assert df_request.dtypes["POINT_IN_TIME"] == "object"

    mocked_session.generate_session_unique_id.return_value = "1"
    await get_historical_features(
        session=mocked_session,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        observation_set=df_request,
        source_type=SourceType.SNOWFLAKE,
        output_table_details=output_table_details,
    )

    # Check POINT_IN_TIME is converted to datetime
    mocked_session.register_table.assert_awaited_once()
    args, _ = mocked_session.register_table.await_args_list[0]
    df_observation_set_registered = args[1]
    assert df_observation_set_registered.dtypes["POINT_IN_TIME"] == "datetime64[ns]"

    mocked_compute_tiles_on_demand.assert_called_once()


@pytest.mark.asyncio
async def test_get_historical_features__skip_tile_cache_if_deployed(
    float_feature,
    config,
    mocked_session,
    mocked_compute_tiles_on_demand,
    output_table_details,
):
    """
    Test that for with is_feature_list_deployed=True on demand tile computation is skipped
    """
    df_request = pd.DataFrame(
        {
            "POINT_IN_TIME": ["2022-01-01", "2022-02-01"],
            "cust_id": ["C1", "C2"],
        }
    )
    mocked_session.generate_session_unique_id.return_value = "1"
    await get_historical_features(
        session=mocked_session,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        observation_set=df_request,
        source_type=SourceType.SNOWFLAKE,
        is_feature_list_deployed=True,
        output_table_details=output_table_details,
    )
    mocked_compute_tiles_on_demand.assert_not_called()


def test_get_historical_feature_sql(float_feature, update_fixtures):
    """Test SQL code generated for historical features is expected"""
    request_table_columns = ["POINT_IN_TIME", "cust_id", "A", "B", "C"]
    sql = get_historical_features_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        request_table_columns=request_table_columns,
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        sql, "tests/fixtures/expected_historical_requests.sql", update_fixture=update_fixtures
    )


def test_get_historical_feature_sql__serving_names_mapping(float_feature, update_fixtures):
    """Test SQL code generated for historical features with serving names mapping"""
    request_table_columns = ["POINT_IN_TIME", "NEW_CUST_ID", "A", "B", "C"]
    serving_names_mapping = {"cust_id": "NEW_CUST_ID"}
    sql = get_historical_features_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        request_table_columns=request_table_columns,
        serving_names_mapping=serving_names_mapping,
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_historical_requests_with_mapping.sql",
        update_fixture=update_fixtures,
    )


def test_validate_historical_requests_point_in_time():
    """Test validate_historical_requests_point_in_time work with timestamps that contain timezone"""
    original_observation_set = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.date_range("2020-01-01T10:00:00+08:00", freq="D", periods=10),
        }
    )
    observation_set = original_observation_set.copy()

    # this should not fail and convert point-in-time values to UTC
    converted_observation_set = convert_point_in_time_dtype_if_needed(observation_set)
    validate_historical_requests_point_in_time(
        get_internal_observation_set(converted_observation_set)
    )
    expected_df = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.date_range("2020-01-01T02:00:00", freq="D", periods=10),
        }
    )
    assert_frame_equal(converted_observation_set, expected_df)

    # observation_set should not be modified
    assert_frame_equal(observation_set, original_observation_set)


def test_get_historical_feature_sql__with_missing_value_imputation(
    query_graph_with_cleaning_ops_and_groupby, update_fixtures
):
    """Test SQL code generated for historical features constructed with missing value imputation"""
    request_table_columns = ["POINT_IN_TIME", "CUSTOMER_ID"]
    graph, node = query_graph_with_cleaning_ops_and_groupby
    sql = get_historical_features_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        request_table_columns=request_table_columns,
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_historical_requests_with_missing_value_imputation.sql",
        update_fixture=update_fixtures,
    )


def test_get_historical_feature_query_set__single_batch(
    float_feature, output_table_details, fixed_object_id, update_fixtures
):
    """
    Test historical features are calculated in single batch when there are not many nodes
    """
    request_table_columns = ["POINT_IN_TIME", "CUSTOMER_ID"]
    query_set = get_historical_features_query_set(
        request_table_name=REQUEST_TABLE_NAME,
        graph=float_feature.graph,
        node_groups=[[float_feature.node]],
        request_table_columns=request_table_columns,
        source_type=SourceType.SNOWFLAKE,
        output_table_details=output_table_details,
    )
    assert query_set.feature_queries == []
    assert_equal_with_expected_fixture(
        query_set.output_query,
        "tests/fixtures/expected_historical_requests_single_batch_output_query.sql",
        update_fixture=update_fixtures,
    )


def test_get_historical_feature_query_set__multiple_batches(
    global_graph,
    feature_nodes_all_types,
    output_table_details,
    fixed_object_id,
    update_fixtures,
):
    """
    Test historical features are executed in batches when there are many nodes
    """
    request_table_columns = ["POINT_IN_TIME", "CUSTOMER_ID"]
    query_set = get_historical_features_query_set(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        node_groups=split_nodes(feature_nodes_all_types, 2),
        request_table_columns=request_table_columns,
        source_type=SourceType.SNOWFLAKE,
        output_table_details=output_table_details,
    )
    for i, feature_query in enumerate(query_set.feature_queries):
        assert_equal_with_expected_fixture(
            feature_query.sql,
            f"tests/fixtures/expected_historical_requests_multiple_batches_feature_set_{i}.sql",
            update_fixture=update_fixtures,
        )
    assert_equal_with_expected_fixture(
        query_set.output_query,
        "tests/fixtures/expected_historical_requests_multiple_batches_output_query.sql",
        update_fixture=update_fixtures,
    )


@pytest.mark.asyncio
async def test_historical_feature_query_set_execute(mocked_session):
    """
    Test HistoricalFeatureQuerySet execution
    """
    progress_callback = Mock(name="mock_progress_callback")
    feature_queries = [
        FeatureQuery(
            sql="some_feature_query_1",
            table_name="A",
            feature_names=["F1"],
        ),
        FeatureQuery(
            sql="some_feature_query_2",
            table_name="B",
            feature_names=["F2"],
        ),
    ]
    historical_feature_query_set = HistoricalFeatureQuerySet(
        feature_queries=feature_queries,
        output_query="some_final_join_query",
    )
    await historical_feature_query_set.execute(mocked_session, progress_callback)
    assert mocked_session.execute_query_long_running.call_args_list == [
        call("some_feature_query_1"),
        call("some_feature_query_2"),
        call("some_final_join_query"),
    ]
    assert mocked_session.drop_table.call_args_list == [
        call(database_name="sf_database", schema_name="sf_schema", table_name="A", if_exists=True),
        call(database_name="sf_database", schema_name="sf_schema", table_name="B", if_exists=True),
    ]
    assert progress_callback.call_args_list == [
        call(33, "Computing features"),
        call(66, "Computing features"),
        call(100, "Computing features"),
    ]
