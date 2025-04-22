"""
Test preview service module
"""

import textwrap

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId
from sqlglot import parse_one

from featurebyte import FeatureStore, SourceType
from featurebyte.common.utils import dataframe_from_json
from featurebyte.exception import (
    DescribeQueryExecutionError,
    MissingPointInTimeColumnError,
    RequiredEntityNotProvidedError,
)
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.schema.feature_list import FeatureListPreview
from featurebyte.schema.feature_store import FeatureStorePreview, FeatureStoreSample
from featurebyte.schema.preview import FeatureOrTargetPreview
from featurebyte.warning import QueryNoLimitWarning
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(name="empty_graph")
def empty_graph_fixture():
    """Fake graph"""
    return {
        "nodes": [],
        "edges": [],
    }


@pytest.fixture(name="feature_store_preview")
def feature_store_preview_fixture(feature_store):
    """
    Fixture for a FeatureStorePreview
    """
    return FeatureStorePreview(
        graph={
            "edges": [{"source": "input_1", "target": "project_1"}],
            "nodes": [
                {
                    "name": "input_1",
                    "output_type": "frame",
                    "parameters": {
                        "type": "event_table",
                        "id": "6332fdb21e8f0eeccc414512",
                        "columns": [
                            {"name": "col_int", "dtype": "INT"},
                            {"name": "col_float", "dtype": "FLOAT"},
                            {"name": "col_char", "dtype": "CHAR"},
                            {"name": "col_text", "dtype": "VARCHAR"},
                            {"name": "col_binary", "dtype": "BINARY"},
                            {"name": "col_boolean", "dtype": "BOOL"},
                            {"name": "event_timestamp", "dtype": "TIMESTAMP"},
                            {"name": "created_at", "dtype": "TIMESTAMP"},
                            {"name": "cust_id", "dtype": "VARCHAR"},
                        ],
                        "table_details": {
                            "database_name": "sf_database",
                            "schema_name": "sf_schema",
                            "table_name": "sf_table",
                        },
                        "feature_store_details": feature_store.json_dict(),
                        "timestamp_column": "event_timestamp",
                        "id_column": "event_timestamp",
                    },
                    "type": "input",
                },
                {
                    "name": "project_1",
                    "output_type": "frame",
                    "parameters": {
                        "columns": [
                            "col_int",
                            "col_float",
                            "col_char",
                            "col_text",
                            "col_binary",
                            "col_boolean",
                            "event_timestamp",
                            "created_at",
                            "cust_id",
                        ]
                    },
                    "type": "project",
                },
            ],
        },
        node_name="project_1",
    )


@pytest.fixture(name="feature_store_preview_project_column")
def feature_store_preview_project_column_fixture(feature_store_preview, project_column):
    """
    Fixture for a FeatureStorePreview with a project column
    """
    project_node = next(
        node for node in feature_store_preview.graph.nodes if node.type == "project"
    )
    project_node.parameters.columns = [project_column]
    return feature_store_preview


@pytest.fixture(name="feature_store_sample")
def feature_store_sample_fixture(feature_store_preview):
    """
    Fixture for a FeatureStoreSample
    """
    return FeatureStoreSample(**feature_store_preview.model_dump())


@pytest.mark.asyncio
async def test_preview_feature__time_based_feature_without_point_in_time_errors(
    feature_preview_service, float_feature
):
    """
    Test preview feature
    """
    feature_preview = FeatureOrTargetPreview(
        feature_store_name="feature_store_name",
        point_in_time_and_serving_name_list=[{}],
        graph=float_feature.graph,
        node_name=float_feature.node_name,
    )
    with pytest.raises(MissingPointInTimeColumnError) as exc:
        await feature_preview_service.preview_target_or_feature(feature_preview)
    assert "Point in time column not provided" in str(exc)


@pytest.mark.asyncio
async def test_preview_feature__non_time_based_feature_without_point_in_time_doesnt_error(
    feature_preview_service, transaction_entity, non_time_based_feature, insert_credential
):
    """
    Test preview feature
    """
    _ = transaction_entity, insert_credential
    feature_preview = FeatureOrTargetPreview(
        feature_store_name="sf_featurestore",
        point_in_time_and_serving_name_list=[
            {
                "transaction_id": 1,
            }
        ],
        graph=non_time_based_feature.graph,
        node_name=non_time_based_feature.node_name,
        feature_store_id=non_time_based_feature.tabular_source.feature_store_id,
    )
    await feature_preview_service.preview_target_or_feature(feature_preview)


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_preview_feature__missing_entity(feature_preview_service, production_ready_feature):
    """
    Test preview feature but without providing the required entity
    """
    feature_preview = FeatureOrTargetPreview(
        feature_store_name="sf_featurestore",
        point_in_time_and_serving_name_list=[
            {
                "POINT_IN_TIME": "2022-05-01",
                "abc": 1,
            }
        ],
        graph=production_ready_feature.graph,
        node_name=production_ready_feature.node_name,
        feature_store_id=production_ready_feature.tabular_source.feature_store_id,
    )
    with pytest.raises(RequiredEntityNotProvidedError) as exc:
        await feature_preview_service.preview_target_or_feature(feature_preview)
    expected = (
        'Required entities are not provided in the request: customer (serving name: "cust_id")'
    )
    assert str(exc.value) == expected


@pytest.mark.asyncio
async def test_preview_featurelist__time_based_feature_errors_without_time(
    feature_preview_service, float_feature
):
    """
    Test preview featurelist
    """
    feature_list_preview = FeatureListPreview(
        feature_store_name="sf_featurestore",
        feature_clusters=[
            FeatureCluster(
                feature_store_id=PydanticObjectId(ObjectId()),
                graph=float_feature.graph,
                node_names=[float_feature.node_name],
            ),
        ],
        point_in_time_and_serving_name_list=[
            {
                "event_id_col": 1,
            }
        ],
    )
    with pytest.raises(MissingPointInTimeColumnError) as exc:
        await feature_preview_service.preview_featurelist(feature_list_preview)
    assert "Point in time column not provided" in str(exc)


@pytest.mark.asyncio
async def test_preview_featurelist__non_time_based_feature_no_error_without_time(
    feature_preview_service, transaction_entity, non_time_based_feature, insert_credential
):
    """
    Test preview featurelist
    """
    _ = transaction_entity, insert_credential
    store = FeatureStore.get("sf_featurestore")
    feature_list_preview = FeatureListPreview(
        feature_clusters=[
            FeatureCluster(
                feature_store_id=store.id,
                graph=non_time_based_feature.graph,
                node_names=[non_time_based_feature.node_name],
            ),
        ],
        point_in_time_and_serving_name_list=[
            {
                "transaction_id": 1,
            }
        ],
    )
    await feature_preview_service.preview_featurelist(feature_list_preview)


@pytest.mark.asyncio
async def test_preview_featurelist__missing_entity(
    feature_preview_service, production_ready_feature_list
):
    """
    Test preview featurelist but without providing the required entity
    """
    feature_list_preview = FeatureListPreview(
        feature_store_name="sf_featurestore",
        feature_clusters=production_ready_feature_list.feature_clusters,
        point_in_time_and_serving_name_list=[
            {
                "POINT_IN_TIME": "2022-05-01",
                "abc": 1,
            }
        ],
    )
    with pytest.raises(RequiredEntityNotProvidedError) as exc:
        await feature_preview_service.preview_featurelist(feature_list_preview)
    expected = (
        'Required entities are not provided in the request: customer (serving name: "cust_id")'
    )
    assert str(exc.value) == expected


async def mock_execute_query(query):
    """Execute query mock"""
    if "%missing" not in query:
        return pd.DataFrame({"count": [100]})
    result_column_names = [
        col.alias_or_name for col in parse_one(query, read="snowflake").expressions
    ]
    # Make %missing stats null in all columns
    data = {col: ["some_value"] if "%missing" not in col else [None] for col in result_column_names}
    return pd.DataFrame(data)


@pytest.mark.parametrize("drop_all_null_stats", [False, True])
@pytest.mark.asyncio
async def test_describe_drop_all_null_stats(
    preview_service,
    feature_store_sample,
    mock_snowflake_session,
    drop_all_null_stats,
):
    """
    Test describe
    """

    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    result = dataframe_from_json(
        await preview_service.describe(
            feature_store_sample,
            size=5000,
            seed=0,
            drop_all_null_stats=drop_all_null_stats,
        )
    )
    if drop_all_null_stats:
        assert "%missing" not in result.index
    else:
        assert "%missing" in result.index


@pytest.mark.asyncio
async def test_describe_dynamic_batching(
    preview_service,
    feature_store_sample,
    mock_snowflake_session,
    update_fixtures,
):
    """
    Test describe with dynamic batching. Expect to see increasing simplified queries being executed
    as execute_query_long_running for stats computation fails.
    """

    async def execute_query_always_fail_for_stats(query):
        if "CREATE TABLE" in query:
            return
        raise RuntimeError("Failing the query on purpose!")

    mock_snowflake_session.execute_query_long_running.side_effect = (
        execute_query_always_fail_for_stats
    )

    with pytest.raises(DescribeQueryExecutionError):
        await preview_service.describe(
            feature_store_sample,
            size=0,
            seed=0,
        )

    queries = extract_session_executed_queries(mock_snowflake_session)
    fixture_filename = (
        "tests/fixtures/preview_service/expected_describe_dynamic_batching_queries.sql"
    )
    assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)


@pytest.mark.asyncio
async def test_value_counts(
    preview_service,
    feature_store_preview,
    mock_snowflake_session,
):
    """
    Test value counts
    """
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "key": ["1", "2", None],
        "count": [100, 50, 3],
    })

    callback_call_args = []

    async def completion_callback(n):
        callback_call_args.append(n)

    result = await preview_service.value_counts(
        feature_store_preview,
        column_names=["col_int", "col_float", "col_char"],
        num_rows=100000,
        num_categories_limit=500,
        completion_callback=completion_callback,
    )
    assert result == {
        "col_int": {1: 100, 2: 50, None: 3},
        "col_float": {1.0: 100, 2.0: 50, None: 3},
        "col_char": {"1": 100, "2": 50, None: 3},
    }
    assert callback_call_args == [1, 2, 3]


@pytest.mark.parametrize(
    "project_column,keys,expected",
    [
        (
            "col_int",
            [1, 2, None],
            {1: 100, 2: 50, None: 3},
        ),
        (
            "col_float",
            np.float32([1.0, 2.0, np.nan]),
            {1.0: 100, 2.0: 50, None: 3},
        ),
        (
            "col_text",
            ["a", "b", None],
            {"a": 100, "b": 50, None: 3},
        ),
    ],
)
@pytest.mark.asyncio
async def test_value_counts_not_convert_keys_to_string(
    preview_service,
    feature_store_preview_project_column,
    project_column,
    keys,
    expected,
    mock_snowflake_session,
):
    """
    Test value counts
    """
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "key": keys,
        "count": [100, 50, 3],
    })
    result = await preview_service.value_counts(
        feature_store_preview_project_column,
        num_rows=100000,
        column_names=[project_column],
        num_categories_limit=500,
    )
    assert result == {project_column: expected}


@pytest.mark.asyncio
async def test_query_no_limit_warning_is_raised(
    preview_service,
    feature_store_sample,
    feature_store_preview,
    mock_snowflake_session,
):
    """Test query no limit warning is raised"""

    # mock execute query & check warning
    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    with pytest.warns(QueryNoLimitWarning):
        await preview_service.describe(feature_store_sample, size=0, seed=0)

    with pytest.warns(QueryNoLimitWarning):
        await preview_service.sample(feature_store_sample, size=0, seed=0)

    with pytest.warns(QueryNoLimitWarning):
        await preview_service.preview(feature_store_preview, limit=0)


@pytest.mark.asyncio
async def test_sample_does_not_change_underlying_graph(
    preview_service,
    feature_store_sample,
    mock_snowflake_session,
):
    """Test sample does not change underlying graph"""

    payload_dict = feature_store_sample.model_dump(by_alias=True)
    graph = QueryGraph(**payload_dict["graph"])
    payload = FeatureStoreSample(**{**payload_dict, "graph": graph})
    before_sample = graph.model_dump_json(by_alias=True)

    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query

    await preview_service.sample(payload, size=0, seed=0, sample_on_primary_table=True)
    after_sample = graph.model_dump_json(by_alias=True)
    assert before_sample == after_sample


@pytest.fixture(name="feature_store_sample_with_filter_graph")
def feature_store_sample_with_filter_graph_fixture(feature_store_sample):
    """Fixture for a FeatureStoreSample with a filter graph"""
    # construct another graph with filtering
    graph = QueryGraph()
    input_node = graph.add_operation_node(
        node=feature_store_sample.graph.nodes_map["input_1"], input_nodes=[]
    )
    project_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["col_int"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    equal_node = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[project_node],
    )
    filter_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node, equal_node],
    )
    new_feature_store_sample = FeatureStoreSample(**{
        **feature_store_sample.model_dump(by_alias=True),
        "graph": graph,
        "node_name": filter_node.name,
    })
    assert new_feature_store_sample.graph.edges_map == {
        "input_1": ["project_1", "filter_1"],
        "project_1": ["eq_1"],
        "eq_1": ["filter_1"],
    }
    assert new_feature_store_sample.node_name == "filter_1"
    return new_feature_store_sample


@pytest.fixture(name="expected_create_sample_table_sql")
def expected_create_sample_table_sql_fixture():
    """Expected create sample table SQL"""
    expected = textwrap.dedent(
        """
        SELECT
          "col_int",
          "col_float",
          "col_char",
          "col_text",
          "col_binary",
          "col_boolean",
          "event_timestamp",
          "created_at",
          "cust_id"
        FROM (
          SELECT
            CAST(BITAND(RANDOM(1234), 2147483647) AS DOUBLE) / 2147483647.0 AS "prob",
            "col_int",
            "col_float",
            "col_char",
            "col_text",
            "col_binary",
            "col_boolean",
            "event_timestamp",
            "created_at",
            "cust_id"
          FROM (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "col_char" AS "col_char",
              CAST("col_text" AS VARCHAR) AS "col_text",
              "col_binary" AS "col_binary",
              "col_boolean" AS "col_boolean",
              "event_timestamp" AS "event_timestamp",
              "created_at" AS "created_at",
              CAST("cust_id" AS VARCHAR) AS "cust_id"
            FROM "sf_database"."sf_schema"."sf_table"
          )
        )
        WHERE
          "prob" <= 0.15000000000000002
        ORDER BY
          "prob"
        LIMIT 10
        """
    ).strip()
    return expected


@pytest.mark.asyncio
async def test_sample_with_sample_on_primary_table_enable(
    preview_service,
    feature_store_sample,
    feature_store_sample_with_filter_graph,
    mock_snowflake_session,
    expected_create_sample_table_sql,
):
    """Test sample with sample_on_primary_table enabled"""

    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    seed = 1234

    await preview_service.sample(
        feature_store_sample, size=10, seed=seed, sample_on_primary_table=True
    )
    create_sample_table_sql = sql_to_string(
        mock_snowflake_session.create_table_as.call_args_list[0][1]["select_expr"],
        source_type=SourceType.SNOWFLAKE,
    )
    assert create_sample_table_sql == expected_create_sample_table_sql

    # check filtered graph
    sample_size_equal_pair = [(10, True), (11, False)]
    for sample_size, is_equal in sample_size_equal_pair:
        # clear the mock
        mock_snowflake_session.reset_mock()

        # try to sample the filtered graph with the same size and seed
        await preview_service.sample(
            feature_store_sample_with_filter_graph,
            size=sample_size,
            seed=seed,
            sample_on_primary_table=True,
        )
        create_sample_table_sql = sql_to_string(
            mock_snowflake_session.create_table_as.call_args_list[0][1]["select_expr"],
            source_type=SourceType.SNOWFLAKE,
        )
        if is_equal:
            assert create_sample_table_sql == expected_create_sample_table_sql
        else:
            assert create_sample_table_sql != expected_create_sample_table_sql


@pytest.mark.asyncio
async def test_describe_with_sample_on_primary_table_enable(
    preview_service,
    feature_store_sample,
    feature_store_sample_with_filter_graph,
    mock_snowflake_session,
    expected_create_sample_table_sql,
):
    """Test describe with sample_on_primary_table enabled"""
    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    seed = 1234

    await preview_service.describe(
        feature_store_sample,
        size=10,
        seed=seed,
        sample_on_primary_table=True,
    )
    create_sample_table_sql = sql_to_string(
        mock_snowflake_session.create_table_as.call_args_list[0][1]["select_expr"],
        source_type=SourceType.SNOWFLAKE,
    )
    assert create_sample_table_sql == expected_create_sample_table_sql

    # check filtered graph
    sample_size_equal_pair = [(10, True), (11, False)]
    for sample_size, is_equal in sample_size_equal_pair:
        # clear the mock
        mock_snowflake_session.reset_mock()

        # try to sample the filtered graph with the same size and seed
        await preview_service.describe(
            feature_store_sample_with_filter_graph,
            size=sample_size,
            seed=seed,
            sample_on_primary_table=True,
        )
        create_sample_table_sql = sql_to_string(
            mock_snowflake_session.create_table_as.call_args_list[0][1]["select_expr"],
            source_type=SourceType.SNOWFLAKE,
        )
        if is_equal:
            assert create_sample_table_sql == expected_create_sample_table_sql
        else:
            assert create_sample_table_sql != expected_create_sample_table_sql


@pytest.mark.asyncio
async def test_value_counts_with_sample_on_primary_table_enable(
    preview_service,
    feature_store_sample,
    feature_store_sample_with_filter_graph,
    mock_snowflake_session,
    expected_create_sample_table_sql,
):
    """Test describe with sample_on_primary_table enabled"""
    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "key": ["1", "2", None],
        "count": [100, 50, 3],
    })
    seed = 1234

    await preview_service.value_counts(
        feature_store_sample,
        column_names=["col_char"],
        num_rows=10,
        num_categories_limit=5,
        seed=seed,
        sample_on_primary_table=True,
    )
    create_sample_table_sql = sql_to_string(
        mock_snowflake_session.create_table_as.call_args_list[0][1]["select_expr"],
        source_type=SourceType.SNOWFLAKE,
    )
    cached_table_name = mock_snowflake_session.create_table_as.call_args_list[0][1]["table_details"]
    assert cached_table_name == "__FB_TEMPORARY_TABLE_000000000000000000000000"
    assert create_sample_table_sql == expected_create_sample_table_sql

    # check another create table SQL
    expected_another_create_sample_table_sql = textwrap.dedent(
        """
        CREATE TABLE "__FB_TEMPORARY_TABLE_000000000000000000000001" AS
        SELECT
          "col_int",
          "col_float",
          "col_char",
          "col_text",
          "col_binary",
          "col_boolean",
          "event_timestamp",
          "created_at",
          "cust_id"
        FROM (
          SELECT
            CAST(BITAND(RANDOM(1234), 2147483647) AS DOUBLE) / 2147483647.0 AS "prob",
            "col_int",
            "col_float",
            "col_char",
            "col_text",
            "col_binary",
            "col_boolean",
            "event_timestamp",
            "created_at",
            "cust_id"
          FROM (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "col_char" AS "col_char",
              CAST("col_text" AS VARCHAR) AS "col_text",
              "col_binary" AS "col_binary",
              "col_boolean" AS "col_boolean",
              "event_timestamp" AS "event_timestamp",
              "created_at" AS "created_at",
              CAST("cust_id" AS VARCHAR) AS "cust_id"
            FROM "__FB_TEMPORARY_TABLE_000000000000000000000000"
          )
        )
        WHERE
          "prob" <= 0.15000000000000002
        ORDER BY
          "prob"
        LIMIT 10
        """
    ).strip()
    assert (
        mock_snowflake_session.execute_query_long_running.call_args_list[-2][0][0]
        == expected_another_create_sample_table_sql
    )

    # check value counts SQL
    expected_value_count_sql = textwrap.dedent(
        """
        SELECT
          "col_char" AS "key",
          "__FB_COUNTS" AS "count"
        FROM (
          SELECT
            "col_char",
            COUNT(*) AS "__FB_COUNTS"
          FROM "__FB_TEMPORARY_TABLE_000000000000000000000001"
          GROUP BY
            "col_char"
          ORDER BY
            "__FB_COUNTS" DESC NULLS LAST
          LIMIT 5
        )
        """
    ).strip()
    assert (
        mock_snowflake_session.execute_query_long_running.call_args_list[-1][0][0]
        == expected_value_count_sql
    )

    # check filtered graph
    sample_size_equal_pair = [(10, True), (11, False)]
    for sample_size, is_equal in sample_size_equal_pair:
        # clear the mock
        mock_snowflake_session.reset_mock()

        # try to sample the filtered graph with the same size and seed
        await preview_service.value_counts(
            feature_store_sample_with_filter_graph,
            column_names=["col_char"],
            num_rows=sample_size,
            num_categories_limit=5,
            seed=seed,
            sample_on_primary_table=True,
        )
        create_sample_table_sql = sql_to_string(
            mock_snowflake_session.create_table_as.call_args_list[0][1]["select_expr"],
            source_type=SourceType.SNOWFLAKE,
        )
        if is_equal:
            assert create_sample_table_sql == expected_create_sample_table_sql
        else:
            assert create_sample_table_sql != expected_create_sample_table_sql
