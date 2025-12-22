"""
Test preview service module
"""

import textwrap
from datetime import datetime
from decimal import Decimal
from typing import AsyncGenerator
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import pytest_asyncio
from bson import ObjectId
from sqlglot import parse_one

from featurebyte import FeatureStore, SourceType
from featurebyte.exception import (
    DescribeQueryExecutionError,
    MissingPointInTimeColumnError,
    RequiredEntityNotProvidedError,
)
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.development_dataset import DevelopmentDatasetModel, DevelopmentTable
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
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
                            {
                                "name": "partition_col",
                                "dtype": "VARCHAR",
                                "partition_metadata": {"is_partition_key": True},
                                "dtype_metadata": {
                                    "timestamp_schema": {"format_string": "%Y-%m-%d"},
                                },
                            },
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


@pytest_asyncio.fixture(name="development_dataset")
async def development_dataset_fixture(
    development_dataset_service,
    feature_store_preview,
):
    """
    Fixture for a DevelopmentDatasetModel
    """
    input_node = feature_store_preview.graph.get_node_by_name("input_1")
    table_id = input_node.parameters.id
    development_dataset = DevelopmentDatasetModel(
        sample_from_timestamp=datetime(2023, 1, 1, 0, 0, 0),
        sample_to_timestamp=datetime(2023, 6, 1, 0, 0, 0),
        development_tables=[
            DevelopmentTable(
                table_id=table_id,
                location=TabularSource(
                    feature_store_id=ObjectId(),
                    table_details=TableDetails(
                        database_name="db",
                        schema_name="schema",
                        table_name="dev_table",
                    ),
                ),
            )
        ],
    )
    # Patch the validation method to avoid unnecessary checks
    with patch(
        "featurebyte.service.development_dataset.DevelopmentDatasetService._validate_development_tables"
    ):
        return await development_dataset_service.create_document(development_dataset)


@pytest.fixture(name="feature_store_sample_with_time_range")
def feature_store_sample_with_time_range_fixture(feature_store_sample, development_dataset):
    """
    Fixture for a FeatureStoreSample with a time range
    """
    return FeatureStoreSample(
        **feature_store_sample.model_dump(
            exclude={"from_timestamp", "to_timestamp", "timestamp_column", "development_dataset_id"}
        ),
        from_timestamp=datetime(2023, 1, 1),
        to_timestamp=datetime(2025, 1, 1),
        timestamp_column="event_timestamp",
        development_dataset_id=development_dataset.id,
    )


@pytest.fixture(name="scd_join_sample_with_time_range")
def scd_join_sample_with_time_range_fixture(feature_store):
    """
    Fixture for an SCD join sample with a time range
    """
    graph_dict = {
        "edges": [
            {"source": "input_1", "target": "graph_1"},
            {"source": "input_2", "target": "graph_2"},
            {"source": "graph_1", "target": "join_1"},
            {"source": "graph_2", "target": "join_1"},
        ],
        "nodes": [
            {
                "name": "input_1",
                "type": "input",
                "output_type": "frame",
                "parameters": {
                    "columns": [
                        {
                            "name": "event_partition_col",
                            "dtype": "VARCHAR",
                            "partition_metadata": {"is_partition_key": True},
                            "dtype_metadata": {
                                "timestamp_schema": {"format_string": "%Y-%m-%d"},
                            },
                        },
                        {
                            "name": "ts",
                            "dtype": "TIMESTAMP",
                            "dtype_metadata": None,
                            "partition_metadata": None,
                        },
                        {
                            "name": "cust_id",
                            "dtype": "INT",
                            "dtype_metadata": None,
                            "partition_metadata": None,
                        },
                        {
                            "name": "event_id",
                            "dtype": "INT",
                            "dtype_metadata": None,
                            "partition_metadata": None,
                        },
                    ],
                    "table_details": {
                        "database_name": "sf_database",
                        "schema_name": "sf_schema",
                        "table_name": "EVENT",
                    },
                    "feature_store_details": {"type": "snowflake", "details": None},
                    "type": "event_table",
                    "id": "6867f9d7b6058fa2167035d0",
                    "timestamp_column": "ts",
                    "id_column": "event_id",
                    "event_timestamp_timezone_offset": None,
                    "event_timestamp_timezone_offset_column": None,
                    "event_timestamp_schema": None,
                },
            },
            {
                "name": "graph_1",
                "type": "graph",
                "output_type": "frame",
                "parameters": {
                    "graph": {
                        "edges": [{"source": "proxy_input_1", "target": "project_1"}],
                        "nodes": [
                            {
                                "name": "proxy_input_1",
                                "type": "proxy_input",
                                "output_type": "frame",
                                "parameters": {"input_order": 0},
                            },
                            {
                                "name": "project_1",
                                "type": "project",
                                "output_type": "frame",
                                "parameters": {"columns": ["ts", "cust_id", "event_id"]},
                            },
                        ],
                    },
                    "output_node_name": "project_1",
                    "type": "event_view",
                    "metadata": {
                        "view_mode": "auto",
                        "drop_column_names": [],
                        "column_cleaning_operations": [],
                        "table_id": "6867f9d7b6058fa2167035d0",
                    },
                },
            },
            {
                "name": "input_2",
                "type": "input",
                "output_type": "frame",
                "parameters": {
                    "columns": [
                        {
                            "name": "scd_partition_col",
                            "dtype": "VARCHAR",
                            "partition_metadata": {"is_partition_key": True},
                            "dtype_metadata": {
                                "timestamp_schema": {"format_string": "%Y-%m-%d"},
                            },
                        },
                        {
                            "name": "effective_ts",
                            "dtype": "TIMESTAMP",
                            "dtype_metadata": None,
                            "partition_metadata": None,
                        },
                        {
                            "name": "end_ts",
                            "dtype": "VARCHAR",
                            "dtype_metadata": {
                                "timestamp_schema": {
                                    "format_string": "YYYY|MM|DD|HH24:MI:SS",
                                    "is_utc_time": True,
                                    "timezone": None,
                                },
                                "timestamp_tuple_schema": None,
                            },
                            "partition_metadata": None,
                        },
                        {
                            "name": "scd_cust_id",
                            "dtype": "INT",
                            "dtype_metadata": None,
                            "partition_metadata": None,
                        },
                        {
                            "name": "scd_value",
                            "dtype": "INT",
                            "dtype_metadata": None,
                            "partition_metadata": None,
                        },
                    ],
                    "table_details": {
                        "database_name": "sf_database",
                        "schema_name": "sf_schema",
                        "table_name": "SCD",
                    },
                    "feature_store_details": {"type": "snowflake", "details": None},
                    "type": "scd_table",
                    "id": "6867f9d9b6058fa2167035dc",
                    "natural_key_column": "scd_cust_id",
                    "effective_timestamp_column": "effective_ts",
                    "surrogate_key_column": None,
                    "end_timestamp_column": "end_ts",
                    "current_flag_column": None,
                },
            },
            {
                "name": "graph_2",
                "type": "graph",
                "output_type": "frame",
                "parameters": {
                    "graph": {
                        "edges": [{"source": "proxy_input_1", "target": "project_1"}],
                        "nodes": [
                            {
                                "name": "proxy_input_1",
                                "type": "proxy_input",
                                "output_type": "frame",
                                "parameters": {"input_order": 0},
                            },
                            {
                                "name": "project_1",
                                "type": "project",
                                "output_type": "frame",
                                "parameters": {
                                    "columns": [
                                        "effective_ts",
                                        "end_ts",
                                        "scd_cust_id",
                                        "scd_value",
                                    ]
                                },
                            },
                        ],
                    },
                    "output_node_name": "project_1",
                    "type": "scd_view",
                    "metadata": {
                        "view_mode": "auto",
                        "drop_column_names": [],
                        "column_cleaning_operations": [],
                        "table_id": "6867f9d9b6058fa2167035dc",
                    },
                },
            },
            {
                "name": "join_1",
                "type": "join",
                "output_type": "frame",
                "parameters": {
                    "left_on": "cust_id",
                    "right_on": "scd_cust_id",
                    "left_input_columns": ["ts", "cust_id", "event_id"],
                    "left_output_columns": ["ts", "cust_id", "event_id"],
                    "right_input_columns": ["scd_value"],
                    "right_output_columns": ["scd_value_latest"],
                    "join_type": "left",
                    "scd_parameters": {
                        "effective_timestamp_column": "effective_ts",
                        "natural_key_column": "scd_cust_id",
                        "current_flag_column": None,
                        "end_timestamp_column": "end_ts",
                        "effective_timestamp_metadata": None,
                        "end_timestamp_metadata": {
                            "timestamp_schema": {
                                "format_string": "YYYY|MM|DD|HH24:MI:SS",
                                "is_utc_time": True,
                                "timezone": None,
                            },
                            "timestamp_tuple_schema": None,
                        },
                        "left_timestamp_column": "ts",
                        "left_timestamp_metadata": None,
                    },
                    "metadata": {"type": "join", "rsuffix": "_latest", "rprefix": ""},
                },
            },
        ],
    }
    return FeatureStoreSample(
        graph=graph_dict,
        node_name="join_1",
        from_timestamp=datetime(2023, 1, 1),
        to_timestamp=datetime(2025, 1, 1),
        timestamp_column="ts",
    )


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


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_preview_featurelist__missing_entity(
    feature_preview_service,
    production_ready_feature_list,
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
    result = await preview_service.describe(
        feature_store_sample,
        size=5000,
        seed=0,
        drop_all_null_stats=drop_all_null_stats,
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
        "count": [100],
    })

    async def mock_get_async_query_generator(
        self, *args, **kwargs
    ) -> AsyncGenerator[pa.RecordBatch, None]:
        yield pa.RecordBatch.from_pandas(
            pd.DataFrame({
                "col_int": [1] * 100 + [2] * 50 + [None] * 3,
                "col_float": [1.0] * 100 + [2.0] * 50 + [None] * 3,
                "col_char": ["a"] * 100 + ["b"] * 50 + [None] * 3,
            })
        )

    mock_snowflake_session.get_async_query_generator.side_effect = mock_get_async_query_generator

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
        "col_char": {"a": 100, "b": 50, None: 3},
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
        "count": [100],
    })

    async def mock_get_async_query_generator(
        self, *args, **kwargs
    ) -> AsyncGenerator[pa.RecordBatch, None]:
        yield pa.RecordBatch.from_pandas(
            pd.DataFrame({
                "col_int": [1] * 100 + [2] * 50 + [None] * 3,
                "col_float": [1.0] * 100 + [2.0] * 50 + [None] * 3,
                "col_text": ["a"] * 100 + ["b"] * 50 + [None] * 3,
            })
        )

    mock_snowflake_session.get_async_query_generator.side_effect = mock_get_async_query_generator
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
          "cust_id",
          "partition_col"
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
            "cust_id",
            "partition_col"
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
              CAST("cust_id" AS VARCHAR) AS "cust_id",
              CAST("partition_col" AS VARCHAR) AS "partition_col"
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
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "count": [100],
    })

    async def mock_get_async_query_generator(
        self, *args, **kwargs
    ) -> AsyncGenerator[pa.RecordBatch, None]:
        yield pa.RecordBatch.from_pandas(
            pd.DataFrame({
                "col_int": [1] * 100 + [2] * 50 + [None] * 3,
                "col_float": [1.0] * 100 + [2.0] * 50 + [None] * 3,
                "col_char": ["a"] * 100 + ["b"] * 50 + [None] * 3,
            })
        )

    mock_snowflake_session.get_async_query_generator.side_effect = mock_get_async_query_generator
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


@pytest.mark.asyncio
async def test_sample_with_partition_column_filters(
    preview_service, feature_store_sample_with_time_range, mock_snowflake_session, update_fixtures
):
    """Test sample with sample_on_primary_table and partition column filtering enabled"""
    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    seed = 1234

    await preview_service.sample(
        feature_store_sample_with_time_range,
        size=10,
        seed=seed,
        sample_on_primary_table=True,
    )
    queries = extract_session_executed_queries(mock_snowflake_session)
    fixture_filename = (
        "tests/fixtures/preview_service/expected_sample_with_partition_column_filters.sql"
    )
    assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)


@pytest.mark.asyncio
async def test_describe_with_partition_column_filters(
    preview_service, feature_store_sample_with_time_range, mock_snowflake_session, update_fixtures
):
    """Test describe with sample_on_primary_table and partition column filtering enabled"""
    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    seed = 1234

    await preview_service.describe(
        feature_store_sample_with_time_range,
        size=10,
        seed=seed,
        sample_on_primary_table=True,
    )
    queries = extract_session_executed_queries(mock_snowflake_session)
    fixture_filename = (
        "tests/fixtures/preview_service/expected_describe_with_partition_column_filters.sql"
    )
    assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)


@pytest.mark.asyncio
async def test_value_counts_with_partition_column_filters(
    preview_service, feature_store_sample_with_time_range, mock_snowflake_session, update_fixtures
):
    """Test value_counts with sample_on_primary_table and partition column filtering enabled"""
    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "count": [100],
    })

    async def mock_get_async_query_generator(
        self, *args, **kwargs
    ) -> AsyncGenerator[pa.RecordBatch, None]:
        yield pa.RecordBatch.from_pandas(
            pd.DataFrame({
                "col_char": ["A", "B", "C", "D", "E"],
            })
        )

    mock_snowflake_session.get_async_query_generator.side_effect = mock_get_async_query_generator
    seed = 1234

    await preview_service.value_counts(
        feature_store_sample_with_time_range,
        column_names=["col_char"],
        num_rows=50,
        num_categories_limit=5,
        seed=seed,
        sample_on_primary_table=True,
    )
    queries = extract_session_executed_queries(mock_snowflake_session)
    fixture_filename = (
        "tests/fixtures/preview_service/expected_value_counts_with_partition_column_filters.sql"
    )
    assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)


@pytest.mark.asyncio
async def test_scd_join_with_partition_column_filters(
    preview_service, scd_join_sample_with_time_range, mock_snowflake_session, update_fixtures
):
    """Test partition column filtering is applied only on primary table"""
    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    seed = 1234

    await preview_service.sample(
        scd_join_sample_with_time_range,
        size=10,
        seed=seed,
        sample_on_primary_table=True,
    )
    queries = extract_session_executed_queries(mock_snowflake_session)
    fixture_filename = (
        "tests/fixtures/preview_service/expected_scd_join_with_partition_column_filters.sql"
    )
    assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)


@pytest.mark.asyncio
async def test_decimal_columns_in_preview(
    preview_service, feature_store_preview, mock_snowflake_session
):
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "col_decimal_int": [Decimal(1), Decimal(2), np.nan],
        "col_decimal_float": [Decimal(1.23), Decimal(4.56), np.nan],
    })
    df = await preview_service.preview(feature_store_preview, 10)
    assert df.col_decimal_int.dtype == float
    assert df.col_decimal_float.dtype == float
    assert np.isclose(df.col_decimal_int.iloc[0], 1.0)
    assert np.isclose(df.col_decimal_float.iloc[0], 1.23)


@pytest.mark.asyncio
@patch("featurebyte.service.preview.PreviewService._describe")
async def test_decimal_columns_in_describe(mock_describe, preview_service, feature_store_sample):
    mock_describe.return_value = (
        pd.DataFrame(
            {
                "column": ["a", Decimal(1), Decimal(1.23)],
            },
            index=["top", "min", "max"],
        ),
        None,
    )
    df = await preview_service.describe(
        feature_store_sample,
        size=5000,
        seed=0,
    )
    assert np.isclose(df.column["min"], 1.0)
    assert np.isclose(df.column["max"], 1.23)
