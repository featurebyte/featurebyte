"""
Tests for query_graph/sql/ast/input.py
"""

from datetime import datetime

import pytest
from bson import ObjectId

from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import (
    PartitionColumnFilter,
    PartitionColumnFilters,
    SQLType,
    sql_to_string,
)
from featurebyte.query_graph.sql.source_info import SourceInfo
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY)
@pytest.mark.parametrize(
    "test_case_name,dtype,timestamp_schema",
    [
        ("date", DBVarType.DATE, TimestampSchema(timezone="Asia/Singapore")),
    ],
)
def test_scd_timestamp_schema(
    global_graph,
    input_details,
    test_case_name,
    dtype,
    timestamp_schema,
    source_type,
    update_fixtures,
):
    """
    Test SCD timestamp schema
    """
    timestamp_schema_dict = {} if timestamp_schema is None else timestamp_schema.model_dump()
    node_params = {
        "type": "scd_table",
        "columns": [
            {
                **{"name": "ts", "dtype": dtype},
                **{"dtype_metadata": {"timestamp_schema": timestamp_schema_dict}},
            },
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "a", "dtype": DBVarType.FLOAT},
        ],
    }
    node_params.update(input_details)
    input_node = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    source_info = SourceInfo(
        database_name="my_db", schema_name="my_schema", source_type=source_type
    )
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.BUILD_TILE, source_info=source_info
    )
    actual = sql_to_string(sql_graph.build(input_node).sql, source_type)
    fixture_filename = (
        f"tests/fixtures/query_graph/test_input/scd_{test_case_name}_{source_type}.sql"
    )
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)


def test_partition_column_filters(global_graph, input_details, update_fixtures):
    """
    Test that partition column filters are applied correctly in the SQL AST for input nodes.
    """
    source_type = SourceType.DATABRICKS_UNITY
    node_params = {
        "id": ObjectId(),
        "type": "event_table",
        "columns": [
            {"name": "partition_col", "dtype": DBVarType.VARCHAR},
            {"name": "ts", "dtype": DBVarType.TIMESTAMP},
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "a", "dtype": DBVarType.FLOAT},
        ],
        "partition_column": "partition_col",
        "partition_column_metadata": DBVarTypeMetadata(
            timestamp_schema=TimestampSchema(format_string="%Y-%m-%d"),
        ).model_dump(),
    }
    node_params.update(input_details)
    input_node = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    source_info = SourceInfo(
        database_name="my_db", schema_name="my_schema", source_type=source_type
    )
    partition_column_filters = PartitionColumnFilters(
        mapping={
            node_params["id"]: PartitionColumnFilter(
                from_timestamp=datetime(2023, 1, 1),
                to_timestamp=datetime(2023, 6, 1),
            )
        }
    )
    sql_graph = SQLOperationGraph(
        global_graph,
        sql_type=SQLType.MATERIALIZE,
        partition_column_filters=partition_column_filters,
        source_info=source_info,
    )
    actual = sql_to_string(sql_graph.build(input_node).sql, source_type)
    fixture_filename = (
        f"tests/fixtures/query_graph/test_input/partition_column_filters_{source_type}.sql"
    )
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)
