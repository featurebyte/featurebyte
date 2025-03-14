"""
Tests for DatetimeExtractNode
"""

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.model.timestamp_schema import (
    ExtendedTimestampSchema,
    TimestampSchema,
    TimestampTupleSchema,
    TimeZoneColumn,
    TimezoneOffsetSchema,
)
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType, sql_to_string
from featurebyte.query_graph.sql.source_info import SourceInfo
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture(params=SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY)
def source_type(request):
    """
    Fixture to parametrize source types
    """
    return request.param


def test_event_table_no_timezone_offset(global_graph, input_details, source_type, update_fixtures):
    """
    Test datetime extract on event table
    """
    node_params = {
        "type": "event_table",
        "columns": [
            {"name": "ts", "dtype": DBVarType.TIMESTAMP},
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
    timestamp_column_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={
            "columns": ["ts"],
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    dt_extract_node = global_graph.add_operation(
        node_type=NodeType.DT_EXTRACT,
        node_params={
            "property": "hour",
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[timestamp_column_node],
    )
    assign_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={
            "name": "hour",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node, dt_extract_node],
    )
    source_info = SourceInfo(
        database_name="my_db", schema_name="my_schema", source_type=source_type
    )
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.AGGREGATION, source_info=source_info
    )
    actual = sql_to_string(sql_graph.build(assign_node).sql, source_type)
    fixture_filename = f"tests/fixtures/query_graph/test_datetime_extract/event_table_no_timezone_offset_{source_type}.sql"
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)


def test_event_table_timezone_offset_literal(
    global_graph, input_details, source_type, update_fixtures
):
    """
    Test datetime extract on event table with timezone offset literal
    """
    node_params = {
        "type": "event_table",
        "columns": [
            {"name": "ts", "dtype": DBVarType.TIMESTAMP},
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
    timestamp_column_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={
            "columns": ["ts"],
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    dt_extract_node = global_graph.add_operation(
        node_type=NodeType.DT_EXTRACT,
        node_params={
            "property": "hour",
            "timezone_offset": "+08:00",
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[timestamp_column_node],
    )
    assign_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={
            "name": "hour",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node, dt_extract_node],
    )
    source_info = SourceInfo(
        database_name="my_db", schema_name="my_schema", source_type=source_type
    )
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.AGGREGATION, source_info=source_info
    )
    actual = sql_to_string(sql_graph.build(assign_node).sql, source_type)
    fixture_filename = f"tests/fixtures/query_graph/test_datetime_extract/event_table_timezone_offset_literal_{source_type}.sql"
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)


def test_event_table_timezone_offset_column(
    global_graph, input_details, source_type, update_fixtures
):
    """
    Test datetime extract on event table with timezone offset column
    """
    node_params = {
        "type": "event_table",
        "columns": [
            {"name": "ts", "dtype": DBVarType.TIMESTAMP},
            {"name": "tz_offset", "dtype": DBVarType.VARCHAR},
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
    timestamp_column_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={
            "columns": ["ts"],
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    timestamp_offset_column_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={
            "columns": ["tz_offset"],
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    dt_extract_node = global_graph.add_operation(
        node_type=NodeType.DT_EXTRACT,
        node_params={
            "property": "hour",
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[timestamp_column_node, timestamp_offset_column_node],
    )
    assign_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={
            "name": "hour",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node, dt_extract_node],
    )
    source_info = SourceInfo(
        database_name="my_db", schema_name="my_schema", source_type=source_type
    )
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.AGGREGATION, source_info=source_info
    )
    actual = sql_to_string(sql_graph.build(assign_node).sql, source_type)
    fixture_filename = f"tests/fixtures/query_graph/test_datetime_extract/event_table_timezone_offset_column_{source_type}.sql"
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)


def test_time_series_table_timezone_offset_literal(
    global_graph, input_details, source_type, update_fixtures
):
    """
    Test datetime extract on a time series table with timezone offset literal
    """
    reference_datetime_schema = TimestampSchema(
        format_string="%Y-%m-%d %H:%M:%S",
        timezone="Asia/Singapore",
    ).model_dump()
    node_params = {
        "type": "time_series_table",
        "columns": [
            {"name": "ts", "dtype": DBVarType.VARCHAR},
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "a", "dtype": DBVarType.FLOAT},
        ],
        "reference_datetime_column": "ts",
        "reference_datetime_schema": reference_datetime_schema,
        "time_interval": {"unit": "DAY", "value": 1},
    }
    node_params.update(input_details)
    input_node = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    timestamp_column_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={
            "columns": ["ts"],
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    dt_extract_node = global_graph.add_operation(
        node_type=NodeType.DT_EXTRACT,
        node_params={
            "property": "hour",
            "timestamp_metadata": DBVarTypeMetadata(
                timestamp_schema=reference_datetime_schema
            ).model_dump(),
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[timestamp_column_node],
    )
    assign_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={
            "name": "hour",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node, dt_extract_node],
    )
    source_info = SourceInfo(
        database_name="my_db", schema_name="my_schema", source_type=source_type
    )
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.AGGREGATION, source_info=source_info
    )
    actual = sql_to_string(sql_graph.build(assign_node).sql, source_type)
    fixture_filename = f"tests/fixtures/query_graph/test_datetime_extract/time_series_table_timezone_offset_literal_{source_type}.sql"
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)


def test_time_series_table_timezone_offset_column_utc(
    global_graph, input_details, source_type, update_fixtures
):
    """
    Test datetime extract on a time series table with timezone offset column with is_utc_time=True
    """
    reference_datetime_schema = TimestampSchema(
        format_string="%Y-%m-%d %H:%M:%S",
        timezone=TimeZoneColumn(column_name="tz_offset", type="timezone"),
        is_utc_time=True,
    ).model_dump()
    node_params = {
        "type": "time_series_table",
        "columns": [
            {"name": "ts", "dtype": DBVarType.VARCHAR},
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "a", "dtype": DBVarType.FLOAT},
        ],
        "reference_datetime_column": "ts",
        "reference_datetime_schema": reference_datetime_schema,
        "time_interval": {"unit": "DAY", "value": 1},
    }
    node_params.update(input_details)
    input_node = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    timestamp_column_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={
            "columns": ["ts"],
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    dt_extract_node = global_graph.add_operation(
        node_type=NodeType.DT_EXTRACT,
        node_params={
            "property": "hour",
            "timestamp_metadata": DBVarTypeMetadata(
                timestamp_schema=reference_datetime_schema
            ).model_dump(),
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[timestamp_column_node],
    )
    assign_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={
            "name": "hour",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node, dt_extract_node],
    )
    source_info = SourceInfo(
        database_name="my_db", schema_name="my_schema", source_type=source_type
    )
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.AGGREGATION, source_info=source_info
    )
    actual = sql_to_string(sql_graph.build(assign_node).sql, source_type)
    fixture_filename = f"tests/fixtures/query_graph/test_datetime_extract/time_series_table_timezone_offset_column_utc_{source_type}.sql"
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)


def test_timestamp_tuple_schema(global_graph, input_details, source_type, update_fixtures):
    """
    Test datetime extract on a timestamp tuple column with timezone offset literal
    """
    reference_datetime_schema = TimestampSchema(
        format_string="%Y-%m-%d %H:%M:%S",
        timezone=TimeZoneColumn(column_name="tz_offset", type="timezone"),
        is_utc_time=True,
    ).model_dump()
    timestamp_tuple_schema = TimestampTupleSchema(
        timestamp_schema=ExtendedTimestampSchema(
            dtype=DBVarType.VARCHAR, **reference_datetime_schema
        ),
        timezone_offset_schema=TimezoneOffsetSchema(dtype=DBVarType.VARCHAR),
    )
    node_params = {
        "type": "time_series_table",
        "columns": [
            {"name": "ts", "dtype": DBVarType.VARCHAR},
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "a", "dtype": DBVarType.FLOAT},
        ],
        "reference_datetime_column": "ts",
        "reference_datetime_schema": reference_datetime_schema,
        "time_interval": {"unit": "DAY", "value": 1},
    }
    node_params.update(input_details)
    input_node = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    timestamp_column_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={
            "columns": ["ts"],
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    dt_extract_node = global_graph.add_operation(
        node_type=NodeType.DT_EXTRACT,
        node_params={
            "property": "hour",
            "timestamp_metadata": DBVarTypeMetadata(
                timestamp_tuple_schema=timestamp_tuple_schema,
            ).model_dump(),
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[timestamp_column_node],
    )
    assign_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={
            "name": "hour",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node, dt_extract_node],
    )
    source_info = SourceInfo(
        database_name="my_db", schema_name="my_schema", source_type=source_type
    )
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.AGGREGATION, source_info=source_info
    )
    actual = sql_to_string(sql_graph.build(assign_node).sql, source_type)
    fixture_filename = (
        f"tests/fixtures/query_graph/test_datetime_extract/timestamp_tuple_schema_{source_type}.sql"
    )
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)
