"""
Test cases for DateDiffNode sql generation
"""

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.model.timestamp_schema import (
    ExtendedTimestampSchema,
    TimestampSchema,
    TimestampTupleSchema,
    TimezoneOffsetSchema,
)
from featurebyte.query_graph.sql.ast.datetime import DateDiffNode
from featurebyte.query_graph.sql.common import sql_to_string
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY
from tests.unit.query_graph.util import make_context, make_str_expression_node
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture(params=SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY)
def source_type(request):
    """
    Fixture to parametrize source types
    """
    return request.param


def test_date_difference(input_node):
    """Test DateDiff node"""
    column1 = make_str_expression_node(table_node=input_node, expr="a")
    column2 = make_str_expression_node(table_node=input_node, expr="b")
    input_nodes = [column1, column2]
    context = make_context(parameters={}, input_sql_nodes=input_nodes)
    node = DateDiffNode.build(context)
    assert node.sql.sql() == (
        "(DATEDIFF(a, b, MICROSECOND) * CAST(1 AS BIGINT) / CAST(1000000 AS BIGINT))"
    )


def test_date_difference_timestamp_schema(input_node):
    """Test DateDiff node handling of timestamp_schema"""
    column1 = make_str_expression_node(table_node=input_node, expr="a")
    column2 = make_str_expression_node(table_node=input_node, expr="b")
    input_nodes = [column1, column2]
    context = make_context(
        parameters={
            "left_timestamp_metadata": DBVarTypeMetadata(
                timestamp_schema=TimestampSchema(format_string="%Y-%m-%d")
            ).model_dump(),
            "right_timestamp_metadata": DBVarTypeMetadata(
                timestamp_schema=TimestampSchema(format_string="%Y|%m|%d")
            ).model_dump(),
        },
        input_sql_nodes=input_nodes,
    )
    node = DateDiffNode.build(context)
    assert node.sql.sql() == (
        "(DATEDIFF(TO_TIMESTAMP(a, '%Y-%m-%d'), TO_TIMESTAMP(b, '%Y|%m|%d'), MICROSECOND) * CAST(1 AS BIGINT) / CAST(1000000 AS BIGINT))"
    )


def test_date_difference_timestamp_tuple_schema(input_node, source_type, update_fixtures):
    """Test DateDiff node handling of timestamp tuple schema"""
    column1 = make_str_expression_node(table_node=input_node, expr="zipped_column_1")
    column2 = make_str_expression_node(table_node=input_node, expr="zipped_column_2")
    input_nodes = [column1, column2]
    context = make_context(
        parameters={
            "left_timestamp_metadata": DBVarTypeMetadata(
                timestamp_tuple_schema=TimestampTupleSchema(
                    timestamp_schema=ExtendedTimestampSchema(
                        dtype=DBVarType.VARCHAR,
                        format_string="%Y-%m-%d",
                    ),
                    timezone_offset_schema=TimezoneOffsetSchema(dtype=DBVarType.VARCHAR),
                )
            ).model_dump(),
            "right_timestamp_metadata": DBVarTypeMetadata(
                timestamp_tuple_schema=TimestampTupleSchema(
                    timestamp_schema=ExtendedTimestampSchema(
                        dtype=DBVarType.VARCHAR,
                        format_string="%Y|%m|%d",
                    ),
                    timezone_offset_schema=TimezoneOffsetSchema(dtype=DBVarType.VARCHAR),
                )
            ).model_dump(),
        },
        input_sql_nodes=input_nodes,
        source_type=source_type,
    )
    node = DateDiffNode.build(context)
    actual = sql_to_string(node.sql, source_type)
    fixture_filename = (
        f"tests/fixtures/query_graph/test_datetime_diff/timestamp_tuple_schema_{source_type}.sql"
    )
    assert_equal_with_expected_fixture(
        actual,
        fixture_filename,
        update_fixtures,
    )
