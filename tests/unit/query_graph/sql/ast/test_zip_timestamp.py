"""
Test cases for ZipTimestampTZTupleNode
"""

import pytest

from featurebyte.query_graph.model.timestamp_schema import (
    TimestampSchema,
    TimeZoneColumn,
)
from featurebyte.query_graph.sql.ast.generic import ParsedExpressionNode
from featurebyte.query_graph.sql.ast.zip_timestamp import ZipTimestampTZTupleNode
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY
from tests.unit.query_graph.util import make_context
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture(params=SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY)
def source_type(request):
    """
    Fixture to parametrize source types
    """
    return request.param


def test_zip_timestamp_sql(input_node, source_type, update_fixtures):
    """
    Test ZipTimestampTZTupleNode sql generation
    """
    timestamp_node = ParsedExpressionNode(
        context=make_context(), table_node=input_node, expr=quoted_identifier("timestamp_col")
    )
    timezone_offset_node = ParsedExpressionNode(
        context=make_context(), table_node=input_node, expr=quoted_identifier("tz_col")
    )
    input_nodes = [timestamp_node, timezone_offset_node]
    context = make_context(
        parameters={
            "timestamp_schema": TimestampSchema(
                timezone=TimeZoneColumn(column_name="tz_col", type="timezone"),
            ),
        },
        input_sql_nodes=input_nodes,
        source_type=source_type,
    )

    node = ZipTimestampTZTupleNode.build(context)
    actual = sql_to_string(node.sql, source_type)
    fixture_filename = (
        f"tests/fixtures/query_graph/test_zip_timestamp/zip_timestamp_{source_type}.sql"
    )
    assert_equal_with_expected_fixture(
        actual,
        fixture_filename,
        update_fixtures,
    )
