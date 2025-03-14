"""
Tests for DatabricksUnityAdapter
"""

import pytest

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter import DatabricksUnityAdapter
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.source_info import SourceInfo
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.mark.parametrize(
    "udf_name,params",
    [
        ("VECTOR_AGGREGATE_MAX", [quoted_identifier("arr")]),
        ("VECTOR_AGGREGATE_SUM", [quoted_identifier("arr")]),
        ("VECTOR_AGGREGATE_AVG", [quoted_identifier("arr"), quoted_identifier("count")]),
        ("VECTOR_AGGREGATE_SIMPLE_AVERAGE", [quoted_identifier("arr")]),
    ],
)
def test_call_vector_aggregation_function(udf_name, params, update_fixtures):
    """
    Test call_vector_aggregation_function
    """
    adapter = DatabricksUnityAdapter(
        SourceInfo(
            database_name="db", schema_name="schema", source_type=SourceType.DATABRICKS_UNITY
        )
    )
    output = sql_to_string(
        adapter.call_vector_aggregation_function(
            udf_name,
            params,
        ),
        SourceType.DATABRICKS_UNITY,
    )
    assert_equal_with_expected_fixture(
        output,
        f"tests/fixtures/adapter/databricks_unity/expected_{udf_name.lower()}.sql",
        update_fixtures,
    )
