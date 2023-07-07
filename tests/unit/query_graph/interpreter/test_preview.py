"""
Unit tests for describe query
"""
import pytest

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.mark.parametrize("source_type", [SourceType.SNOWFLAKE, SourceType.SPARK])
def test_graph_interpreter_describe(simple_graph, source_type, update_fixtures):
    """Test graph sample"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, source_type)

    sql_code = interpreter.construct_describe_sql(node.name, num_rows=10, seed=1234)[0]
    expected_filename = f"tests/fixtures/query_graph/expected_describe_{source_type.lower()}.sql"
    assert_equal_with_expected_fixture(sql_code, expected_filename, update_fixtures)
