"""
Tests for the featurebyte.query_graph.sql module
"""
import textwrap

import pytest
import sqlglot

from featurebyte.query_graph import sql
from featurebyte.query_graph.enum import NodeType


@pytest.fixture(name="input_node")
def input_node_fixture():
    """Fixture for a generic InputNode"""
    columns_map = {
        "col_1": sqlglot.parse_one("col_1"),
        "col_2": sqlglot.parse_one("col_2"),
        "col_3": sqlglot.parse_one("col_3"),
    }
    return sql.GenericInputNode(
        columns_map=columns_map,
        column_names=["col_1", "col_2", "col_3"],
        dbtable={
            "database_name": "my_database",
            "schema_name": "my_schema",
            "table_name": "my_table",
        },
        feature_store={
            "type": "snowflake",
            "details": {
                "database": "db",
                "sf_schema": "public",
            },
        },
    )


@pytest.mark.parametrize(
    "node_type, expected",
    [
        (NodeType.ADD, "(a + b)"),
        (NodeType.SUB, "(a - b)"),
        (NodeType.MUL, "(a * b)"),
        (NodeType.DIV, "(a / NULLIF(b, 0))"),
        (NodeType.EQ, "(a = b)"),
        (NodeType.NE, "(a <> b)"),
        (NodeType.LT, "(a < b)"),
        (NodeType.LE, "(a <= b)"),
        (NodeType.GT, "(a > b)"),
        (NodeType.GE, "(a >= b)"),
        (NodeType.AND, "(a AND b)"),
        (NodeType.OR, "(a OR b)"),
    ],
)
def test_binary_operation_node__series(node_type, expected, input_node):
    """Test binary operation node when another side is Series"""
    column1 = sql.StrExpressionNode(table_node=input_node, expr="a")
    column2 = sql.StrExpressionNode(table_node=input_node, expr="b")
    input_nodes = [column1, column2]
    parameters = {}
    node = sql.make_binary_operation_node(node_type, input_nodes, parameters)
    assert node.sql.sql() == expected


def test_binary_operation_node__consecutive_ops_1(input_node):
    """Test multiple binary operations"""
    col_a = sql.StrExpressionNode(table_node=input_node, expr="a")
    col_b = sql.StrExpressionNode(table_node=input_node, expr="b")
    col_c = sql.StrExpressionNode(table_node=input_node, expr="c")
    a_plus_b = sql.make_binary_operation_node(NodeType.ADD, [col_a, col_b], {})
    a_plus_b_div_c = sql.make_binary_operation_node(NodeType.DIV, [a_plus_b, col_c], {})
    assert a_plus_b_div_c.sql.sql() == "((a + b) / NULLIF(c, 0))"


def test_binary_operation_node__consecutive_ops_2(input_node):
    """Test multiple binary operations"""
    col_a = sql.StrExpressionNode(table_node=input_node, expr="a")
    col_b = sql.StrExpressionNode(table_node=input_node, expr="b")
    col_c = sql.StrExpressionNode(table_node=input_node, expr="c")
    a_plus_b = sql.make_binary_operation_node(NodeType.ADD, [col_a, col_b], {})
    c_div_a_plus_b = sql.make_binary_operation_node(NodeType.DIV, [col_c, a_plus_b], {})
    assert c_div_a_plus_b.sql.sql() == "(c / NULLIF((a + b), 0))"


@pytest.mark.parametrize(
    "node_type, value, right_op, expected",
    [
        (NodeType.ADD, 1, False, "(a + 1)"),
        (NodeType.ADD, 1, True, "(1 + a)"),
        (NodeType.SUB, 1, False, "(a - 1)"),
        (NodeType.SUB, 1, True, "(1 - a)"),
        (NodeType.MUL, 1.0, False, "(a * 1.0)"),
        (NodeType.MUL, 1.0, True, "(1.0 * a)"),
        (NodeType.DIV, 1.0, False, "(a / NULLIF(1.0, 0))"),
        (NodeType.DIV, 1.0, True, "(1.0 / NULLIF(a, 0))"),
        (NodeType.EQ, "apple", False, "(a = 'apple')"),
    ],
)
def test_binary_operation_node__scalar(node_type, value, right_op, expected, input_node):
    """Test binary operation node when another side is scalar"""
    column1 = sql.StrExpressionNode(table_node=input_node, expr="a")
    input_nodes = [column1]
    parameters = {"value": value, "right_op": right_op}
    node = sql.make_binary_operation_node(node_type, input_nodes, parameters)
    assert node.sql.sql() == expected


def test_make_input_node_escape_special_characters():
    """Test input node quotes all identifiers to handle special characters"""
    parameters = {
        "columns": ["SUM(a)", "b", "c"],
        "dbtable": {"database_name": "db", "schema_name": "public", "table_name": "my_table"},
        "feature_store": {
            "type": "snowflake",
            "details": {
                "database": "db",
                "sf_schema": "public",
            },
        },
    }
    node = sql.make_input_node(parameters=parameters, sql_type=sql.SQLType.EVENT_VIEW_PREVIEW)
    expected = textwrap.dedent(
        """
        SELECT
          "SUM(a)" AS "SUM(a)",
          "b" AS "b",
          "c" AS "c"
        FROM "db"."public"."my_table"
        """
    ).strip()
    assert node.sql.sql(pretty=True) == expected


@pytest.mark.parametrize(
    "parameters, expected",
    [
        ({"transform_type": "entropy"}, "F_COUNT_DICT_ENTROPY(cd_val)"),
        (
            {"transform_type": "most_frequent"},
            "CAST(F_COUNT_DICT_MOST_FREQUENT(cd_val) AS VARCHAR)",
        ),
        ({"transform_type": "unique_count"}, "F_COUNT_DICT_NUM_UNIQUE(cd_val)"),
        (
            {"transform_type": "unique_count", "include_missing": False},
            "F_COUNT_DICT_NUM_UNIQUE(OBJECT_DELETE(cd_val, '__MISSING__'))",
        ),
    ],
)
def test_count_dict_transform(parameters, expected, input_node):
    """Test binary operation node when another side is Series"""
    column = sql.StrExpressionNode(table_node=input_node, expr="cd_val")
    node = sql.make_expression_node(
        input_sql_nodes=[column],
        node_type=NodeType.COUNT_DICT_TRANSFORM,
        parameters=parameters,
    )
    assert node.sql.sql() == expected
