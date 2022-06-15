"""
Tests for the featurebyte.query_graph.sql module
"""
import textwrap

import pytest

from featurebyte.query_graph import sql
from featurebyte.query_graph.enum import NodeType


@pytest.fixture(name="input_node")
def input_node_fixture():
    """Fixture for a generic InputNode"""
    return sql.GenericInputNode(
        column_names=["col_1", "col_2", "col_3"],
        timestamp="ts",
        dbtable="dbtable",
    )


def test_assign_node__replace(input_node):
    """Test assign node replacing an existing column"""
    node = sql.AssignNode(
        table_node=input_node,
        column_node=sql.Project(table_node=input_node, column_name="a"),
        name="col_1",
    )
    assert node.columns == ["col_2", "col_3", "col_1"]


def test_assign_node__new_column(input_node):
    """Test assign node adding a new column"""
    node = sql.AssignNode(
        table_node=input_node,
        column_node=sql.Project(table_node=input_node, column_name="a"),
        name="col_11",
    )
    assert node.columns == ["col_1", "col_2", "col_3", "col_11"]


@pytest.mark.parametrize(
    "node_type, expected",
    [
        (NodeType.ADD, "a + b"),
        (NodeType.SUB, "a - b"),
        (NodeType.MUL, "a * b"),
        (NodeType.DIV, "a / b"),
        (NodeType.EQ, "a = b"),
        (NodeType.NE, "a <> b"),
        (NodeType.LT, "a < b"),
        (NodeType.LE, "a <= b"),
        (NodeType.GT, "a > b"),
        (NodeType.GE, "a >= b"),
        (NodeType.AND, "a AND b"),
        (NodeType.OR, "a OR b"),
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


@pytest.mark.parametrize(
    "node_type, value, right_op, expected",
    [
        (NodeType.ADD, 1, False, "a + 1"),
        (NodeType.ADD, 1, True, "1 + a"),
        (NodeType.SUB, 1, False, "a - 1"),
        (NodeType.SUB, 1, True, "1 - a"),
        (NodeType.MUL, 1.0, False, "a * 1.0"),
        (NodeType.MUL, 1.0, True, "1.0 * a"),
        (NodeType.DIV, 1.0, False, "a / 1.0"),
        (NodeType.DIV, 1.0, True, "1.0 / a"),
        (NodeType.EQ, "apple", False, "a = 'apple'"),
    ],
)
def test_binary_operation_node__scalar(node_type, value, right_op, expected, input_node):
    """Test binary operation node when another side is scalar"""
    column1 = sql.StrExpressionNode(table_node=input_node, expr="a")
    input_nodes = [column1]
    parameters = {"value": value, "right_op": right_op}
    node = sql.make_binary_operation_node(node_type, input_nodes, parameters)
    assert node.sql.sql() == expected


def test_project_node__special_chars(input_node):
    """Test project node with names colliding with SQL keywords"""
    node = sql.ProjectMulti(
        input_node=input_node,
        column_names=["col", "SUM(a)", "some name"],
    )
    expected = (
        'SELECT "col", "SUM(a)", "some name" FROM (SELECT "col_1", "col_2", "col_3" FROM "dbtable")'
    )
    assert node.sql.sql() == expected


def test_project__escape_names(input_node):
    """Test project node with names colliding with SQL keywords"""
    node = sql.Project(table_node=input_node, column_name="SUM(a)")
    assert node.sql.sql() == '"SUM(a)"'
    assert node.sql_standalone.sql() == (
        'SELECT "SUM(a)" FROM (SELECT "col_1", "col_2", "col_3" FROM "dbtable")'
    )


def test_project_multi__escape_names(input_node):
    """Test project node with names colliding with SQL keywords"""
    node = sql.ProjectMulti(
        input_node=input_node,
        column_names=["col", "SUM(a)", "some name"],
    )
    expected = (
        'SELECT "col", "SUM(a)", "some name" FROM (SELECT "col_1", "col_2", "col_3" FROM "dbtable")'
    )
    assert node.sql.sql() == expected


def test_assign__escape_names(input_node):
    """Test assign node properly escape column names"""
    node = sql.AssignNode(
        table_node=input_node,
        column_node=sql.Project(table_node=input_node, column_name="SUM(a)"),
        name="COUNT(a)",
    )
    expected = textwrap.dedent(
        """
        SELECT
          "col_1",
          "col_2",
          "col_3",
          "SUM(a)" AS "COUNT(a)"
        FROM (
            SELECT
              "col_1",
              "col_2",
              "col_3"
            FROM "dbtable"
        )
        """
    ).strip()
    assert node.sql.sql(pretty=True) == expected
