import pytest
import sqlglot

from featurebyte.query_graph import sql


@pytest.fixture(name="input_node")
def input_node_fixture():
    return sql.InputNode(
        columns=["col_1", "col_2", "col_3"],
        timestamp="ts",
        input=sql.ExpressionNode(sqlglot.parse_one("dbtable")),
    )


def test_assign_node__replace(input_node):
    # replace an existing column
    node = sql.AssignNode(table=input_node, column=sql.Project(columns=["a"]), name="col_1")
    assert node.columns == ["col_2", "col_3", "col_1"]


def test_assign_node__new_column(input_node):
    # add a new column
    node = sql.AssignNode(table=input_node, column=sql.Project(columns=["a"]), name="col_11")
    assert node.columns == ["col_1", "col_2", "col_3", "col_11"]
