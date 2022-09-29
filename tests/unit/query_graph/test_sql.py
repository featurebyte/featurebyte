"""
Tests for the featurebyte.query_graph.sql module
"""
import textwrap

import numpy as np
import pytest
from sqlglot import parse_one

from featurebyte.enum import DBVarType
from featurebyte.query_graph import sql
from featurebyte.query_graph.enum import NodeType


@pytest.fixture(name="input_node")
def input_node_fixture():
    """Fixture for a generic InputNode"""
    columns_map = {
        "col_1": parse_one("col_1"),
        "col_2": parse_one("col_2"),
        "col_3": parse_one("col_3"),
    }
    return sql.InputNode(
        columns_map=columns_map,
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


def test_input_node__subset_columns(input_node):
    """
    Test input node subset columns
    """
    expr_node_1 = sql.ParsedExpressionNode(input_node, parse_one("a + 1"))
    expr_node_2 = sql.ParsedExpressionNode(input_node, parse_one("a + 2"))
    input_node.assign_column("col_new_1", expr_node_1)
    input_node.assign_column("col_new_2", expr_node_2)

    # check subset node
    subset_node = input_node.subset_columns(["col_1", "col_new_1"])
    assert subset_node.columns_map == {"col_1": parse_one("col_1"), "col_new_1": parse_one("a + 1")}
    assert subset_node.columns_node == {"col_new_1": expr_node_1}
    assert subset_node.get_column_expr("col_1") == parse_one("col_1")
    assert subset_node.get_column_node("col_new_1") == expr_node_1
    assert subset_node.get_column_node("col_new_2") is None

    # check input node without subsetting
    assert input_node.columns_map == {
        "col_1": parse_one("col_1"),
        "col_2": parse_one("col_2"),
        "col_3": parse_one("col_3"),
        "col_new_1": parse_one("a + 1"),
        "col_new_2": parse_one("a + 2"),
    }
    assert input_node.columns_node == {"col_new_1": expr_node_1, "col_new_2": expr_node_2}


def test_resolve_project_node(input_node):
    """Test resolve_project_node helper"""
    expr_node = sql.ParsedExpressionNode(input_node, parse_one("a + 1"))
    input_node.assign_column("new_col", expr_node)
    project_node = sql.Project(input_node, "new_col")
    project_node_original_column = sql.Project(input_node, "col_1")
    assert sql.resolve_project_node(project_node) == expr_node
    assert sql.resolve_project_node(project_node_original_column) is None
    # no-op if not a Project node
    assert sql.resolve_project_node(expr_node) == expr_node


def test_make_assign_node(input_node):
    """Test make_assign_node"""
    expr_node = sql.ParsedExpressionNode(input_node, parse_one("a + 1"))
    result = sql.make_assign_node([input_node, expr_node], {"name": "new_col"})
    # should create a copy and not modify the original input node
    assert result is not input_node
    assert input_node.get_column_node("new_col") is None
    assert result.get_column_node("new_col") == expr_node


@pytest.mark.parametrize(
    "node_type, expected",
    [
        (NodeType.ADD, "(a + b)"),
        (NodeType.SUB, "(a - b)"),
        (NodeType.MUL, "(a * b)"),
        (NodeType.DIV, "(a / NULLIF(b, 0))"),
        (NodeType.MOD, "(a % NULLIF(b, 0))"),
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
        "table_details": {"database_name": "db", "schema_name": "public", "table_name": "my_table"},
        "feature_store_details": {
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
    """Test count dict transformation node"""
    column = sql.StrExpressionNode(table_node=input_node, expr="cd_val")
    node = sql.make_expression_node(
        input_sql_nodes=[column],
        node_type=NodeType.COUNT_DICT_TRANSFORM,
        parameters=parameters,
    )
    assert node.sql.sql() == expected


@pytest.mark.parametrize(
    "parameters, expected",
    [
        ({"type": "int", "from_dtype": DBVarType.BOOL}, "CAST(val AS INT)"),
        ({"type": "int", "from_dtype": DBVarType.INT}, "CAST(val AS INT)"),
        ({"type": "int", "from_dtype": DBVarType.FLOAT}, "CAST(FLOOR(val) AS INT)"),
        ({"type": "int", "from_dtype": DBVarType.VARCHAR}, "CAST(val AS INT)"),
        ({"type": "float", "from_dtype": DBVarType.BOOL}, "CAST(CAST(val AS INT) AS FLOAT)"),
        ({"type": "float", "from_dtype": DBVarType.INT}, "CAST(val AS FLOAT)"),
        ({"type": "float", "from_dtype": DBVarType.FLOAT}, "CAST(val AS FLOAT)"),
        ({"type": "float", "from_dtype": DBVarType.VARCHAR}, "CAST(val AS FLOAT)"),
        ({"type": "str", "from_dtype": DBVarType.BOOL}, "CAST(val AS VARCHAR)"),
        ({"type": "str", "from_dtype": DBVarType.INT}, "CAST(val AS VARCHAR)"),
        ({"type": "str", "from_dtype": DBVarType.FLOAT}, "CAST(val AS VARCHAR)"),
        ({"type": "str", "from_dtype": DBVarType.VARCHAR}, "CAST(val AS VARCHAR)"),
    ],
)
def test_cast(parameters, expected, input_node):
    """Test cast node for type conversion"""
    column = sql.StrExpressionNode(table_node=input_node, expr="val")
    node = sql.make_expression_node(
        input_sql_nodes=[column],
        node_type=NodeType.CAST,
        parameters=parameters,
    )
    assert node.sql.sql() == expected


def test_cosine_similarity(input_node):
    """Test cosine similarity node"""
    column1 = sql.StrExpressionNode(table_node=input_node, expr="a")
    column2 = sql.StrExpressionNode(table_node=input_node, expr="b")
    input_nodes = [column1, column2]
    parameters = {}
    node = sql.make_binary_operation_node(NodeType.COSINE_SIMILARITY, input_nodes, parameters)
    assert node.sql.sql() == "(F_COUNT_DICT_COSINE_SIMILARITY(a, b))"


def test_lag(input_node):
    """Test lag node"""
    column = sql.StrExpressionNode(table_node=input_node, expr="val")
    node = sql.make_expression_node(
        input_sql_nodes=[column],
        node_type=NodeType.LAG,
        parameters={"timestamp_column": "ts", "entity_columns": ["cust_id"], "offset": 1},
    )
    assert node.sql.sql() == 'LAG(val, 1) OVER(PARTITION BY "cust_id" ORDER BY "ts")'


def test_date_difference(input_node):
    """Test DateDiff node"""
    column1 = sql.StrExpressionNode(table_node=input_node, expr="a")
    column2 = sql.StrExpressionNode(table_node=input_node, expr="b")
    input_nodes = [column1, column2]
    node = sql.make_binary_operation_node(
        NodeType.DATE_DIFF,
        input_nodes,
        parameters={},
    )
    assert node.sql.sql() == "DATEDIFF(second, b, a)"


def test_timedelta(input_node):
    """Test TimedeltaNode"""
    column = sql.StrExpressionNode(table_node=input_node, expr="a")
    node = sql.make_expression_node(
        [column],
        NodeType.TIMEDELTA,
        parameters={"unit": "second"},
    )
    # when previewing, this should show the value component of the timedelta without unit
    assert node.sql.sql() == "a"


def test_date_add__timedelta(input_node):
    """Test DateAdd node"""
    column = sql.StrExpressionNode(table_node=input_node, expr="num_seconds")
    timedelta_node = sql.make_expression_node(
        [column],
        NodeType.TIMEDELTA,
        parameters={"unit": "second"},
    )
    date_column = sql.StrExpressionNode(table_node=input_node, expr="date_col")
    date_add_node = sql.make_binary_operation_node(
        NodeType.DATE_ADD, [date_column, timedelta_node], {}
    )
    assert date_add_node.sql.sql() == "DATEADD(second, num_seconds, date_col)"


def test_date_add__datediff(input_node):
    """Test DateAdd node when the timedelta is the result of date difference"""
    # make a date diff node
    column1 = sql.StrExpressionNode(table_node=input_node, expr="a")
    column2 = sql.StrExpressionNode(table_node=input_node, expr="b")
    input_nodes = [column1, column2]
    date_diff_node = sql.make_binary_operation_node(
        NodeType.DATE_DIFF,
        input_nodes,
        parameters={},
    )
    # make a date add node
    date_column = sql.StrExpressionNode(table_node=input_node, expr="date_col")
    date_add_node = sql.make_binary_operation_node(
        NodeType.DATE_ADD, [date_column, date_diff_node], {}
    )
    assert date_add_node.sql.sql() == "DATEADD(microsecond, DATEDIFF(microsecond, b, a), date_col)"


def test_date_add__constant(input_node):
    """Test DateAdd node when the timedelta is a fixed constant"""
    date_column = sql.StrExpressionNode(table_node=input_node, expr="date_col")
    date_add_node = sql.make_binary_operation_node(
        NodeType.DATE_ADD, [date_column], {"value": 3600}
    )
    assert date_add_node.sql.sql() == "DATEADD(second, 3600, date_col)"


@pytest.mark.parametrize(
    "input_unit, output_unit, expected",
    [
        ("second", "minute", "(date_col * 1000000 / 60000000)"),
        ("hour", "minute", "(date_col * 3600000000 / 60000000)"),
        ("minute", "minute", "(date_col * 60000000 / 60000000)"),
        ("second", "minute", "(date_col * 1000000 / 60000000)"),
        ("millisecond", "minute", "(date_col * 1000 / 60000000)"),
        ("microsecond", "minute", "(date_col * 1 / 60000000)"),
    ],
)
def test_convert_timedelta_unit(input_node, input_unit, output_unit, expected):
    """Test convert_timedelta_unit"""
    date_column = sql.StrExpressionNode(table_node=input_node, expr="date_col")
    converted = sql.TimedeltaExtractNode.convert_timedelta_unit(
        date_column.sql, input_unit, output_unit
    )
    assert converted.sql() == expected


def test_datediff_resolves_correctly(dataframe):
    """Test datediff node resolution works correctly even in weird case

    It is important that assignment operation makes a copy of the involved TableNode
    """
    dataframe["diff"] = dataframe["TIMESTAMP_VALUE"] - dataframe["TIMESTAMP_VALUE"]
    diff = dataframe["diff"]
    dataframe["diff"] = 123
    dataframe["NEW_TIMESTAMP"] = dataframe["TIMESTAMP_VALUE"] + diff
    sql = dataframe.preview_sql()
    expected_sql = textwrap.dedent(
        """
        SELECT
          "CUST_ID" AS "CUST_ID",
          "PRODUCT_ACTION" AS "PRODUCT_ACTION",
          "VALUE" AS "VALUE",
          "MASK" AS "MASK",
          "TIMESTAMP_VALUE" AS "TIMESTAMP_VALUE",
          123 AS "diff",
          DATEADD(microsecond, DATEDIFF(microsecond, "TIMESTAMP_VALUE", "TIMESTAMP_VALUE"), "TIMESTAMP_VALUE") AS "NEW_TIMESTAMP"
        FROM "db"."public"."transaction"
        LIMIT 10
        """
    ).strip()
    assert sql == expected_sql


@pytest.mark.parametrize(
    "value, expected_sql",
    [
        (123, "123"),
        (123.0, "123.0"),
        ("some_string", "'some_string'"),
        (float("nan"), "NULL"),
        (np.nan, "NULL"),
        (None, "NULL"),
    ],
)
def test_make_literal_value(value, expected_sql):
    """
    Test make_literal_value helper function
    """
    expr = sql.make_literal_value(value)
    assert expr.sql() == expected_sql
