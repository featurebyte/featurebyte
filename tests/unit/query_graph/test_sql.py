"""
Tests for the featurebyte.query_graph.sql module
"""
import textwrap
from unittest.mock import Mock

import numpy as np
import pytest
from sqlglot import parse_one

from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.binary import BinaryOp
from featurebyte.query_graph.sql.ast.count_dict import (
    CountDictTransformNode,
    DictionaryKeysNode,
    GetRankNode,
    GetRelativeFrequencyNode,
    GetValueFromDictionaryNode,
)
from featurebyte.query_graph.sql.ast.datetime import (
    DateAddNode,
    DateDiffNode,
    TimedeltaExtractNode,
    TimedeltaNode,
)
from featurebyte.query_graph.sql.ast.generic import (
    ParsedExpressionNode,
    Project,
    StrExpressionNode,
    make_assign_node,
    resolve_project_node,
)
from featurebyte.query_graph.sql.ast.input import InputNode
from featurebyte.query_graph.sql.ast.is_in import IsInNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.ast.string import IsStringNode
from featurebyte.query_graph.sql.ast.unary import CastNode, LagNode
from featurebyte.query_graph.sql.builder import SQLNodeContext
from featurebyte.query_graph.sql.common import SQLType


def make_context(node_type=None, parameters=None, input_sql_nodes=None, sql_type=None):
    """
    Helper function to create a SQLNodeContext with only arguments that matter in tests
    """
    if parameters is None:
        parameters = {}
    if sql_type is None:
        sql_type = SQLType.EVENT_VIEW_PREVIEW
    mock_query_node = Mock(type=node_type)
    mock_query_node.parameters.dict.return_value = parameters
    mock_graph = Mock()
    context = SQLNodeContext(
        graph=mock_graph,
        query_node=mock_query_node,
        input_sql_nodes=input_sql_nodes,
        sql_type=sql_type,
        source_type=SourceType.SNOWFLAKE,
    )
    return context


def make_str_expression_node(table_node, expr):
    """
    Helper function to create a StrExpressionNode used only in tests
    """
    return StrExpressionNode(make_context(), table_node=table_node, expr=expr)


@pytest.fixture(name="input_node")
def input_node_fixture():
    """Fixture for a generic InputNode"""
    columns_map = {
        "col_1": parse_one("col_1"),
        "col_2": parse_one("col_2"),
        "col_3": parse_one("col_3"),
    }
    return InputNode(
        context=make_context(node_type=NodeType.INPUT),
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
    expr_node_1 = ParsedExpressionNode(make_context(), input_node, parse_one("a + 1"))
    expr_node_2 = ParsedExpressionNode(make_context(), input_node, parse_one("a + 2"))
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
    expr_node = ParsedExpressionNode(make_context(), input_node, parse_one("a + 1"))
    input_node.assign_column("new_col", expr_node)
    project_node = Project(make_context(), input_node, "new_col")
    project_node_original_column = Project(make_context(), input_node, "col_1")
    assert resolve_project_node(project_node) == expr_node
    assert resolve_project_node(project_node_original_column) is None
    # no-op if not a Project node
    assert resolve_project_node(expr_node) == expr_node


def test_make_assign_node(input_node):
    """Test make_assign_node"""
    expr_node = ParsedExpressionNode(make_context(), input_node, parse_one("a + 1"))
    result = make_assign_node(
        make_context(input_sql_nodes=[input_node, expr_node], parameters={"name": "new_col"})
    )
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
    column1 = make_str_expression_node(table_node=input_node, expr="a")
    column2 = make_str_expression_node(table_node=input_node, expr="b")
    input_nodes = [column1, column2]
    context = make_context(node_type=node_type, parameters={}, input_sql_nodes=input_nodes)
    node = BinaryOp.build(context)
    assert node.sql.sql() == expected


def test_binary_operation_node__consecutive_ops_1(input_node):
    """Test multiple binary operations"""
    col_a = make_str_expression_node(table_node=input_node, expr="a")
    col_b = make_str_expression_node(table_node=input_node, expr="b")
    col_c = make_str_expression_node(table_node=input_node, expr="c")
    a_plus_b = BinaryOp.build(
        make_context(node_type=NodeType.ADD, parameters={}, input_sql_nodes=[col_a, col_b])
    )
    a_plus_b_div_c = BinaryOp.build(
        make_context(node_type=NodeType.DIV, parameters={}, input_sql_nodes=[a_plus_b, col_c])
    )
    assert a_plus_b_div_c.sql.sql() == "((a + b) / NULLIF(c, 0))"


def test_binary_operation_node__consecutive_ops_2(input_node):
    """Test multiple binary operations"""
    col_a = make_str_expression_node(table_node=input_node, expr="a")
    col_b = make_str_expression_node(table_node=input_node, expr="b")
    col_c = make_str_expression_node(table_node=input_node, expr="c")
    a_plus_b = BinaryOp.build(
        make_context(node_type=NodeType.ADD, parameters={}, input_sql_nodes=[col_a, col_b])
    )
    c_div_a_plus_b = BinaryOp.build(
        make_context(node_type=NodeType.DIV, parameters={}, input_sql_nodes=[col_c, a_plus_b])
    )
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
    column1 = StrExpressionNode(make_context(), table_node=input_node, expr="a")
    input_nodes = [column1]
    parameters = {"value": value, "right_op": right_op}
    node = BinaryOp.build(
        make_context(node_type=node_type, input_sql_nodes=input_nodes, parameters=parameters)
    )
    assert node.sql.sql() == expected


def test_make_input_node_escape_special_characters():
    """Test input node quotes all identifiers to handle special characters"""
    parameters = {
        "type": "event_data",
        "columns": [
            {"name": "SUM(a)", "dtype": "FLOAT"},
            {"name": "b", "dtype": "FLOAT"},
            {"name": "c", "dtype": "FLOAT"},
        ],
        "table_details": {"database_name": "db", "schema_name": "public", "table_name": "my_table"},
        "feature_store_details": {
            "type": "snowflake",
            "details": {
                "database": "db",
                "sf_schema": "public",
            },
        },
    }
    node = InputNode.build(
        make_context(
            node_type=NodeType.INPUT,
            parameters=parameters,
            sql_type=SQLType.EVENT_VIEW_PREVIEW,
        )
    )
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
    column = make_str_expression_node(table_node=input_node, expr="cd_val")
    node = CountDictTransformNode.build(
        make_context(
            node_type=NodeType.COUNT_DICT_TRANSFORM,
            parameters=parameters,
            input_sql_nodes=[column],
        )
    )
    assert node.sql.sql() == expected


def test_dictionary_keys_node(input_node):
    """
    Test dictionary keys node
    """
    column = make_str_expression_node(table_node=input_node, expr="cd_val")
    node = DictionaryKeysNode.build(
        make_context(
            node_type=NodeType.DICTIONARY_KEYS,
            input_sql_nodes=[column],
        )
    )
    assert node.sql.sql() == "OBJECT_KEYS(cd_val)"


def test_get_value_node(input_node):
    """
    Test get value node
    """
    dictionary_node = make_str_expression_node(table_node=input_node, expr="dictionary")
    lookup_node = make_str_expression_node(table_node=input_node, expr="lookup")
    node = GetValueFromDictionaryNode.build(
        make_context(
            node_type=NodeType.GET_VALUE,
            input_sql_nodes=[dictionary_node, lookup_node],
        )
    )
    assert node.sql.sql() == "GET(dictionary, lookup)"


@pytest.mark.parametrize(
    "parameters, expected",
    [
        ({"descending": True}, "F_GET_RANK(dictionary, lookup, TRUE)"),
        ({"descending": False}, "F_GET_RANK(dictionary, lookup, FALSE)"),
    ],
)
def test_get_rank_node(parameters, expected, input_node):
    """
    Test get rank node
    """
    dictionary_node = make_str_expression_node(table_node=input_node, expr="dictionary")
    lookup_node = make_str_expression_node(table_node=input_node, expr="lookup")
    node = GetRankNode.build(
        make_context(
            node_type=NodeType.GET_RANK,
            parameters=parameters,
            input_sql_nodes=[dictionary_node, lookup_node],
        )
    )
    assert node.sql.sql() == expected


def test_get_relative_frequency_node(input_node):
    """
    Test get relative frequency node
    """
    dictionary_node = make_str_expression_node(table_node=input_node, expr="dictionary")
    lookup_node = make_str_expression_node(table_node=input_node, expr="lookup")
    node = GetRelativeFrequencyNode.build(
        make_context(
            node_type=NodeType.GET_RELATIVE_FREQUENCY,
            input_sql_nodes=[dictionary_node, lookup_node],
        )
    )
    assert node.sql.sql() == "F_GET_RELATIVE_FREQUENCY(dictionary, lookup)"


def test_is_in_node(input_node):
    """
    Test is in node
    """
    input_series_expr_str = "cd_val"
    input_expr_node = make_str_expression_node(table_node=input_node, expr=input_series_expr_str)
    array_expr_str = "ARRAY(1, 2, 3)"
    array_expr_node = make_str_expression_node(table_node=input_node, expr=array_expr_str)
    node = IsInNode.build(
        make_context(
            node_type=NodeType.IS_IN,
            parameters={},
            input_sql_nodes=[input_expr_node, array_expr_node],
        )
    )
    assert (
        node.sql.sql()
        == f"CASE WHEN CAST(ARRAY_CONTAINS(TO_VARIANT({input_series_expr_str}), {array_expr_str}) IS NULL AS BOOLEAN) "
        f"THEN FALSE ELSE ARRAY_CONTAINS(TO_VARIANT({input_series_expr_str}), {array_expr_str}) END"
    )


@pytest.mark.parametrize(
    "parameters, expected",
    [
        ({"type": "int", "from_dtype": DBVarType.BOOL}, "CAST(val AS BIGINT)"),
        ({"type": "int", "from_dtype": DBVarType.INT}, "CAST(val AS BIGINT)"),
        ({"type": "int", "from_dtype": DBVarType.FLOAT}, "CAST(FLOOR(val) AS BIGINT)"),
        ({"type": "int", "from_dtype": DBVarType.VARCHAR}, "CAST(val AS BIGINT)"),
        ({"type": "float", "from_dtype": DBVarType.BOOL}, "CAST(CAST(val AS BIGINT) AS FLOAT)"),
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
    column = make_str_expression_node(table_node=input_node, expr="val")
    node = CastNode.build(
        make_context(
            node_type=NodeType.CAST,
            input_sql_nodes=[column],
            parameters=parameters,
        )
    )
    assert node.sql.sql() == expected


def test_is_string_node(input_node):
    """Test IS_STRING node SQL generation"""
    input_expr = make_str_expression_node(table_node=input_node, expr="a")
    context = make_context(
        node_type=NodeType.IS_STRING, parameters={}, input_sql_nodes=[input_expr]
    )
    node = IsStringNode.build(context)
    assert node.sql.sql() == "IS_VARCHAR(TO_VARIANT(a))"


def test_cosine_similarity(input_node):
    """Test cosine similarity node"""
    column1 = make_str_expression_node(table_node=input_node, expr="a")
    column2 = make_str_expression_node(table_node=input_node, expr="b")
    input_nodes = [column1, column2]
    parameters = {}
    node = BinaryOp.build(
        make_context(
            node_type=NodeType.COSINE_SIMILARITY,
            input_sql_nodes=input_nodes,
            parameters=parameters,
        )
    )
    assert node.sql.sql() == "(F_COUNT_DICT_COSINE_SIMILARITY(a, b))"


def test_lag(input_node):
    """Test lag node"""
    column = make_str_expression_node(table_node=input_node, expr="val")
    node = LagNode.build(
        make_context(
            input_sql_nodes=[column],
            node_type=NodeType.LAG,
            parameters={"timestamp_column": "ts", "entity_columns": ["cust_id"], "offset": 1},
        )
    )
    assert node.sql.sql() == 'LAG(val, 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST)'


def test_date_difference(input_node):
    """Test DateDiff node"""
    column1 = make_str_expression_node(table_node=input_node, expr="a")
    column2 = make_str_expression_node(table_node=input_node, expr="b")
    input_nodes = [column1, column2]
    context = make_context(parameters={}, input_sql_nodes=input_nodes)
    node = DateDiffNode.build(context)
    assert node.sql.sql() == (
        "(DATEDIFF(microsecond, b, a) * CAST(1 AS BIGINT) / CAST(1000000 AS BIGINT))"
    )


def test_timedelta(input_node):
    """Test TimedeltaNode"""
    column = make_str_expression_node(table_node=input_node, expr="a")
    context = make_context(parameters={"unit": "second"}, input_sql_nodes=[column])
    node = TimedeltaNode.build(context)
    # when previewing, this should show the value component of the timedelta without unit
    assert node.sql.sql() == "a"


def test_date_add__timedelta(input_node):
    """Test DateAdd node"""
    column = make_str_expression_node(table_node=input_node, expr="num_seconds")
    context = make_context(parameters={"unit": "second"}, input_sql_nodes=[column])
    timedelta_node = TimedeltaNode.build(context)
    date_column = make_str_expression_node(table_node=input_node, expr="date_col")
    context = make_context(parameters={}, input_sql_nodes=[date_column, timedelta_node])
    date_add_node = DateAddNode.build(context)
    assert date_add_node.sql.sql() == (
        "DATEADD(microsecond, (num_seconds * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)), date_col)"
    )


def test_date_add__datediff(input_node):
    """Test DateAdd node when the timedelta is the result of date difference"""
    # make a date diff node
    column1 = make_str_expression_node(table_node=input_node, expr="a")
    column2 = make_str_expression_node(table_node=input_node, expr="b")
    input_nodes = [column1, column2]
    context = make_context(node_type=None, parameters={}, input_sql_nodes=input_nodes)
    date_diff_node = DateDiffNode.build(context)
    # make a date add node
    date_column = make_str_expression_node(table_node=input_node, expr="date_col")
    context = make_context(input_sql_nodes=[date_column, date_diff_node])
    date_add_node = DateAddNode.build(context)
    assert date_add_node.sql.sql() == "DATEADD(microsecond, DATEDIFF(microsecond, b, a), date_col)"


def test_date_add__constant(input_node):
    """Test DateAdd node when the timedelta is a fixed constant"""
    date_column = make_str_expression_node(table_node=input_node, expr="date_col")
    context = make_context(
        node_type=None, parameters={"value": 3600}, input_sql_nodes=[date_column]
    )
    date_add_node = DateAddNode.build(context)
    assert date_add_node.sql.sql() == (
        "DATEADD(microsecond, (3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)), date_col)"
    )


@pytest.mark.parametrize(
    "input_unit, output_unit, expected",
    [
        ("second", "minute", "(date_col * CAST(1000000 AS BIGINT) / CAST(60000000 AS BIGINT))"),
        ("hour", "minute", "(date_col * CAST(3600000000 AS BIGINT) / CAST(60000000 AS BIGINT))"),
        ("minute", "minute", "(date_col * CAST(60000000 AS BIGINT) / CAST(60000000 AS BIGINT))"),
        ("second", "minute", "(date_col * CAST(1000000 AS BIGINT) / CAST(60000000 AS BIGINT))"),
        ("millisecond", "minute", "(date_col * CAST(1000 AS BIGINT) / CAST(60000000 AS BIGINT))"),
        ("microsecond", "minute", "(date_col * CAST(1 AS BIGINT) / CAST(60000000 AS BIGINT))"),
    ],
)
def test_convert_timedelta_unit(input_node, input_unit, output_unit, expected):
    """Test convert_timedelta_unit"""
    date_column = make_str_expression_node(table_node=input_node, expr="date_col")
    converted = TimedeltaExtractNode.convert_timedelta_unit(
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
          DATEADD(
            microsecond,
            DATEDIFF(microsecond, "TIMESTAMP_VALUE", "TIMESTAMP_VALUE"),
            "TIMESTAMP_VALUE"
          ) AS "NEW_TIMESTAMP"
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
        (True, "TRUE"),
        (False, "FALSE"),
    ],
)
def test_make_literal_value(value, expected_sql):
    """
    Test make_literal_value helper function
    """
    expr = make_literal_value(value)
    assert expr.sql() == expected_sql
