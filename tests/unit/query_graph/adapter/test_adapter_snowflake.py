import pytest
from sqlglot import select

from featurebyte.enum import DBVarType
from featurebyte.query_graph.sql.adapter import SnowflakeAdapter


@pytest.mark.parametrize(
    "dtype, expected",
    [
        (DBVarType.FLOAT, "FLOAT"),
        (DBVarType.INT, "FLOAT"),
        (DBVarType.VARCHAR, "VARCHAR"),
        (DBVarType.OBJECT, "OBJECT"),
        (DBVarType.BINARY, "VARIANT"),
        (DBVarType.ARRAY, "ARRAY"),
    ],
)
def test_get_online_store_type_from_dtype(dtype, expected):
    """
    Test get_online_store_type_from_dtype for SnowflakeAdapter
    """
    assert SnowflakeAdapter.get_physical_type_from_dtype(dtype) == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT abc as A", "SELECT abc as A"),
        ("SELECT 'abc' as A", "SELECT ''abc'' as A"),
        ("SELECT ''abc'' as A", "SELECT ''abc'' as A"),
    ],
)
def test_escape_quote_char__snowflake(query, expected):
    """
    Test escape_quote_char for SnowflakeAdapter
    """
    assert SnowflakeAdapter.escape_quote_char(query) == expected


@pytest.mark.parametrize(
    "column_name, will_be_quoted",
    [
        ("CUSTOMER_ID", False),
        ("CUSTOMER_ID_123", False),
        ("_CUSTOMER_ID", False),
        ("_CUSTOMER$ID", False),
        ("1CUSTOMER$ID", True),
        ("$CUSTOMER_ID", True),
        ("customerID", True),
        ("123", True),
    ],
)
def test_will_pivoted_column_name_be_quoted(column_name, will_be_quoted):
    """
    Test will_pivoted_column_name_be_quoted for SnowflakeAdapter
    """
    assert SnowflakeAdapter.will_pivoted_column_name_be_quoted(column_name) is will_be_quoted


@pytest.mark.parametrize(
    "dtype, expected",
    [
        (DBVarType.FLOAT, '"VALUE"'),
        (DBVarType.OBJECT, 'TO_JSON("VALUE") AS "VALUE"'),
        (DBVarType.STRUCT, 'TO_JSON("VALUE") AS "VALUE"'),
        (DBVarType.ARRAY, 'TO_JSON("VALUE") AS "VALUE"'),
    ],
)
def test_online_store_pivot_prepare_value_column(dtype, expected):
    """
    Test online_store_pivot_prepare_value_column for SnowflakeAdapter
    """
    actual = SnowflakeAdapter.online_store_pivot_prepare_value_column(dtype).sql()
    assert actual == expected


@pytest.mark.parametrize(
    "dtype, expected",
    [
        (DBVarType.FLOAT, "\"'result'\""),
        (DBVarType.OBJECT, "PARSE_JSON(\"'result'\")"),
        (DBVarType.STRUCT, "PARSE_JSON(\"'result'\")"),
        (DBVarType.ARRAY, "PARSE_JSON(\"'result'\")"),
    ],
)
def test_online_store_pivot_finalise_value_column(dtype, expected):
    """
    Test online_store_pivot_finalise_value_column for SnowflakeAdapter
    """
    actual = SnowflakeAdapter.online_store_pivot_finalise_value_column("result", dtype).sql()
    assert actual == expected


@pytest.mark.parametrize(
    "percent, expected_format",
    [
        (10.01, "10.01"),
        (10.0, "10"),
        (10, "10"),
        (0.1, "0.1"),
        (0.000001, "0.000001"),
        (0.000000001, "0.000000001"),
    ],
)
def test_tablesample_percentage_formatting(percent, expected_format):
    """
    Test the percentage in TABLESAMPLE is not formatted using scientific notation because that is
    not supported in some engines like Spark
    """
    out = SnowflakeAdapter.tablesample(select("*").from_("A"), percent).sql()
    expected = f"SELECT * FROM (SELECT * FROM A) TABLESAMPLE({expected_format})"
    assert out == expected
