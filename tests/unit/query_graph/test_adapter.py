import pytest
from sqlglot import parse_one

from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.sql.adapter import SnowflakeAdapter, SparkAdapter, get_sql_adapter


@pytest.mark.parametrize(
    "dtype, expected",
    [
        (DBVarType.FLOAT, "FLOAT"),
        (DBVarType.INT, "FLOAT"),
        (DBVarType.VARCHAR, "VARCHAR"),
        (DBVarType.OBJECT, "OBJECT"),
        (DBVarType.BINARY, "VARIANT"),
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
    "query, expected",
    [
        ("SELECT abc as A", "SELECT abc as A"),
        ("SELECT 'abc' as A", "SELECT \\'abc\\' as A"),
        ("SELECT \\'abc\\' as A", "SELECT \\'abc\\' as A"),
    ],
)
def test_escape_quote_char__spark(query, expected):
    """
    Test escape_quote_char for SparkAdapter
    """
    assert SparkAdapter.escape_quote_char(query) == expected


@pytest.mark.parametrize(
    "source_type, query, expected",
    [
        (
            SourceType.SNOWFLAKE,
            "CREATE TABLE abc AS SELECT * FROM A",
            "CREATE TABLE abc AS SELECT * FROM A",
        ),
        (
            SourceType.SPARK,
            "CREATE TABLE abc AS SELECT * FROM A",
            "CREATE TABLE abc USING DELTA TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5') AS SELECT * FROM A",
        ),
        (
            SourceType.DATABRICKS,
            "CREATE TABLE abc AS SELECT * FROM A",
            "CREATE TABLE abc USING DELTA TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5') AS SELECT * FROM A",
        ),
    ],
)
def test_create_table_expression(source_type, query, expected):
    """
    Test escape_quote_char for SparkAdapter
    """
    expr = parse_one(query)
    new_expr = get_sql_adapter(source_type).create_table_expression(expr)
    assert new_expr.sql(dialect=source_type).strip() == expected
