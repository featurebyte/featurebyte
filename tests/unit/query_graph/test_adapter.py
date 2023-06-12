from typing import cast

import pytest
from sqlglot import parse_one
from sqlglot.expressions import Select

from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.node.schema import TableDetails
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
    "source_type, expected",
    [
        (
            SourceType.SNOWFLAKE,
            'CREATE TABLE "db1"."schema1"."table1" AS SELECT * FROM A',
        ),
        (
            SourceType.SPARK,
            "CREATE TABLE `db1`.`schema1`.`table1` USING DELTA TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5') AS SELECT * FROM A",
        ),
        (
            SourceType.DATABRICKS,
            "CREATE TABLE `db1`.`schema1`.`table1` USING DELTA TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5') AS SELECT * FROM A",
        ),
    ],
)
def test_create_table_as(source_type, expected):
    """
    Test create_table_as for Adapter
    """

    table_details = TableDetails(
        database_name="db1",
        schema_name="schema1",
        table_name="table1",
    )
    expr = parse_one("SELECT * FROM A")
    new_expr = get_sql_adapter(source_type).create_table_as(table_details, cast(Select, expr))
    assert new_expr.sql(dialect=source_type).strip() == expected


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
