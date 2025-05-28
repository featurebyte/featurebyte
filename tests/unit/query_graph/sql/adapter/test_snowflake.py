"""
Test snowflake adapter module
"""

import textwrap

import pytest
from sqlglot.expressions import select

from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.sql.adapter import SnowflakeAdapter
from tests.unit.query_graph.sql.adapter.base_adapter_test import BaseAdapterTest
from tests.util.helper import get_sql_adapter_from_source_type


class TestSnowflakeAdapter(BaseAdapterTest):
    """
    Test snowflake adapter class
    """

    adapter = get_sql_adapter_from_source_type(SourceType.SNOWFLAKE)
    expected_physical_type_from_dtype_mapping = {
        "BOOL": "BOOLEAN",
        "CHAR": "VARIANT",
        "DATE": "VARIANT",
        "FLOAT": "FLOAT",
        "INT": "FLOAT",
        "TIME": "VARIANT",
        "TIMESTAMP": "TIMESTAMP_NTZ",
        "TIMESTAMP_TZ": "TIMESTAMP_TZ",
        "VARCHAR": "VARCHAR",
        "ARRAY": "ARRAY",
        "DICT": "OBJECT",
        "TIMEDELTA": "VARIANT",
        "EMBEDDING": "ARRAY",
        "FLAT_DICT": "OBJECT",
        "OBJECT": "OBJECT",
        "TIMESTAMP_TZ_TUPLE": "VARCHAR",
        "UNKNOWN": "VARIANT",
        "BINARY": "VARIANT",
        "VOID": "VARIANT",
        "MAP": "OBJECT",
        "STRUCT": "OBJECT",
    }

    @classmethod
    def get_group_by_expected_result(cls) -> str:
        """
        Returns expected result of group by query
        """
        return textwrap.dedent(
            """
                SELECT
                  VECTOR_T0."serving_name" AS "serving_name",
                  VECTOR_T0."serving_name_2" AS "serving_name_2",
                  VECTOR_T0."entity_column" AS entity_column,
                  VECTOR_T0."entity_column_2" AS "entity_column_2",
                  GROUP_BY_RESULT."sum_result" AS "sum_result",
                  GROUP_BY_RESULT."avg_result" AS "avg_result",
                  VECTOR_T0."result" AS "result",
                  VECTOR_T1."result2" AS "result2",
                  VECTOR_T2."result3" AS "result3"
                FROM (
                  SELECT
                    INITIAL_DATA."serving_name" AS "serving_name",
                    INITIAL_DATA."serving_name_2" AS "serving_name_2",
                    AGG_0."VECTOR_AGG_RESULT" AS "result"
                  FROM (
                    SELECT
                      REQ."serving_name" AS "serving_name",
                      REQ."serving_name_2" AS "serving_name_2",
                      TABLE."parent" AS parent
                  ) AS INITIAL_DATA, TABLE(
                    VECTOR_AGGREGATE_SUM(parent) OVER (PARTITION BY INITIAL_DATA."serving_name", INITIAL_DATA."serving_name_2")
                  ) AS "AGG_0"
                ) AS VECTOR_T0
                INNER JOIN (
                  SELECT
                    INITIAL_DATA."serving_name" AS "serving_name",
                    INITIAL_DATA."serving_name_2" AS "serving_name_2",
                    AGG_1."VECTOR_AGG_RESULT" AS "result2"
                  FROM (
                    SELECT
                      REQ."serving_name" AS "serving_name",
                      REQ."serving_name_2" AS "serving_name_2",
                      TABLE."parent2" AS parent2
                  ) AS INITIAL_DATA, TABLE(
                    VECTOR_AGGREGATE_SUM(parent2) OVER (PARTITION BY INITIAL_DATA."serving_name", INITIAL_DATA."serving_name_2")
                  ) AS "AGG_1"
                ) AS VECTOR_T1
                  ON VECTOR_T0."serving_name" = VECTOR_T1."serving_name"
                  AND VECTOR_T0."serving_name_2" = VECTOR_T1."serving_name_2"
                  AND VECTOR_T0."entity_column" = VECTOR_T1."entity_column"
                  AND VECTOR_T0."entity_column_2" = VECTOR_T1."entity_column_2"
                INNER JOIN (
                  SELECT
                    INITIAL_DATA."serving_name" AS "serving_name",
                    INITIAL_DATA."serving_name_2" AS "serving_name_2",
                    AGG_2."VECTOR_AGG_RESULT" AS "result3"
                  FROM (
                    SELECT
                      REQ."serving_name" AS "serving_name",
                      REQ."serving_name_2" AS "serving_name_2",
                      TABLE."parent3" AS parent3
                  ) AS INITIAL_DATA, TABLE(
                    VECTOR_AGGREGATE_SUM(parent3) OVER (PARTITION BY INITIAL_DATA."serving_name", INITIAL_DATA."serving_name_2")
                  ) AS "AGG_2"
                ) AS VECTOR_T2
                  ON VECTOR_T1."serving_name" = VECTOR_T2."serving_name"
                  AND VECTOR_T1."serving_name_2" = VECTOR_T2."serving_name_2"
                  AND VECTOR_T1."entity_column" = VECTOR_T2."entity_column"
                  AND VECTOR_T1."entity_column_2" = VECTOR_T2."entity_column_2"
                INNER JOIN (
                  SELECT
                    a,
                    b,
                    REQ."serving_name" AS "serving_name",
                    REQ."serving_name_2" AS "serving_name_2",
                    entity_column,
                    "entity_column_2",
                    SUM("parent") AS "sum_result",
                    AVG("parent_avg") AS "avg_result"
                  GROUP BY
                    REQ."serving_name",
                    REQ."serving_name_2"
                ) AS GROUP_BY_RESULT
                  ON GROUP_BY_RESULT."serving_name" = VECTOR_T2."serving_name"
                  AND GROUP_BY_RESULT."serving_name_2" = VECTOR_T2."serving_name_2"
                  AND GROUP_BY_RESULT.entity_column = VECTOR_T2."entity_column"
                  AND GROUP_BY_RESULT."entity_column_2" = VECTOR_T2."entity_column_2"
            """
        ).strip()

    @classmethod
    def get_expected_haversine_sql(cls) -> str:
        return 'HAVERSINE(TABLE."lat1", TABLE."lon1", TABLE."lat2", TABLE."lon2")'


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
    out = SnowflakeAdapter.tablesample(select("*").from_("A"), percent).sql(dialect="snowflake")
    expected = f"SELECT * FROM (SELECT * FROM A) TABLESAMPLE ({expected_format})"
    assert out == expected
