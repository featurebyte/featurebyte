"""
Test groupby helper
"""

import textwrap

import pytest
from sqlglot import parse_one, select

from featurebyte import AggFunc, SourceType
from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import get_qualified_column_identifier
from featurebyte.query_graph.sql.groupby_helper import (
    GroupbyColumn,
    GroupbyKey,
    get_aggregation_expression,
    get_groupby_expr,
    get_vector_agg_column_snowflake,
    update_aggregation_expression_for_columns,
)
from featurebyte.query_graph.sql.source_info import SourceInfo
from tests.unit.query_graph.sql.fixtures.snowflake_double_vector_agg_only import (
    SNOWFLAKE_DOUBLE_VECTOR_AGG_ONLY_QUERY,
)
from tests.unit.query_graph.sql.fixtures.snowflake_double_vector_agg_only_no_value_by import (
    SNOWFLAKE_DOUBLE_VECTOR_AGG_NO_VALUE_BY_QUERY,
)
from tests.unit.query_graph.sql.fixtures.snowflake_vector_agg_with_normal_agg import (
    SNOWFLAKE_VECTOR_AGG_WITH_NORMAL_AGG_QUERY,
)


@pytest.mark.parametrize(
    "agg_func,input_column,parent_dtype,expected",
    [
        (AggFunc.COUNT, None, None, "COUNT(*)"),
        (
            AggFunc.COUNT_DISTINCT,
            "col",
            None,
            'COUNT(DISTINCT "col") + CAST(COUNT_IF("col" IS NULL) > 0 AS BIGINT)',
        ),
        (AggFunc.SUM, "col", None, 'SUM("col")'),
        (AggFunc.AVG, "col", None, 'AVG("col")'),
        (AggFunc.MIN, "col", None, 'MIN("col")'),
        (AggFunc.MAX, "col", None, 'MAX("col")'),
        (AggFunc.STD, "col", None, 'STDDEV("col")'),
        (AggFunc.NA_COUNT, "col", None, 'SUM(CAST("col" IS NULL AS INTEGER))'),
        (AggFunc.SUM, parse_one("a + b"), None, "SUM(a + b)"),
        (AggFunc.AVG, "col", DBVarType.ARRAY, 'VECTOR_AGGREGATE_SIMPLE_AVERAGE("col")'),
    ],
)
def test_get_aggregation_expression(agg_func, input_column, parent_dtype, expected, adapter):
    """
    Test get_aggregation_expression
    """
    expr = get_aggregation_expression(agg_func, input_column, parent_dtype, adapter)
    assert expr.sql(pretty=True) == expected


@pytest.fixture(name="common_params")
def common_params_fixture():
    """
    Common parameters for tests
    """
    select_expr = select().from_("REQ")
    groupby_key = GroupbyKey(
        expr=get_qualified_column_identifier("serving_name", "REQ"),
        name="serving_name",
    )
    groupby_key_point_in_time = GroupbyKey(
        expr=get_qualified_column_identifier(SpecialColumnName.POINT_IN_TIME, "REQ"),
        name=SpecialColumnName.POINT_IN_TIME,
    )
    valueby_key = GroupbyKey(
        expr=get_qualified_column_identifier("value_by", "REQ"),
        name="value_by",
    )
    return select_expr, groupby_key, groupby_key_point_in_time, valueby_key


@pytest.mark.parametrize(
    "agg_func, expected_table_func, expect_error",
    [
        (AggFunc.SUM, "VECTOR_AGGREGATE_SUM", False),
        (AggFunc.MAX, "VECTOR_AGGREGATE_MAX", False),
        (AggFunc.AVG, "VECTOR_AGGREGATE_SIMPLE_AVERAGE", False),
        (AggFunc.MIN, "n/a", True),
    ],
)
def test_get_vector_agg_column_snowflake(
    agg_func, expected_table_func, expect_error, common_params
):
    """
    Test get_vector_agg_expr for snowflake
    """
    select_expr, groupby_key, _, _ = common_params
    groupby_column = GroupbyColumn(
        agg_func=AggFunc.SUM,
        parent_expr=(get_qualified_column_identifier("parent", "TABLE")),
        result_name="result",
        parent_dtype=DBVarType.ARRAY,
        parent_cols=[(get_qualified_column_identifier("parent", "TABLE"))],
    )

    # If error expected, check the assertion and return.
    if expect_error:
        with pytest.raises(AssertionError):
            get_vector_agg_column_snowflake(
                select_expr,
                agg_func,
                groupby_keys=[groupby_key],
                groupby_column=groupby_column,
                index=0,
                is_tile=False,
            )
        return

    # If no error expected, check the SQL.
    vector_agg_col = get_vector_agg_column_snowflake(
        select_expr,
        agg_func,
        groupby_keys=[groupby_key],
        groupby_column=groupby_column,
        index=0,
        is_tile=False,
    )
    expected = textwrap.dedent(
        f"""
            SELECT
              INITIAL_DATA."serving_name" AS "serving_name",
              AGG_0."VECTOR_AGG_RESULT" AS "result"
            FROM (
              SELECT
                REQ."serving_name" AS "serving_name",
                TABLE."parent" AS parent
              FROM REQ
            ) AS INITIAL_DATA, TABLE({expected_table_func}(parent) OVER (PARTITION BY INITIAL_DATA."serving_name")) AS "AGG_0"
        """
    ).strip()
    parsed_expression = parse_one(expected)
    assert vector_agg_col.aggr_expr.sql(pretty=True) == parsed_expression.sql(pretty=True)


def _maybe_wrap_in_variant(source_type: SourceType, expr_str: str) -> str:
    if source_type == SourceType.SPARK:
        return expr_str
    return f"TO_VARIANT({expr_str})"


@pytest.mark.parametrize(
    "column_params, methods, source_type",
    [
        ([(AggFunc.SUM, None), (AggFunc.SUM, None)], ["SUM", "SUM"], SourceType.SNOWFLAKE),
        ([(AggFunc.MAX, None), (AggFunc.MAX, None)], ["MAX", "MAX"], SourceType.SNOWFLAKE),
        (
            [(AggFunc.MAX, DBVarType.INT), (AggFunc.MAX, DBVarType.INT)],
            ["MAX", "MAX"],
            SourceType.SNOWFLAKE,
        ),
        ([(AggFunc.SUM, None), (AggFunc.SUM, None)], ["SUM", "SUM"], SourceType.SPARK),
        ([(AggFunc.MAX, None), (AggFunc.MAX, None)], ["MAX", "MAX"], SourceType.SPARK),
        (
            [(AggFunc.MAX, DBVarType.INT), (AggFunc.MAX, DBVarType.INT)],
            ["MAX", "MAX"],
            SourceType.SPARK,
        ),
        (
            [(AggFunc.MAX, DBVarType.ARRAY), (AggFunc.MAX, DBVarType.ARRAY)],
            ["VECTOR_AGGREGATE_MAX", "VECTOR_AGGREGATE_MAX"],
            SourceType.SPARK,
        ),
        (
            [(AggFunc.MAX, DBVarType.ARRAY), (AggFunc.MAX, DBVarType.INT)],
            ["VECTOR_AGGREGATE_MAX", "MAX"],
            SourceType.SPARK,
        ),
    ],
)
def test_get_groupby_expr__multiple_groupby_columns__non_snowflake_vector_aggrs(
    column_params, methods, source_type, common_params
):
    """
    Test get_groupby_expr with multiple groupby columns
    """
    select_expr, groupby_key, groupby_key_point_in_time, valueby_key = common_params

    groupby_columns = []
    i = 0
    for param in column_params:
        groupby_column = GroupbyColumn(
            agg_func=param[0],
            parent_expr=(get_qualified_column_identifier("parent", "TABLE")),
            result_name=f"result_{i}",
            parent_dtype=param[1],
            parent_cols=[(get_qualified_column_identifier("parent", "TABLE"))],
        )
        i += 1
        groupby_columns.append(groupby_column)
    groupby_expr = get_groupby_expr(
        input_expr=select_expr,
        groupby_keys=[groupby_key, groupby_key_point_in_time],
        groupby_columns=groupby_columns,
        value_by=valueby_key,
        adapter=get_sql_adapter(
            SourceInfo(source_type=source_type, database_name="", schema_name="")
        ),
    )

    result_0 = _maybe_wrap_in_variant(source_type, 'INNER_."result_0_inner"')
    result_1 = _maybe_wrap_in_variant(source_type, 'INNER_."result_1_inner"')
    if source_type == "snowflake":
        expected = textwrap.dedent(
            f"""
            SELECT
              INNER_."serving_name",
              INNER_."POINT_IN_TIME",
              OBJECT_AGG(
                CASE
                  WHEN INNER_."value_by" IS NULL
                  THEN '__MISSING__'
                  ELSE CAST(INNER_."value_by" AS TEXT)
                END,
                {result_0}
              ) AS "result_0",
              OBJECT_AGG(
                CASE
                  WHEN INNER_."value_by" IS NULL
                  THEN '__MISSING__'
                  ELSE CAST(INNER_."value_by" AS TEXT)
                END,
                {result_1}
              ) AS "result_1"
            FROM (
              SELECT
                "serving_name",
                "POINT_IN_TIME",
                "value_by",
                "result_0_inner",
                "result_1_inner"
              FROM (
                SELECT
                  "serving_name",
                  "POINT_IN_TIME",
                  "value_by",
                  "result_0_inner",
                  "result_1_inner",
                  ROW_NUMBER() OVER (PARTITION BY "serving_name", "POINT_IN_TIME" ORDER BY "result_0_inner" DESC) AS "__fb_object_agg_row_number"
                FROM (
                  SELECT
                    REQ."serving_name" AS "serving_name",
                    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
                    REQ."value_by" AS "value_by",
                    {methods[0]}(TABLE."parent") AS "result_0_inner",
                    {methods[1]}(TABLE."parent") AS "result_1_inner"
                  FROM REQ
                  GROUP BY
                    REQ."serving_name",
                    REQ."POINT_IN_TIME",
                    REQ."value_by"
                )
              )
              WHERE
                "__fb_object_agg_row_number" <= 500
            ) AS INNER_
            GROUP BY
              INNER_."serving_name",
              INNER_."POINT_IN_TIME"
            """
        ).strip()
    else:
        expected = textwrap.dedent(
            f"""
            SELECT
              INNER_."serving_name",
              INNER_."POINT_IN_TIME",
              MAP_FILTER(
                MAP_FROM_ENTRIES(
                  COLLECT_LIST(
                    STRUCT(
                      CASE
                        WHEN INNER_."value_by" IS NULL
                        THEN '__MISSING__'
                        ELSE CAST(INNER_."value_by" AS TEXT)
                      END,
                      {result_0}
                    )
                  )
                ),
                (k, v) -> NOT v IS NULL
              ) AS "result_0",
              MAP_FILTER(
                MAP_FROM_ENTRIES(
                  COLLECT_LIST(
                    STRUCT(
                      CASE
                        WHEN INNER_."value_by" IS NULL
                        THEN '__MISSING__'
                        ELSE CAST(INNER_."value_by" AS TEXT)
                      END,
                      {result_1}
                    )
                  )
                ),
                (k, v) -> NOT v IS NULL
              ) AS "result_1"
            FROM (
              SELECT
                "serving_name",
                "POINT_IN_TIME",
                "value_by",
                "result_0_inner",
                "result_1_inner"
              FROM (
                SELECT
                  "serving_name",
                  "POINT_IN_TIME",
                  "value_by",
                  "result_0_inner",
                  "result_1_inner",
                  ROW_NUMBER() OVER (PARTITION BY "serving_name", "POINT_IN_TIME" ORDER BY "result_0_inner" DESC) AS "__fb_object_agg_row_number"
                FROM (
                  SELECT
                    REQ."serving_name" AS "serving_name",
                    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
                    REQ."value_by" AS "value_by",
                    {methods[0]}(TABLE."parent") AS "result_0_inner",
                    {methods[1]}(TABLE."parent") AS "result_1_inner"
                  FROM REQ
                  GROUP BY
                    REQ."serving_name",
                    REQ."POINT_IN_TIME",
                    REQ."value_by"
                )
              )
              WHERE
                "__fb_object_agg_row_number" <= 500
            ) AS INNER_
            GROUP BY
              INNER_."serving_name",
              INNER_."POINT_IN_TIME"
            """
        ).strip()
    assert groupby_expr.sql(pretty=True) == expected


@pytest.mark.parametrize(
    "column_params, methods, source_type, use_value_by, result",
    [
        (
            [(AggFunc.MAX, DBVarType.ARRAY), (AggFunc.MAX, DBVarType.ARRAY)],
            ["VECTOR_AGGREGATE_MAX", "VECTOR_AGGREGATE_MAX"],
            SourceType.SNOWFLAKE,
            True,
            SNOWFLAKE_DOUBLE_VECTOR_AGG_ONLY_QUERY,
        ),
        (
            [(AggFunc.MAX, DBVarType.ARRAY), (AggFunc.MAX, DBVarType.INT)],
            ["VECTOR_AGGREGATE_MAX", "MAX"],
            SourceType.SNOWFLAKE,
            True,
            SNOWFLAKE_VECTOR_AGG_WITH_NORMAL_AGG_QUERY,
        ),
        (
            [(AggFunc.MAX, DBVarType.ARRAY), (AggFunc.MAX, DBVarType.ARRAY)],
            ["VECTOR_AGGREGATE_MAX", "VECTOR_AGGREGATE_MAX"],
            SourceType.SNOWFLAKE,
            False,
            SNOWFLAKE_DOUBLE_VECTOR_AGG_NO_VALUE_BY_QUERY,
        ),
    ],
)
def test_get_groupby_expr__multiple_groupby_columns__snowflake_vector_aggrs(
    column_params, methods, source_type, use_value_by, result, common_params
):
    """
    Test get_groupby_expr with multiple groupby columns
    """
    select_expr, groupby_key, groupby_key_point_in_time, value_by = common_params
    adapter = get_sql_adapter(SourceInfo(source_type=source_type, database_name="", schema_name=""))

    groupby_columns = []
    i = 0
    for param in column_params:
        groupby_column = GroupbyColumn(
            agg_func=param[0],
            parent_expr=(get_qualified_column_identifier("parent", "TABLE")),
            result_name=f"result_{i}",
            parent_dtype=param[1],
            parent_cols=[(get_qualified_column_identifier("parent", "TABLE"))],
        )
        i += 1
        groupby_columns.append(groupby_column)
    groupby_keys = [groupby_key, groupby_key_point_in_time]
    groupby_columns = update_aggregation_expression_for_columns(
        groupby_columns, groupby_keys, None, adapter
    )
    if not use_value_by:
        value_by = None
    groupby_expr = get_groupby_expr(
        input_expr=select_expr,
        groupby_keys=groupby_keys,
        groupby_columns=groupby_columns,
        value_by=value_by,
        adapter=adapter,
    )
    assert groupby_expr.sql(pretty=True) == result.strip()


@pytest.mark.parametrize(
    "agg_func, parent_dtype, method",
    [
        (AggFunc.SUM, None, "SUM"),
        (AggFunc.MAX, None, "MAX"),
        (AggFunc.MAX, DBVarType.INT, "MAX"),
        (AggFunc.MAX, DBVarType.ARRAY, "VECTOR_AGGREGATE_MAX"),
    ],
)
def test_get_groupby_expr(agg_func, parent_dtype, method, common_params, spark_source_info):
    """
    Test get_groupby_expr
    """
    select_expr, groupby_key, groupby_key_point_in_time, valueby_key = common_params
    groupby_column = GroupbyColumn(
        agg_func=agg_func,
        parent_expr=(get_qualified_column_identifier("parent", "TABLE")),
        result_name="result",
        parent_dtype=parent_dtype,
        parent_cols=[(get_qualified_column_identifier("parent", "TABLE"))],
    )
    groupby_expr = get_groupby_expr(
        input_expr=select_expr,
        groupby_keys=[groupby_key, groupby_key_point_in_time],
        groupby_columns=[groupby_column],
        value_by=valueby_key,
        adapter=get_sql_adapter(spark_source_info),
    )
    expected = textwrap.dedent(
        f"""
        SELECT
          INNER_."serving_name",
          INNER_."POINT_IN_TIME",
          MAP_FILTER(
            MAP_FROM_ENTRIES(
              COLLECT_LIST(
                STRUCT(
                  CASE
                    WHEN INNER_."value_by" IS NULL
                    THEN '__MISSING__'
                    ELSE CAST(INNER_."value_by" AS TEXT)
                  END,
                  INNER_."result_inner"
                )
              )
            ),
            (k, v) -> NOT v IS NULL
          ) AS "result"
        FROM (
          SELECT
            "serving_name",
            "POINT_IN_TIME",
            "value_by",
            "result_inner"
          FROM (
            SELECT
              "serving_name",
              "POINT_IN_TIME",
              "value_by",
              "result_inner",
              ROW_NUMBER() OVER (PARTITION BY "serving_name", "POINT_IN_TIME" ORDER BY "result_inner" DESC) AS "__fb_object_agg_row_number"
            FROM (
              SELECT
                REQ."serving_name" AS "serving_name",
                REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
                REQ."value_by" AS "value_by",
                {method}(TABLE."parent") AS "result_inner"
              FROM REQ
              GROUP BY
                REQ."serving_name",
                REQ."POINT_IN_TIME",
                REQ."value_by"
            )
          )
          WHERE
            "__fb_object_agg_row_number" <= 500
        ) AS INNER_
        GROUP BY
          INNER_."serving_name",
          INNER_."POINT_IN_TIME"
        """
    ).strip()
    assert groupby_expr.sql(pretty=True) == expected
