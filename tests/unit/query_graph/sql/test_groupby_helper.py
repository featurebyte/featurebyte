"""
Test groupby helper
"""
import textwrap

import pytest
from sqlglot import select

from featurebyte import AggFunc, SourceType
from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import get_qualified_column_identifier
from featurebyte.query_graph.sql.groupby_helper import (
    GroupbyColumn,
    GroupbyKey,
    get_groupby_expr,
    get_vector_agg_column_snowflake,
)


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
        (AggFunc.AVG, "VECTOR_AGGREGATE_AVG", False),
        (AggFunc.MIN, "n/a", True),
    ],
)
def test_get_vector_agg_column_snowflake(
    agg_func, expected_table_func, expect_error, common_params
):
    """
    Test get_vector_agg_expr for snowflake
    """
    _, groupby_key, _, _ = common_params
    groupby_column = GroupbyColumn(
        agg_func=AggFunc.SUM,
        parent_expr=(get_qualified_column_identifier("parent", "TABLE")),
        result_name="result",
        parent_dtype=DBVarType.ARRAY,
    )

    # If error expected, check the assertion and return.
    if expect_error:
        with pytest.raises(AssertionError):
            get_vector_agg_column_snowflake(
                agg_func, groupby_keys=[groupby_key], groupby_column=groupby_column, index=0
            )
        return

    # If no error expected, check the SQL.
    vector_agg_col = get_vector_agg_column_snowflake(
        agg_func, groupby_keys=[groupby_key], groupby_column=groupby_column, index=0
    )
    expected = textwrap.dedent(
        f"""
            SELECT
              REQ."serving_name" AS "serving_name",
              AGG_0.VECTOR_AGG_RESULT AS "result"
            FROM REQ, TABLE({expected_table_func}(TABLE."parent") OVER (PARTITION BY REQ."serving_name")) AS "AGG_0"
        """
    ).strip()
    assert vector_agg_col.aggr_expr.sql(pretty=True) == expected


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
        )
        i += 1
        groupby_columns.append(groupby_column)
    groupby_expr = get_groupby_expr(
        input_expr=select_expr,
        groupby_keys=[groupby_key, groupby_key_point_in_time],
        groupby_columns=groupby_columns,
        value_by=valueby_key,
        adapter=get_sql_adapter(source_type),
    )

    result_0 = _maybe_wrap_in_variant(source_type, 'INNER_."result_0_inner"')
    result_1 = _maybe_wrap_in_variant(source_type, 'INNER_."result_1_inner"')
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
            ) AS INNER_
            GROUP BY
              INNER_."serving_name",
              INNER_."POINT_IN_TIME"
            """
    ).strip()
    assert groupby_expr.sql(pretty=True) == expected


@pytest.mark.parametrize(
    "column_params, methods, source_type",
    [
        # (
        #     [(AggFunc.MAX, DBVarType.ARRAY), (AggFunc.MAX, DBVarType.ARRAY)],
        #     ["VECTOR_AGGREGATE_MAX", "VECTOR_AGGREGATE_MAX"],
        #     SourceType.SNOWFLAKE,
        # ),
        (
            [(AggFunc.MAX, DBVarType.ARRAY), (AggFunc.MAX, DBVarType.INT)],
            ["VECTOR_AGGREGATE_MAX", "MAX"],
            SourceType.SNOWFLAKE,
        ),
    ],
)
def test_get_groupby_expr__multiple_groupby_columns__snowflake_vector_aggrs(
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
        )
        i += 1
        groupby_columns.append(groupby_column)
    groupby_expr = get_groupby_expr(
        input_expr=select_expr,
        groupby_keys=[groupby_key, groupby_key_point_in_time],
        groupby_columns=groupby_columns,
        value_by=valueby_key,
        adapter=get_sql_adapter(source_type),
    )

    result_0 = _maybe_wrap_in_variant(source_type, 'INNER_."result_0_inner"')
    result_1 = _maybe_wrap_in_variant(source_type, 'INNER_."result_1_inner"')
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
                TO_VARIANT(INNER_."result_0_inner")
              ) AS "result_0",
              OBJECT_AGG(
                CASE
                  WHEN INNER_."value_by" IS NULL
                  THEN '__MISSING__'
                  ELSE CAST(INNER_."value_by" AS TEXT)
                END,
                TO_VARIANT(INNER_."result_1_inner")
              ) AS "result_1"
            FROM (
              SELECT
                REQ."serving_name" AS "serving_name",
                REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
                REQ."value_by" AS "value_by",
                T0.result_0 AS "result_0",
                T1.result_1 AS "result_1"
              FROM REQ, (
                SELECT
                  REQ."serving_name" AS "serving_name",
                  REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
                  AGG_0.VECTOR_AGG_RESULT AS "result_0"
                FROM REQ, TABLE(
                  VECTOR_AGGREGATE_MAX(TABLE."parent") OVER (PARTITION BY REQ."serving_name", REQ."POINT_IN_TIME")
                ) AS "AGG_0"
              ) AS T0
              INNER JOIN (
                SELECT
                  REQ."serving_name" AS "serving_name",
                  REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
                  AGG_1.VECTOR_AGG_RESULT AS "result_1"
                FROM REQ, TABLE(
                  VECTOR_AGGREGATE_MAX(TABLE."parent") OVER (PARTITION BY REQ."serving_name", REQ."POINT_IN_TIME")
                ) AS "AGG_1"
              ) AS T1
                ON T0."serving_name" = T1."serving_name"
                AND T0."POINT_IN_TIME" = T1."POINT_IN_TIME"
                AND T0."value_by" = T1."value_by"
            ) AS INNER_
            GROUP BY
              INNER_."serving_name",
              INNER_."POINT_IN_TIME"
        """
    ).strip()
    assert groupby_expr.sql(pretty=True) == expected


@pytest.mark.parametrize(
    "agg_func, parent_dtype, method",
    [
        (AggFunc.SUM, None, "SUM"),
        (AggFunc.MAX, None, "MAX"),
        (AggFunc.MAX, DBVarType.INT, "MAX"),
        (AggFunc.MAX, DBVarType.ARRAY, "VECTOR_AGGREGATE_MAX"),
    ],
)
def test_get_groupby_expr(agg_func, parent_dtype, method, common_params):
    """
    Test get_groupby_expr
    """
    select_expr, groupby_key, groupby_key_point_in_time, valueby_key = common_params
    groupby_column = GroupbyColumn(
        agg_func=agg_func,
        parent_expr=(get_qualified_column_identifier("parent", "TABLE")),
        result_name="result",
        parent_dtype=parent_dtype,
    )
    groupby_expr = get_groupby_expr(
        input_expr=select_expr,
        groupby_keys=[groupby_key, groupby_key_point_in_time],
        groupby_columns=[groupby_column],
        value_by=valueby_key,
        adapter=get_sql_adapter(SourceType.SPARK),
    )
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
            INNER_."result_inner"
          ) AS "result"
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
        ) AS INNER_
        GROUP BY
          INNER_."serving_name",
          INNER_."POINT_IN_TIME"
        """
    ).strip()
    assert groupby_expr.sql(pretty=True) == expected
