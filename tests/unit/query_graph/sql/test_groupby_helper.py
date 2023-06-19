"""
Test groupby helper
"""
import textwrap

from sqlglot import select

from featurebyte import AggFunc, SourceType
from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import get_qualified_column_identifier
from featurebyte.query_graph.sql.groupby_helper import GroupbyColumn, GroupbyKey, get_groupby_expr


def test_get_groupby_expr():
    """
    Test get_groupby_expr
    """

    select_expr = select("a", "b", "c")
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
    groupby_column = GroupbyColumn(
        agg_func=AggFunc.SUM,
        parent_expr=(get_qualified_column_identifier("parent", "TABLE")),
        result_name="result",
    )
    groupby_expr = get_groupby_expr(
        input_expr=select_expr,
        groupby_keys=[groupby_key, groupby_key_point_in_time],
        groupby_columns=[groupby_column],
        value_by=valueby_key,
        adapter=get_sql_adapter(SourceType.SNOWFLAKE),
    )
    expected = textwrap.dedent(
        """
        SELECT
          INNER_."serving_name",
          INNER_."POINT_IN_TIME",
          OBJECT_AGG(
            CASE
              WHEN INNER_."value_by" IS NULL
              THEN '__MISSING__'
              ELSE CAST(INNER_."value_by" AS TEXT)
            END,
            TO_VARIANT(INNER_."result_inner")
          ) AS "result"
        FROM (
          SELECT
            a,
            b,
            c,
            REQ."serving_name" AS "serving_name",
            REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
            REQ."value_by" AS "value_by",
            SUM(TABLE."parent") AS "result_inner"
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
