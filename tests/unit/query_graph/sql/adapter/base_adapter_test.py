"""
Base adapter test class that can be reused across all adapters
"""

from typing import cast

import textwrap
from select import select

from sqlglot import select
from sqlglot.expressions import Select

from featurebyte import AggFunc
from featurebyte.enum import DBVarType
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import get_qualified_column_identifier
from featurebyte.query_graph.sql.groupby_helper import (
    GroupbyColumn,
    GroupbyKey,
    get_aggregation_expression,
    get_vector_agg_expr_snowflake,
)


class BaseAdapterTest:
    """
    Base adapter test class that can be reused across all adapters
    """

    adapter: BaseAdapter

    @classmethod
    def get_group_by_expected_result(cls) -> str:
        """
        Returns expected result of group by query
        """
        return textwrap.dedent(
            """
                SELECT
                  a,
                  b,
                  REQ."serving_name" AS "serving_name",
                  REQ."serving_name_2" AS "serving_name_2",
                  SUM("parent"),
                  AVG("parent_avg")
                GROUP BY
                  REQ."serving_name",
                  REQ."serving_name_2"
            """
        ).strip()

    def test_group_by(self):
        """
        Test group_by method
        """
        adapter = self.adapter
        select_expr: Select = cast(Select, select("a", "b"))
        groupby_keys = [
            GroupbyKey(
                expr=get_qualified_column_identifier("serving_name", "REQ"),
                name="serving_name",
            ),
            GroupbyKey(
                expr=get_qualified_column_identifier("serving_name_2", "REQ"),
                name="serving_name_2",
            ),
        ]
        select_keys = [k.get_alias() for k in groupby_keys]
        keys = [k.expr for k in groupby_keys]
        agg_exprs = [
            get_aggregation_expression(AggFunc.SUM, "parent", None),
            get_aggregation_expression(AggFunc.AVG, "parent_avg", None),
        ]
        vector_aggregate_exprs = [
            get_vector_agg_expr_snowflake(
                AggFunc.SUM,
                groupby_keys,
                GroupbyColumn(
                    AggFunc.SUM,
                    parent_expr=(get_qualified_column_identifier("parent", "TABLE")),
                    parent_dtype=DBVarType.ARRAY,
                    result_name="result",
                ),
                0,
            ),
            get_vector_agg_expr_snowflake(
                AggFunc.SUM,
                groupby_keys,
                GroupbyColumn(
                    AggFunc.SUM,
                    parent_expr=(get_qualified_column_identifier("parent2", "TABLE")),
                    parent_dtype=DBVarType.ARRAY,
                    result_name="result2",
                ),
                1,
            ),
            get_vector_agg_expr_snowflake(
                AggFunc.SUM,
                groupby_keys,
                GroupbyColumn(
                    AggFunc.SUM,
                    parent_expr=(get_qualified_column_identifier("parent3", "TABLE")),
                    parent_dtype=DBVarType.ARRAY,
                    result_name="result3",
                ),
                2,
            ),
        ]
        group_by_expr = adapter.group_by(
            select_expr, select_keys, agg_exprs, keys, vector_aggregate_exprs
        )
        expected = self.get_group_by_expected_result()
        assert group_by_expr.sql(pretty=True) == expected
