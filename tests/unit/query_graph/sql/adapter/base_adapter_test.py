"""
Base adapter test class that can be reused across all adapters
"""

from typing import cast

import textwrap
from select import select

from sqlglot import select
from sqlglot.expressions import Select

from featurebyte import AggFunc
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import get_qualified_column_identifier
from featurebyte.query_graph.sql.groupby_helper import GroupbyKey, get_aggregation_expression


class BaseAdapterTest:
    """
    Base adapter test class that can be reused across all adapters
    """

    adapter: BaseAdapter

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
            )
        ]
        select_keys = [k.get_alias() for k in groupby_keys]
        keys = [k.expr for k in groupby_keys]
        agg_exprs = [get_aggregation_expression(AggFunc.SUM, "parent", None)]
        group_by_expr = adapter.group_by(select_expr, select_keys, agg_exprs, keys)
        expected = textwrap.dedent(
            """
                SELECT
                  a,
                  b,
                  REQ."serving_name" AS "serving_name",
                  SUM("parent")
                GROUP BY
                  REQ."serving_name"
            """
        ).strip()
        assert group_by_expr.sql(pretty=True) == expected
