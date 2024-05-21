"""
Helpers to handle duplicated rows in a table
"""

from __future__ import annotations

from sqlglot.expressions import Select, alias_, select

from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import quoted_identifier


def get_deduplicated_expr(
    adapter: BaseAdapter, table_expr: Select, expected_primary_keys: list[str]
) -> Select:
    """
    Remove duplicate rows based on the provided list of columns

    Parameters
    ----------
    adapter: BaseAdapter
        BaseAdapter object
    table_expr: Select
        Expression of the table to be deduplicated
    expected_primary_keys: list[str]
        The columns that will be used to determine duplicates

    Returns
    -------
    Select
    """
    deduplicated_columns = []
    for col_expr in table_expr.expressions:
        col_name = col_expr.alias or col_expr.name
        if col_name in expected_primary_keys:
            deduplicated_columns.append(quoted_identifier(col_name))
        else:
            deduplicated_columns.append(
                alias_(adapter.any_value(quoted_identifier(col_name)), alias=col_name, quoted=True)
            )
    deduplicated_expr = (
        select(*deduplicated_columns)
        .from_(table_expr.subquery())
        .group_by(*[quoted_identifier(col) for col in expected_primary_keys])
    )
    return deduplicated_expr
