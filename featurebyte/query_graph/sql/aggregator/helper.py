"""
Helper functions for implementing aggregators
"""

from __future__ import annotations

from sqlglot import expressions
from sqlglot.expressions import Select, select

from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier


def join_aggregated_expr_with_distinct_point_in_time(
    aggregated_expr: Select,
    distinct_key: str,
    serving_names: list[str],
    aggregated_column_names: list[str],
    distinct_by_point_in_time_table_name: str,
) -> Select:
    """
    Join aggregated result that is distinct by a different key (e.g. scheduled feature job time) and
    serving names with the distinct by point in time request table. This is usually the last step in
    the aggregation process.

    The result can be used to construct a LeftJoinableSubquery that can be used to join with the
    actual request table in the final query.

    Parameters
    ----------
    aggregated_expr: Select
        Aggregated expression
    distinct_key: str
        Column name of the distinct key, such as the scheduled feature job time
    serving_names: list[str]
        Serving names
    aggregated_column_names: list[str]
        Aggregated column names
    distinct_by_point_in_time_table_name: str
        Table name of the distinct by point in time request table. This table has both the
        POINT_IN_TIME and the distinct_key column.

    Returns
    -------
    Select
    """
    join_condition = expressions.and_(*[
        expressions.EQ(
            this=get_qualified_column_identifier(column_name, "AGGREGATED"),
            expression=get_qualified_column_identifier(column_name, "DISTINCT_POINT_IN_TIME"),
        )
        for column_name in [distinct_key] + serving_names
    ])
    aggregated_expr = (
        select(
            *[
                get_qualified_column_identifier(
                    column_name,
                    "DISTINCT_POINT_IN_TIME",
                )
                for column_name in [SpecialColumnName.POINT_IN_TIME.value] + serving_names
            ],
            *[
                get_qualified_column_identifier(
                    column_name,
                    "AGGREGATED",
                )
                for column_name in aggregated_column_names
            ],
        )
        .from_(
            expressions.Table(
                this=quoted_identifier(distinct_by_point_in_time_table_name),
                alias="DISTINCT_POINT_IN_TIME",
            )
        )
        .join(
            expressions.Table(this=aggregated_expr.subquery(), alias="AGGREGATED"),
            join_type="left",
            on=join_condition,
        )
    )
    return aggregated_expr
