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
    join_on_serving_names: bool = True,
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
    join_on_serving_names: bool
        Whether to include serving names in the join condition. Default is True. Otherwise, the join
        condition will only include the distinct key, and the serving names will be selected from the
        aggregated table as additional output columns. This is useful when the distinct point in
        time request table is independent of the serving names, e.g. in aggregate_asat for
        SnapshotsTable.

    Returns
    -------
    Select
    """
    join_key_columns = [distinct_key]
    if join_on_serving_names:
        join_key_columns += serving_names
    join_condition = expressions.and_(*[
        expressions.EQ(
            this=get_qualified_column_identifier(column_name, "AGGREGATED"),
            expression=get_qualified_column_identifier(column_name, "DISTINCT_POINT_IN_TIME"),
        )
        for column_name in join_key_columns
    ])
    columns_from_distinct_point_in_time = [SpecialColumnName.POINT_IN_TIME.value]
    columns_from_aggregated = aggregated_column_names[:]
    if join_on_serving_names:
        columns_from_distinct_point_in_time += serving_names
    else:
        columns_from_aggregated += serving_names
    aggregated_expr = (
        select(
            *[
                get_qualified_column_identifier(
                    column_name,
                    "DISTINCT_POINT_IN_TIME",
                )
                for column_name in columns_from_distinct_point_in_time
            ],
            *[
                get_qualified_column_identifier(
                    column_name,
                    "AGGREGATED",
                )
                for column_name in columns_from_aggregated
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
