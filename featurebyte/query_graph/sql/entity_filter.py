"""
Helpers to filter a table by entity
"""

from __future__ import annotations

from typing import Optional

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, select

from featurebyte.enum import InternalName
from featurebyte.query_graph.sql.common import get_qualified_column_identifier


def get_table_filtered_by_entity(
    input_expr: Select,
    entity_column_names: list[str],
    timestamp_column: Optional[str] = None,
    inject_entity_table_placeholder: bool = False,
) -> Select:
    """
    Construct sql to filter the data used when building tiles for selected entities only

    The selected entities are expected to be available in an "entity table". It can be injected
    as a subquery by replacing the placeholder InternalName.ENTITY_TABLE_SQL_PLACEHOLDER.

    Entity table is expected to have these columns:
    * entity column(s)
    * InternalName.ENTITY_TABLE_START_DATE
    * InternalName.ENTITY_TABLE_END_DATE

    Parameters
    ----------
    input_expr: Select
        Input table to be filtered
    entity_column_names: list[str]
        Entity column name(s) that the filter will be based on
    timestamp_column: Optional[str]
        If specified, additionally filter using the timestamp column based on the start and end date
        specified in the entity table
    inject_entity_table_placeholder: bool
        If set, define the entity table placeholder as a CTE in the generated select statement

    Returns
    -------
    Select
    """
    entity_table = InternalName.ENTITY_TABLE_NAME.value
    start_date = InternalName.ENTITY_TABLE_START_DATE.value
    end_date = InternalName.ENTITY_TABLE_END_DATE.value

    join_conditions: list[Expression] = []
    for col in entity_column_names:
        condition = expressions.EQ(
            this=get_qualified_column_identifier(col, "R"),
            expression=get_qualified_column_identifier(col, entity_table),
        )
        join_conditions.append(condition)

    if timestamp_column is not None:
        join_conditions.append(
            expressions.GTE(
                this=get_qualified_column_identifier(timestamp_column, "R"),
                expression=get_qualified_column_identifier(
                    start_date, entity_table, quote_column=False
                ),
            )
        )
        join_conditions.append(
            expressions.LT(
                this=get_qualified_column_identifier(timestamp_column, "R"),
                expression=get_qualified_column_identifier(
                    end_date, entity_table, quote_column=False
                ),
            )
        )

    join_conditions_expr = expressions.and_(*join_conditions)
    if inject_entity_table_placeholder:
        select_expr = select().with_(
            entity_table, as_=InternalName.ENTITY_TABLE_SQL_PLACEHOLDER.value
        )
    else:
        select_expr = select()
    select_expr = (
        select_expr.select("R.*")
        .from_(entity_table)
        .join(
            input_expr.subquery(),
            join_alias="R",
            join_type="inner",
            on=join_conditions_expr,
            copy=False,
        )
    )
    return select_expr
