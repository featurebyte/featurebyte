"""
Helpers to filter a table by entity
"""

from __future__ import annotations

from typing import Optional

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, select

from featurebyte.enum import InternalName
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier
from featurebyte.query_graph.sql.timestamp_helper import convert_timestamp_to_utc


def get_table_filtered_by_entity(
    input_expr: Select,
    entity_column_names: list[str],
    adapter: BaseAdapter,
    table_column_names: Optional[list[str]] = None,
    timestamp_column: Optional[str] = None,
    timestamp_metadata: Optional[DBVarTypeMetadata] = None,
    distinct: bool = False,
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
    adapter: BaseAdapter
        SQL adapter
    table_column_names: list[str]
        Column names in the table that correspond to entity_column_names if they are not the same
    timestamp_column: Optional[str]
        If specified, additionally filter using the timestamp column based on the start and end date
        specified in the entity table
    timestamp_metadata: Optional[DBVarTypeMetadata]
        Metadata for the timestamp column
    distinct: bool
        If set, select distinct entity values from the entity table. Applicable for entity tables
        with composite keys.

    Returns
    -------
    Select
    """
    entity_table = InternalName.ENTITY_TABLE_NAME.value
    start_date = InternalName.ENTITY_TABLE_START_DATE.value
    end_date = InternalName.ENTITY_TABLE_END_DATE.value

    if table_column_names is None:
        table_column_names = entity_column_names

    join_conditions: list[Expression] = []
    for entity_col, table_col in zip(entity_column_names, table_column_names):
        condition = expressions.EQ(
            this=get_qualified_column_identifier(table_col, "R"),
            expression=get_qualified_column_identifier(entity_col, entity_table),
        )
        join_conditions.append(condition)

    if timestamp_column is not None:
        timestamp_expr = get_qualified_column_identifier(timestamp_column, "R")
        if timestamp_metadata is not None and timestamp_metadata.timestamp_schema is not None:
            timestamp_expr = convert_timestamp_to_utc(
                timestamp_expr,
                timestamp_metadata.timestamp_schema,
                adapter,
            )
        normalized_timestamp_column = adapter.normalize_timestamp_before_comparison(timestamp_expr)
        join_conditions.append(
            expressions.GTE(
                this=normalized_timestamp_column,
                expression=get_qualified_column_identifier(
                    start_date, entity_table, quote_column=False
                ),
            )
        )
        join_conditions.append(
            expressions.LT(
                this=normalized_timestamp_column,
                expression=get_qualified_column_identifier(
                    end_date, entity_table, quote_column=False
                ),
            )
        )

    join_conditions_expr = expressions.and_(*join_conditions)

    select_expr = select("R.*").join(
        input_expr.subquery(),
        join_alias="R",
        join_type="inner",
        on=join_conditions_expr,
        copy=False,
    )

    if distinct:
        select_expr = select_expr.from_(
            select(*[quoted_identifier(entity_col) for entity_col in entity_column_names])
            .distinct()
            .from_(entity_table)
            .subquery(alias=expressions.Identifier(this=entity_table))
        )
    else:
        select_expr = select_expr.from_(entity_table)

    return select_expr
