"""
Module for input data sql generation
"""
from __future__ import annotations

from typing import Any

from dataclasses import dataclass

from sqlglot import Expression, expressions, parse_one, select

from featurebyte.enum import InternalName, SourceType
from featurebyte.query_graph.sql.ast.base import TableNode
from featurebyte.query_graph.sql.common import SQLType, escape_column_name


@dataclass
class InputNode(TableNode):
    """Input data node"""

    dbtable: dict[str, str]
    feature_store: dict[str, Any]

    @property
    def sql(self) -> Expression:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """
        # QUALIFY clause
        if self.qualify_condition is not None:
            qualify_expr = expressions.Qualify(this=self.qualify_condition)
            select_expr = expressions.Select(qualify=qualify_expr)
        else:
            select_expr = select()

        # SELECT clause
        select_args = []
        for col, expr in self.columns_map.items():
            col = expressions.Identifier(this=col, quoted=True)
            select_args.append(expressions.alias_(expr, col))
        select_expr = select_expr.select(*select_args)

        # FROM clause
        if self.feature_store["type"] == SourceType.SNOWFLAKE:
            database = self.dbtable["database_name"]
            schema = self.dbtable["schema_name"]
            table = self.dbtable["table_name"]
            dbtable = f'"{database}"."{schema}"."{table}"'
        else:
            dbtable = escape_column_name(self.dbtable["table_name"])
        select_expr = select_expr.from_(dbtable)

        # WHERE clause
        if self.where_condition is not None:
            select_expr = select_expr.where(self.where_condition)

        return select_expr


@dataclass
class BuildTileInputNode(InputNode):
    """Input data node used when building tiles"""

    timestamp: str

    @property
    def sql(self) -> Expression:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """
        table_expr = super().sql
        assert isinstance(table_expr, expressions.Select)
        # Apply tile start and end date filters on a nested subquery to avoid filtering out data
        # required by window function
        select_expr = select("*").from_(table_expr.subquery())
        timestamp = escape_column_name(self.timestamp)
        start_cond = (
            f"{timestamp} >= CAST({InternalName.TILE_START_DATE_SQL_PLACEHOLDER} AS TIMESTAMP)"
        )
        end_cond = f"{timestamp} < CAST({InternalName.TILE_END_DATE_SQL_PLACEHOLDER} AS TIMESTAMP)"
        select_expr = select_expr.where(start_cond, end_cond)
        return select_expr


@dataclass
class SelectedEntityBuildTileInputNode(InputNode):
    """Input data node used when building tiles for selected entities only

    The selected entities are expected to be available in an "entity table". It can be injected as a
    subquery by replacing the placeholder InternalName.ENTITY_TABLE_SQL_PLACEHOLDER.

    Entity table is expected to have these columns:
    * entity column(s)
    * InternalName.ENTITY_TABLE_START_DATE
    * InternalName.ENTITY_TABLE_END_DATE

    Entity column(s) is expected to be unique in the entity table (in the primary key sense).
    """

    timestamp: str
    entity_columns: list[str]

    @property
    def sql(self) -> Expression:

        entity_table = InternalName.ENTITY_TABLE_NAME.value
        start_date = InternalName.ENTITY_TABLE_START_DATE.value
        end_date = InternalName.ENTITY_TABLE_END_DATE.value

        join_conditions = []
        for col in self.entity_columns:
            condition = parse_one(f'R."{col}" = {entity_table}."{col}"')
            join_conditions.append(condition)
        join_conditions.append(parse_one(f'R."{self.timestamp}" >= {entity_table}.{start_date}'))
        join_conditions.append(parse_one(f'R."{self.timestamp}" < {entity_table}.{end_date}'))
        join_conditions_expr = expressions.and_(*join_conditions)

        table_sql = super().sql
        result = (
            select()
            .with_(entity_table, as_=InternalName.ENTITY_TABLE_SQL_PLACEHOLDER.value)
            .select("R.*", start_date)
            .from_(entity_table)
            .join(
                table_sql,
                join_alias="R",
                join_type="left",
                on=join_conditions_expr,
            )
        )
        return result


def make_input_node(
    parameters: dict[str, Any],
    sql_type: SQLType,
    groupby_keys: list[str] | None = None,
) -> BuildTileInputNode | InputNode:
    """Create a SQLNode corresponding to a query graph input node

    Parameters
    ----------
    parameters : dict[str, Any]
        Query graph node parameters
    sql_type: SQLType
        Type of SQL code to generate
    groupby_keys : list[str] | None
        List of groupby keys that is used for the downstream groupby operation. This information is
        required so that only tiles corresponding to specific entities are built (vs building tiles
        using all available data). This option is only used when SQLType is BUILD_TILE_ON_DEMAND.

    Returns
    -------
    BuildTileInputNode | InputNode | SelectedEntityBuildTileInputNode
        SQLNode corresponding to the query graph input node
    """
    columns_map = {}
    for colname in parameters["columns"]:
        columns_map[colname] = expressions.Identifier(this=colname, quoted=True)
    sql_node: BuildTileInputNode | SelectedEntityBuildTileInputNode | InputNode
    feature_store = parameters["feature_store_details"]
    if sql_type == SQLType.BUILD_TILE:
        sql_node = BuildTileInputNode(
            columns_map=columns_map,
            timestamp=parameters["timestamp"],
            dbtable=parameters["table_details"],
            feature_store=feature_store,
        )
    elif sql_type == SQLType.BUILD_TILE_ON_DEMAND:
        assert groupby_keys is not None
        sql_node = SelectedEntityBuildTileInputNode(
            columns_map=columns_map,
            timestamp=parameters["timestamp"],
            dbtable=parameters["table_details"],
            feature_store=feature_store,
            entity_columns=groupby_keys,
        )
    else:
        sql_node = InputNode(
            columns_map=columns_map,
            dbtable=parameters["table_details"],
            feature_store=feature_store,
        )
    return sql_node
