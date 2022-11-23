"""
Module for input data sql generation
"""
from __future__ import annotations

from typing import Any, cast

from dataclasses import dataclass

from sqlglot import Expression, Select, expressions, parse_one, select

from featurebyte.enum import InternalName, SourceType, TableDataType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import SQLType, quoted_identifier


def is_event_data(parameters: dict[str, Any]) -> bool:
    """
    Returns whether the input data is an EventData

    Parameters
    ----------
    parameters : dict
        Query node parameters

    Returns
    -------
    bool
    """
    return cast(bool, parameters["type"] == TableDataType.EVENT_DATA)


@dataclass
class InputNode(TableNode):
    """Input data node"""

    dbtable: dict[str, str]
    feature_store: dict[str, Any]
    query_node_type = NodeType.INPUT

    def from_query_impl(self, select_expr: Select) -> Select:
        if self.feature_store["type"] in {SourceType.SNOWFLAKE, SourceType.DATABRICKS}:
            database = self.dbtable["database_name"]
            schema = self.dbtable["schema_name"]
            table = self.dbtable["table_name"]
            # expressions.Table's notation for three part fully qualified name is
            # {catalog}.{db}.{this}
            dbtable = expressions.Table(
                this=quoted_identifier(table),
                db=quoted_identifier(schema),
                catalog=quoted_identifier(database),
            )
        else:
            dbtable = quoted_identifier(self.dbtable["table_name"])
        select_expr = select_expr.from_(dbtable)
        return select_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> InputNode | None:
        if is_event_data(context.parameters) and context.sql_type in {
            SQLType.BUILD_TILE,
            SQLType.BUILD_TILE_ON_DEMAND,
        }:
            return None
        columns_map = cls.make_input_columns_map(context)
        feature_store = context.parameters["feature_store_details"]
        sql_node = InputNode(
            context=context,
            columns_map=columns_map,
            dbtable=context.parameters["table_details"],
            feature_store=feature_store,
        )
        return sql_node

    @classmethod
    def make_input_columns_map(cls, context: SQLNodeContext) -> dict[str, Expression]:
        """
        Construct mapping from column name to expression

        Parameters
        ----------
        context : SQLNodeContext
            Context to build SQLNode

        Returns
        -------
        dict[str, Expression]
        """
        columns_map = {}
        for colname in context.parameters["columns"]:
            columns_map[colname] = expressions.Identifier(this=colname, quoted=True)
        return columns_map


@dataclass
class BuildTileInputNode(InputNode):
    """Input data node used when building tiles"""

    timestamp: str
    query_node_type = NodeType.INPUT

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
        timestamp = quoted_identifier(self.timestamp).sql()
        start_cond = (
            f"{timestamp} >= CAST({InternalName.TILE_START_DATE_SQL_PLACEHOLDER} AS TIMESTAMP)"
        )
        end_cond = f"{timestamp} < CAST({InternalName.TILE_END_DATE_SQL_PLACEHOLDER} AS TIMESTAMP)"
        select_expr = select_expr.where(start_cond, end_cond)
        return select_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> BuildTileInputNode | None:
        if context.sql_type != SQLType.BUILD_TILE or not is_event_data(context.parameters):
            # Filtering input data with a date range is only relevant for EventData
            return None
        columns_map = cls.make_input_columns_map(context)
        feature_store = context.parameters["feature_store_details"]
        sql_node = BuildTileInputNode(
            context=context,
            columns_map=columns_map,
            timestamp=context.parameters["timestamp"],
            dbtable=context.parameters["table_details"],
            feature_store=feature_store,
        )
        return sql_node


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
    query_node_type = NodeType.INPUT

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

    @classmethod
    def build(cls, context: SQLNodeContext) -> SelectedEntityBuildTileInputNode | None:
        if context.sql_type != SQLType.BUILD_TILE_ON_DEMAND:
            return None
        columns_map = cls.make_input_columns_map(context)
        feature_store = context.parameters["feature_store_details"]
        assert context.groupby_keys is not None
        sql_node = SelectedEntityBuildTileInputNode(
            context=context,
            columns_map=columns_map,
            timestamp=context.parameters["timestamp"],
            dbtable=context.parameters["table_details"],
            feature_store=feature_store,
            entity_columns=context.groupby_keys,
        )
        return sql_node
