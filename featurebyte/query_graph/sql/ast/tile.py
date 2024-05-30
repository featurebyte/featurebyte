"""
Module for tile related sql generation
"""

from __future__ import annotations

from typing import cast

from dataclasses import dataclass

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, alias_, select

from featurebyte.enum import DBVarType, InternalName
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    SQLType,
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.groupby_helper import (
    GroupbyColumn,
    GroupbyKey,
    _split_agg_and_snowflake_vector_aggregation_columns,
)
from featurebyte.query_graph.sql.query_graph_util import get_parent_dtype
from featurebyte.query_graph.sql.tiling import InputColumn, TileSpec, get_aggregator


@dataclass
class BuildTileNode(TableNode):  # pylint: disable=too-many-instance-attributes
    """Tile builder node

    This node is responsible for generating the tile building SQL for a groupby operation.
    """

    input_node: TableNode
    keys: list[str]
    value_by: str | None
    tile_specs: list[TileSpec]
    timestamp: str
    frequency_minute: int
    blind_spot: int
    time_modulo_frequency: int

    is_on_demand: bool
    is_order_dependent: bool
    query_node_type = NodeType.GROUPBY

    adapter: BaseAdapter

    # Internal names
    ROW_NUMBER = "__FB_ROW_NUMBER"

    @property
    def sql(self) -> Expression:
        input_filtered = self._get_input_filtered_within_date_range()
        tile_index_expr = alias_(
            expressions.Anonymous(
                this="F_TIMESTAMP_TO_INDEX",
                expressions=[
                    self.context.adapter.convert_to_utc_timestamp(
                        quoted_identifier(self.timestamp)
                    ),
                    make_literal_value(self.time_modulo_frequency),
                    make_literal_value(self.blind_spot),
                    make_literal_value(self.frequency_minute),
                ],
            ),
            alias="index",
        )
        input_tiled = select("*", tile_index_expr).from_(input_filtered.subquery(copy=False))
        keys = [quoted_identifier(k) for k in self.keys]
        if self.value_by is not None:
            keys.append(quoted_identifier(self.value_by))

        if self.is_order_dependent:
            return self._get_tile_sql_order_dependent(keys, input_tiled)

        return self._get_tile_sql_order_independent(keys, input_tiled)

    def _get_input_filtered_within_date_range(self) -> Select:
        """
        Construct sql to filter input data to be within a date range. Only data within this range
        will be used to calculate tiles.

        Returns
        -------
        Select
        """
        if self.is_on_demand:
            return self._get_input_filtered_within_date_range_on_demand()
        return self._get_input_filtered_within_date_range_scheduled()

    def _get_input_filtered_within_date_range_scheduled(self) -> Select:
        """
        Construct sql with placeholders for start and end dates to be filled in by the scheduled
        tile computation jobs

        Returns
        -------
        Select
        """
        select_expr = select("*").from_(self.input_node.sql_nested())
        timestamp = quoted_identifier(self.timestamp).sql()
        start_cond = (
            f"{timestamp} >= CAST({InternalName.TILE_START_DATE_SQL_PLACEHOLDER} AS TIMESTAMP)"
        )
        end_cond = f"{timestamp} < CAST({InternalName.TILE_END_DATE_SQL_PLACEHOLDER} AS TIMESTAMP)"
        select_expr = select_expr.where(start_cond, end_cond)
        return select_expr

    def _get_input_filtered_within_date_range_on_demand(self) -> Select:
        """
        Construct sql to filter the data used when building tiles for selected entities only

        The selected entities are expected to be available in an "entity table". It can be injected
        as a subquery by replacing the placeholder InternalName.ENTITY_TABLE_SQL_PLACEHOLDER.

        Entity table is expected to have these columns:
        * entity column(s)
        * InternalName.ENTITY_TABLE_START_DATE
        * InternalName.ENTITY_TABLE_END_DATE

        Returns
        -------
        Select
        """
        entity_table = InternalName.ENTITY_TABLE_NAME.value
        start_date = InternalName.ENTITY_TABLE_START_DATE.value
        end_date = InternalName.ENTITY_TABLE_END_DATE.value

        join_conditions: list[Expression] = []
        for col in self.keys:
            condition = expressions.EQ(
                this=get_qualified_column_identifier(col, "R"),
                expression=get_qualified_column_identifier(col, entity_table),
            )
            join_conditions.append(condition)
        join_conditions.append(
            expressions.GTE(
                this=get_qualified_column_identifier(self.timestamp, "R"),
                expression=get_qualified_column_identifier(
                    start_date, entity_table, quote_column=False
                ),
            )
        )
        join_conditions.append(
            expressions.LT(
                this=get_qualified_column_identifier(self.timestamp, "R"),
                expression=get_qualified_column_identifier(
                    end_date, entity_table, quote_column=False
                ),
            )
        )
        join_conditions_expr = expressions.and_(*join_conditions)

        select_expr = (
            select()
            .with_(entity_table, as_=InternalName.ENTITY_TABLE_SQL_PLACEHOLDER.value)
            .select("R.*")
            .from_(entity_table)
            .join(
                self.input_node.sql,
                join_alias="R",
                join_type="inner",
                on=join_conditions_expr,
                copy=False,
            )
        )
        return cast(Select, select_expr)

    @staticmethod
    def _get_db_var_type_from_col_type(col_type: str) -> DBVarType:
        mapping: dict[str, DBVarType] = {"ARRAY": DBVarType.ARRAY, "EMBEDDING": DBVarType.EMBEDDING}
        return mapping.get(col_type, DBVarType.UNKNOWN)

    def _get_tile_sql_order_independent(
        self,
        keys: list[Expression],
        input_tiled: expressions.Select,
    ) -> Expression:
        inner_groupby_keys = [expressions.Identifier(this="index"), *keys]
        groupby_keys = [GroupbyKey(expr=key, name=key.name) for key in inner_groupby_keys]
        original_query = select().from_(input_tiled.subquery(copy=False))

        groupby_columns = []
        for spec in self.tile_specs:
            groupby_columns.append(
                GroupbyColumn(
                    parent_dtype=self._get_db_var_type_from_col_type(spec.tile_column_type),
                    agg_func=spec.tile_aggregation_type,
                    parent_expr=spec.tile_expr,
                    result_name=spec.tile_column_name,
                    parent_cols=spec.tile_expr.expressions or [spec.tile_expr.alias_or_name],
                    quote_result_name=False,
                )
            )
        agg_exprs, vector_agg_exprs = _split_agg_and_snowflake_vector_aggregation_columns(
            original_query,
            groupby_keys,
            groupby_columns,
            None,
            self.adapter.source_type,
            is_tile=True,
        )

        return self.adapter.group_by(
            original_query,
            select_keys=inner_groupby_keys,
            agg_exprs=agg_exprs,
            keys=inner_groupby_keys,
            vector_aggregate_columns=vector_agg_exprs,
            quote_vector_agg_aliases=False,
        )

    def _get_tile_sql_order_dependent(
        self,
        keys: list[Expression],
        input_tiled: expressions.Select,
    ) -> Expression:
        def _make_window_expr(expr: str | Expression) -> Expression:
            order = expressions.Order(
                expressions=[
                    expressions.Ordered(this=quoted_identifier(self.timestamp), desc=True),
                ]
            )
            partition_by = [cast(Expression, expressions.Identifier(this="index"))] + keys
            window_expr = expressions.Window(this=expr, partition_by=partition_by, order=order)
            return window_expr

        window_exprs = [
            alias_(
                _make_window_expr(expressions.Anonymous(this="ROW_NUMBER")), alias=self.ROW_NUMBER
            ),
            *[
                alias_(_make_window_expr(spec.tile_expr), alias=spec.tile_column_name)
                for spec in self.tile_specs
            ],
        ]
        inner_expr = select(
            "index",
            *keys,
            *window_exprs,
        ).from_(input_tiled.subquery(copy=False), copy=False)

        outer_condition = expressions.EQ(
            this=quoted_identifier(self.ROW_NUMBER),
            expression=make_literal_value(1),
        )
        tile_expr = (
            select(
                "index",
                *keys,
                *[spec.tile_column_name for spec in self.tile_specs],
            )
            .from_(inner_expr.subquery(copy=False))
            .where(outer_condition, copy=False)
        )

        return tile_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> BuildTileNode | None:
        sql_node = None
        if context.sql_type == SQLType.BUILD_TILE:
            sql_node = cls.make_build_tile_node(context, is_on_demand=False)
        elif context.sql_type == SQLType.BUILD_TILE_ON_DEMAND:
            sql_node = cls.make_build_tile_node(context, is_on_demand=True)
        return sql_node

    @classmethod
    def make_build_tile_node(cls, context: SQLNodeContext, is_on_demand: bool) -> BuildTileNode:
        """Create a BuildTileNode

        Parameters
        ----------
        context : SQLNodeContext
            SQLNodeContext object
        is_on_demand : bool
            Whether the SQL is for on-demand tile building for historical features

        Returns
        -------
        BuildTileNode
        """
        parameters = context.parameters
        input_node = context.input_sql_nodes[0]
        assert isinstance(input_node, TableNode)
        if parameters["parent"] is None:
            parent_column = None
            parent_dtype = None
        else:
            parent_dtype = get_parent_dtype(parameters["parent"], context.graph, context.query_node)
            parent_column = InputColumn(name=parameters["parent"], dtype=parent_dtype)
        aggregator = get_aggregator(
            parameters["agg_func"], adapter=context.adapter, parent_dtype=parent_dtype
        )
        tile_specs = aggregator.tile(parent_column, parameters["aggregation_id"])
        columns = (
            [InternalName.TILE_START_DATE.value]
            + parameters["keys"]
            + [spec.tile_column_name for spec in tile_specs]
        )
        columns_map = {col: quoted_identifier(col) for col in columns}
        fjs = FeatureJobSetting(**parameters["feature_job_setting"])
        sql_node = BuildTileNode(
            context=context,
            columns_map=columns_map,
            input_node=input_node,
            keys=parameters["keys"],
            value_by=parameters["value_by"],
            tile_specs=tile_specs,
            timestamp=parameters["timestamp"],
            frequency_minute=fjs.period_seconds // 60,
            blind_spot=fjs.blind_spot_seconds,
            time_modulo_frequency=fjs.offset_seconds,
            is_on_demand=is_on_demand,
            is_order_dependent=aggregator.is_order_dependent,
            adapter=context.adapter,
        )
        return sql_node
