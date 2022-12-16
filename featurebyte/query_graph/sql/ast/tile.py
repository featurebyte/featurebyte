"""
Module for tile related sql generation
"""
from __future__ import annotations

from typing import cast

from dataclasses import dataclass

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression, alias_, select

from featurebyte.enum import InternalName
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import SQLType, quoted_identifier
from featurebyte.query_graph.sql.specs import WindowAggregationSpec
from featurebyte.query_graph.sql.tiling import TileSpec, get_aggregator


@dataclass
class BuildTileNode(TableNode):
    """Tile builder node

    This node is responsible for generating the tile building SQL for a groupby operation.
    """

    input_node: TableNode
    keys: list[str]
    value_by: str | None
    tile_specs: list[TileSpec]
    timestamp: str
    frequency: int
    is_on_demand: bool
    is_order_dependent: bool
    query_node_type = NodeType.GROUPBY

    # Internal names
    ROW_NUMBER = "__FB_ROW_NUMBER"

    @property
    def sql(self) -> Expression:
        if self.is_on_demand:
            start_date_expr = InternalName.ENTITY_TABLE_START_DATE
        else:
            start_date_expr = InternalName.TILE_START_DATE_SQL_PLACEHOLDER

        start_date_epoch = self.context.adapter.to_epoch_seconds(
            cast(Expression, parse_one(f"CAST({start_date_expr} AS TIMESTAMP)"))
        ).sql()
        timestamp_epoch = self.context.adapter.to_epoch_seconds(
            quoted_identifier(self.timestamp)
        ).sql()

        input_tiled = select(
            "*",
            f"FLOOR(({timestamp_epoch} - {start_date_epoch}) / {self.frequency}) AS tile_index",
        ).from_(self.input_node.sql_nested())

        tile_start_date = f"TO_TIMESTAMP({start_date_epoch} + tile_index * {self.frequency})"
        keys = [quoted_identifier(k) for k in self.keys]
        if self.value_by is not None:
            keys.append(quoted_identifier(self.value_by))

        if self.is_order_dependent:
            return self._get_tile_sql_order_dependent(keys, tile_start_date, input_tiled)

        return self._get_tile_sql_order_independent(keys, tile_start_date, input_tiled)

    def _get_tile_sql_order_independent(self, keys, tile_start_date, input_tiled) -> Expression:

        if self.is_on_demand:
            groupby_keys = keys + [InternalName.ENTITY_TABLE_START_DATE.value]
        else:
            groupby_keys = keys

        groupby_sql = (
            select(
                f"{tile_start_date} AS {InternalName.TILE_START_DATE}",
                *keys,
                *[f"{spec.tile_expr} AS {spec.tile_column_name}" for spec in self.tile_specs],
            )
            .from_(input_tiled.subquery())
            .group_by("tile_index", *groupby_keys)
        )

        return groupby_sql

    def _get_tile_sql_order_dependent(self, keys, tile_start_date, input_tiled) -> Expression:

        order = expressions.Order(
            expressions=[
                expressions.Ordered(this=quoted_identifier(self.timestamp), desc=True),
            ]
        )

        def _make_window_expr(expr):
            partition_by = ["tile_index"]
            partition_by.extend([quoted_identifier(col) for col in keys])
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
            alias_(tile_start_date, alias=InternalName.TILE_START_DATE, quoted=False),
            *keys,
            *window_exprs,
        ).from_(input_tiled.subquery())

        outer_condition = expressions.EQ(
            this=quoted_identifier(self.ROW_NUMBER),
            expression=make_literal_value(1),
        )
        expr = (
            select(
                InternalName.TILE_START_DATE,
                *keys,
                *[spec.tile_column_name for spec in self.tile_specs],
            )
            .from_(inner_expr.subquery())
            .where(outer_condition)
        )

        return expr

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
        aggregator = get_aggregator(parameters["agg_func"])
        tile_specs = aggregator.tile(parameters["parent"], parameters["aggregation_id"])
        columns = (
            [InternalName.TILE_START_DATE.value]
            + parameters["keys"]
            + [spec.tile_column_name for spec in tile_specs]
        )
        columns_map = {col: quoted_identifier(col) for col in columns}
        sql_node = BuildTileNode(
            context=context,
            columns_map=columns_map,
            input_node=input_node,
            keys=parameters["keys"],
            value_by=parameters["value_by"],
            tile_specs=tile_specs,
            timestamp=parameters["timestamp"],
            frequency=parameters["frequency"],
            is_on_demand=is_on_demand,
            is_order_dependent=aggregator.is_order_dependent,
        )
        return sql_node


@dataclass
class AggregatedTilesNode(TableNode):
    """Node with tiles already aggregated

    The purpose of this node is to allow feature SQL generation to retrieve the post-aggregation
    feature transform expression. The columns_map of this node has the mapping from user defined
    feature names to internal aggregated column names. The feature expression can be obtained by
    calling get_column_expr().
    """

    query_node_type = NodeType.GROUPBY

    @property
    def sql(self) -> Expression:
        # This will not be called anywhere
        raise NotImplementedError()

    @classmethod
    def build(cls, context: SQLNodeContext) -> AggregatedTilesNode | None:
        sql_node = None
        if context.sql_type == SQLType.POST_AGGREGATION:
            agg_specs = WindowAggregationSpec.from_groupby_query_node(context.query_node)
            columns_map = {}
            for agg_spec in agg_specs:
                columns_map[agg_spec.feature_name] = quoted_identifier(agg_spec.agg_result_name)
            sql_node = AggregatedTilesNode(context=context, columns_map=columns_map)
        return sql_node
