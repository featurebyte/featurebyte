"""
Module for tile related sql generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from sqlglot.expressions import Expression, Select, alias_

from featurebyte.enum import InternalName
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import SQLType, quoted_identifier
from featurebyte.query_graph.sql.query_graph_util import get_parent_dtype
from featurebyte.query_graph.sql.tile_compute_spec import TileComputeSpec, TileTableInputColumn
from featurebyte.query_graph.sql.tiling import InputColumn, TileSpec, get_aggregator
from featurebyte.query_graph.sql.timestamp_helper import convert_timestamp_to_utc


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
    timestamp_metadata: DBVarTypeMetadata | None
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
        raise RuntimeError("sql property is not supported for BuildTileNode")

    def get_tile_compute_spec(self) -> TileComputeSpec:
        """
        Get a TileComputeSpec object that fully describes the tile computation.

        Returns
        -------
        TileComputeSpec
        """
        source_expr = cast(Select, self.input_node.get_sql_for_expressions([]))
        if self.context.parameters["parent"] is None:
            value_columns = []
        else:
            parent_col = self.context.parameters["parent"]
            value_columns = [
                TileTableInputColumn(
                    name=self._get_aggregation_input_column_name(
                        self.context.parameters["aggregation_id"]
                    ),
                    expr=self.input_node.columns_map[parent_col],
                )
            ]
        key_columns = [
            TileTableInputColumn(name=col, expr=self.input_node.columns_map[col])
            for col in self.keys
        ]
        if self.value_by is not None:
            value_by_column = TileTableInputColumn(
                name=self.value_by, expr=self.input_node.columns_map[self.value_by]
            )
        else:
            value_by_column = None
        timestamp_column = TileTableInputColumn(
            name=self.timestamp, expr=self.input_node.columns_map[self.timestamp]
        )
        if (
            self.timestamp_metadata is not None
            and self.timestamp_metadata.timestamp_schema is not None
        ):
            timestamp_expr = convert_timestamp_to_utc(
                quoted_identifier(self.timestamp),
                self.timestamp_metadata.timestamp_schema,
                self.adapter,
            )
        else:
            timestamp_expr = self.context.adapter.convert_to_utc_timestamp(
                quoted_identifier(self.timestamp)
            )
        tile_index_expr = alias_(
            self.adapter.call_udf(
                "F_TIMESTAMP_TO_INDEX",
                [
                    timestamp_expr,
                    make_literal_value(self.time_modulo_frequency),
                    make_literal_value(self.blind_spot),
                    make_literal_value(self.frequency_minute),
                ],
            ),
            alias="index",
        )
        return TileComputeSpec(
            source_expr=source_expr,
            entity_table_expr=None,
            timestamp_column=timestamp_column,
            timestamp_metadata=self.timestamp_metadata,
            key_columns=key_columns,
            value_by_column=value_by_column,
            value_columns=value_columns,
            tile_index_expr=tile_index_expr,
            tile_column_specs=self.tile_specs,
            is_order_dependent=self.is_order_dependent,
            is_on_demand=self.is_on_demand,
            source_info=self.context.source_info,
        )

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
            parent_dtype = None
            parent_column = None
        else:
            parent_dtype = get_parent_dtype(parameters["parent"], context.graph, context.query_node)
            parent_column = InputColumn(
                name=cls._get_aggregation_input_column_name(parameters["aggregation_id"]),
                dtype=parent_dtype,
            )
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
        if parameters["timestamp_metadata"] is None:
            timestamp_metadata = None
        else:
            timestamp_metadata = DBVarTypeMetadata(**parameters["timestamp_metadata"])
        sql_node = BuildTileNode(
            context=context,
            columns_map=columns_map,
            input_node=input_node,
            keys=parameters["keys"],
            value_by=parameters["value_by"],
            tile_specs=tile_specs,
            timestamp=parameters["timestamp"],
            timestamp_metadata=timestamp_metadata,
            frequency_minute=fjs.period_seconds // 60,
            blind_spot=fjs.blind_spot_seconds,
            time_modulo_frequency=fjs.offset_seconds,
            is_on_demand=is_on_demand,
            is_order_dependent=aggregator.is_order_dependent,
            adapter=context.adapter,
        )
        return sql_node

    @staticmethod
    def _get_aggregation_input_column_name(aggregation_id: str) -> str:
        """
        Get the column that should be constructed before performing tile aggregation based on the
        aggregation id. The original column name doesn't matter because that is already factored in
        the aggregation id.

        Parameters
        ----------
        aggregation_id : str
            Aggregation id

        Returns
        -------
        str
        """
        return f"input_col_{aggregation_id}"
