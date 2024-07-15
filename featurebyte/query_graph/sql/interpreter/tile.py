"""
This module contains the Query Graph Interpreter
"""

from __future__ import annotations

from typing import Optional, cast

from dataclasses import dataclass

from featurebyte.enum import SourceType
from featurebyte.query_graph.algorithm import dfs_traversal
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupByNode
from featurebyte.query_graph.node.metadata.operation import SourceDataColumn
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import EventTableTimestampFilter, SQLType
from featurebyte.query_graph.sql.interpreter.base import BaseGraphInterpreter
from featurebyte.query_graph.sql.template import SqlExpressionTemplate
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.transform.pruning import prune_query_graph


@dataclass
class TileGenSql:
    """Information about a tile building SQL

    This information is required by the Tile Manager to perform tile related operations such as
    scheduling tile computation jobs.

    Parameters
    ----------
    sql_template: SqlExpressionTemplate
        Templated SQL code for building tiles
    columns : List[str]
        List of columns in the tile table after executing the SQL code
    time_modulo_frequency: int
        Offset used to determine the time for jobs scheduling. Should be smaller than frequency.
    frequency : int
        Job frequency. Needed for job scheduling.
    blind_spot : int
        Blind spot. Needed for job scheduling.
    windows : list[str | None]
        List of window sizes. Not needed for job scheduling, but can be used for other purposes such
        as determining the required tiles to build on demand during preview.
    """

    # pylint: disable=too-many-instance-attributes
    tile_table_id: str
    tile_id_version: int
    aggregation_id: str
    sql_template: SqlExpressionTemplate
    columns: list[str]
    entity_columns: list[str]
    tile_value_columns: list[str]
    tile_value_types: list[str]

    # group-by node parameters
    time_modulo_frequency: int
    frequency: int
    blind_spot: int
    windows: list[str | None]
    offset: str | None
    serving_names: list[str]
    value_by_column: str | None
    parent: str | None
    agg_func: str | None

    @property
    def sql(self) -> str:
        """
        Templated SQL code for building tiles

        Returns
        -------
        str
        """
        return cast(str, self.sql_template.render())


class TileSQLGenerator:
    """Generator for Tile-building SQL

    Parameters
    ----------
    query_graph : QueryGraphModel
    """

    def __init__(self, query_graph: QueryGraphModel, is_on_demand: bool, source_type: SourceType):
        self.query_graph = query_graph
        self.is_on_demand = is_on_demand
        self.source_type = source_type

    def construct_tile_gen_sql(self, starting_node: Node) -> list[TileGenSql]:
        """Construct a list of tile building SQLs for the given Query Graph

        There can be more than one tile table to build if the feature depends on more than one
        groupby operations. However, before we support complex features, there will only be one tile
        table to build.

        Parameters
        ----------
        starting_node : Node
            Starting node (typically corresponding to selected features) to search from

        Returns
        -------
        list[TileGenSql]
        """
        # Groupby operations requires building tiles (assuming the aggregation type supports tiling)
        sqls = []
        for groupby_node in self.query_graph.iterate_nodes(starting_node, NodeType.GROUPBY):
            assert isinstance(groupby_node, GroupByNode)

            if self.is_on_demand:
                event_table_timestamp_filter = None
            else:
                event_table_timestamp_filter = self._get_event_table_timestamp_filter(groupby_node)

            info = self.make_one_tile_sql(groupby_node, event_table_timestamp_filter)
            sqls.append(info)

        return sqls

    def _get_event_table_timestamp_filter(
        self, groupby_node: GroupByNode
    ) -> Optional[EventTableTimestampFilter]:
        pruned_graph, node_name_map, _ = prune_query_graph(
            graph=self.query_graph, node=groupby_node
        )

        # Check there is no lag operation, otherwise cannot apply timestamp filter directly
        pruned_node = pruned_graph.get_node_by_name(node_name_map[groupby_node.name])
        for node in dfs_traversal(pruned_graph, pruned_node):
            if node.type == NodeType.LAG:
                return None

        # Identify which EventTable can be filtered. This is based on the timestamp column used in
        # the groupby node.
        groupby_input_node = pruned_graph.get_node_by_name(
            pruned_graph.get_input_node_names(pruned_node)[0]
        )
        op_struct_info = OperationStructureExtractor(graph=pruned_graph).extract(groupby_input_node)
        op_struct = op_struct_info.operation_structure_map[groupby_input_node.name]
        for column in op_struct.columns:
            if (
                column.name == groupby_node.parameters.timestamp
                and isinstance(column, SourceDataColumn)
                and column.table_id is not None
            ):
                assert isinstance(column, SourceDataColumn), "SourceDataColumn expected"
                return EventTableTimestampFilter(
                    timestamp_column_name=column.name,
                    event_table_id=column.table_id,
                )

        return None

    def make_one_tile_sql(
        self,
        groupby_node: GroupByNode,
        event_table_timestamp_filter: Optional[EventTableTimestampFilter],
    ) -> TileGenSql:
        """Construct tile building SQL for a specific groupby query graph node

        Parameters
        ----------
        groupby_node: GroupByNode
            Groupby query graph node
        event_table_timestamp_filter: Optional[EventTableTimestampFilter]
            Event table timestamp filter to apply if applicable

        Returns
        -------
        TileGenSql
        """
        if self.is_on_demand:
            sql_type = SQLType.BUILD_TILE_ON_DEMAND
        else:
            sql_type = SQLType.BUILD_TILE
        groupby_sql_node = SQLOperationGraph(
            query_graph=self.query_graph,
            sql_type=sql_type,
            source_type=self.source_type,
            event_table_timestamp_filter=event_table_timestamp_filter,
        ).build(groupby_node)
        sql = groupby_sql_node.sql
        tile_table_id = groupby_node.parameters.tile_id
        aggregation_id = groupby_node.parameters.aggregation_id
        entity_columns = groupby_sql_node.keys
        tile_value_columns = [spec.tile_column_name for spec in groupby_sql_node.tile_specs]
        tile_value_types = [spec.tile_column_type for spec in groupby_sql_node.tile_specs]
        assert tile_table_id is not None, "Tile table ID is required"
        assert aggregation_id is not None, "Aggregation ID is required"
        sql_template = SqlExpressionTemplate(sql_expr=sql, source_type=self.source_type)
        fjs = groupby_node.parameters.feature_job_setting
        info = TileGenSql(
            tile_table_id=tile_table_id,
            tile_id_version=groupby_node.parameters.tile_id_version,
            aggregation_id=aggregation_id,
            sql_template=sql_template,
            columns=groupby_sql_node.columns,
            entity_columns=entity_columns,
            tile_value_columns=tile_value_columns,
            tile_value_types=tile_value_types,
            time_modulo_frequency=fjs.offset_seconds,
            frequency=fjs.period_seconds,
            blind_spot=fjs.blind_spot_seconds,
            windows=groupby_node.parameters.windows,
            offset=groupby_node.parameters.offset,
            serving_names=groupby_node.parameters.serving_names,
            value_by_column=groupby_node.parameters.value_by,
            parent=groupby_node.parameters.parent,
            agg_func=groupby_node.parameters.agg_func,
        )
        return info


class TileGenMixin(BaseGraphInterpreter):
    """Interprets a given Query Graph and generates SQL for different purposes

    Parameters
    ----------
    query_graph : QueryGraphModel
        Query graph
    source_type : SourceType
        Data source type information
    """

    def construct_tile_gen_sql(self, starting_node: Node, is_on_demand: bool) -> list[TileGenSql]:
        """Construct a list of tile building SQLs for the given Query Graph

        Parameters
        ----------
        starting_node : Node
            Starting node (typically corresponding to selected features) to search from
        is_on_demand : bool
            Whether the SQL is for on-demand tile building for historical features

        Returns
        -------
        List[TileGenSql]
        """
        flat_starting_node = self.get_flattened_node(starting_node.name)
        generator = TileSQLGenerator(
            self.query_graph, is_on_demand=is_on_demand, source_type=self.source_type
        )
        return generator.construct_tile_gen_sql(flat_starting_node)
