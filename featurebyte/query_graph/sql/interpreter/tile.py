"""
This module contains the Query Graph Interpreter
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, Set, Tuple, cast

from bson import ObjectId

from featurebyte.models.tile_compute_query import TileComputeQuery
from featurebyte.query_graph.algorithm import dfs_traversal
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupByNode, JoinNodeParameters
from featurebyte.query_graph.node.metadata.operation import OperationStructure, SourceDataColumn
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import (
    EventTableTimestampFilter,
    OnDemandEntityFilters,
    PartitionColumnFilters,
    SQLType,
    sql_to_string,
)
from featurebyte.query_graph.sql.interpreter.base import BaseGraphInterpreter
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.tile_compute_spec import TileComputeSpec
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

    tile_table_id: str
    tile_id_version: int
    aggregation_id: str
    tile_compute_spec: TileComputeSpec
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
    def tile_compute_query(self) -> TileComputeQuery:
        """
        Get a TileComputeQuery object from the tile compute spec

        Returns
        -------
        TileComputeQuery
        """
        return self.tile_compute_spec.get_tile_compute_query()

    @property
    def sql(self) -> str:
        """
        Templated SQL code for building tiles

        Returns
        -------
        str
        """
        tile_compute_query = self.tile_compute_spec.get_tile_compute_query()
        return sql_to_string(
            tile_compute_query.get_combined_query_expr(),
            tile_compute_query.aggregation_query.source_type,
        )


JoinKeysLineageKey = Tuple[ObjectId, str]


class JoinKeysLineage:
    """Helper to keep track of join keys lineage"""

    def __init__(self) -> None:
        self.edges: dict[JoinKeysLineageKey, list[JoinKeysLineageKey]] = defaultdict(list)

    def add_join_key(
        self,
        left_table_id: ObjectId,
        left_column_name: str,
        right_table_id: ObjectId,
        right_column_name: str,
    ) -> None:
        """
        Add a join key corresponding to a join node

        Parameters
        ----------
        left_table_id: ObjectId
            Id of the left table
        left_column_name: str
            Join key of the left table
        right_table_id: ObjectId
            Id of the right table
        right_column_name: str
            Join key of the right table
        """
        self.edges[(left_table_id, left_column_name)].append((right_table_id, right_column_name))

    def get_other_columns(self, table_id: ObjectId, column_name: str) -> list[JoinKeysLineageKey]:
        """
        Get other columns that have been joined with the provided join key

        Parameters
        ----------
        table_id: ObjectId
            Table id of the join key
        column_name: str
            Join key column

        Returns
        -------
        list[JoinKeysLineageKey]
        """

        def _traverse(cur_table_id: ObjectId, cur_column_name: str) -> list[JoinKeysLineageKey]:
            key = (cur_table_id, cur_column_name)
            if key not in self.edges:
                return []
            out = self.edges[key][:]
            for next_table_id, next_column_name in self.edges[key]:
                out.extend(_traverse(next_table_id, next_column_name))
            return out

        return _traverse(table_id, column_name)


@dataclass
class InputFilterContext:
    """
    Information that determines whether filters on input data can be applied
    """

    node_types: Set[NodeType]
    operation_structure: OperationStructure
    join_keys_lineage: JoinKeysLineage

    @property
    def has_lag_operation(self) -> bool:
        """
        Returns whether any lag operation is applied

        Returns
        -------
        bool
        """
        return NodeType.LAG in self.node_types


class TileSQLGenerator:
    """Generator for Tile-building SQL

    Parameters
    ----------
    query_graph : QueryGraphModel
    """

    def __init__(self, query_graph: QueryGraphModel, is_on_demand: bool, source_info: SourceInfo):
        self.query_graph = query_graph
        self.is_on_demand = is_on_demand
        self.source_info = source_info

    def construct_tile_gen_sql(
        self, starting_node: Node, partition_column_filters: Optional[PartitionColumnFilters]
    ) -> list[TileGenSql]:
        """Construct a list of tile building SQLs for the given Query Graph

        There can be more than one tile table to build if the feature depends on more than one
        groupby operations. However, before we support complex features, there will only be one tile
        table to build.

        Parameters
        ----------
        starting_node : Node
            Starting node (typically corresponding to selected features) to search from
        partition_column_filters : Optional[PartitionColumnFilters]
            Optional partition column filters to apply to the tile building SQLs

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
                on_demand_entity_filters = self._get_on_demand_entity_filters(groupby_node)
            else:
                event_table_timestamp_filter = self._get_event_table_timestamp_filter(groupby_node)
                on_demand_entity_filters = None

            info = self.make_one_tile_sql(
                groupby_node,
                event_table_timestamp_filter,
                on_demand_entity_filters,
                partition_column_filters=partition_column_filters,
            )
            sqls.append(info)

        return sqls

    def _get_input_filter_context(self, groupby_node: GroupByNode) -> InputFilterContext:
        pruned_graph, node_name_map, _ = prune_query_graph(
            graph=self.query_graph, node=groupby_node
        )

        # Get node_types lineage
        pruned_node = pruned_graph.get_node_by_name(node_name_map[groupby_node.name])
        node_types = set()
        for node in dfs_traversal(pruned_graph, pruned_node):
            node_types.add(node.type)

        # Get operation structure of the view before tile aggregation
        groupby_input_node = pruned_graph.get_node_by_name(
            pruned_graph.get_input_node_names(pruned_node)[0]
        )
        op_struct_info = OperationStructureExtractor(graph=pruned_graph).extract(groupby_input_node)
        op_struct = op_struct_info.operation_structure_map[groupby_input_node.name]

        # Identify join keys lineage to propagate filtering to applicable tables
        def _get_join_key_from_op_struct(
            op_struct_obj: OperationStructure, column_name: str
        ) -> Optional[SourceDataColumn]:
            for column in op_struct_obj.columns:
                if (
                    column.name == column_name
                    and isinstance(column, SourceDataColumn)
                    and column.table_id is not None
                ):
                    return column  # type: ignore[return-value]
            return None

        join_keys_lineage = JoinKeysLineage()
        for join_node in pruned_graph.iterate_nodes(pruned_node, NodeType.JOIN):
            input_nodes = pruned_graph.get_input_node_names(join_node)
            assert len(input_nodes) == 2
            params = cast(JoinNodeParameters, join_node.parameters)
            join_key_left = _get_join_key_from_op_struct(
                op_struct_info.operation_structure_map[input_nodes[0]], params.left_on
            )
            join_key_right = _get_join_key_from_op_struct(
                op_struct_info.operation_structure_map[input_nodes[1]], params.right_on
            )
            if join_key_left is not None and join_key_right is not None:
                join_keys_lineage.add_join_key(
                    left_table_id=cast(ObjectId, join_key_left.table_id),
                    left_column_name=params.left_on,
                    right_table_id=cast(ObjectId, join_key_right.table_id),
                    right_column_name=params.right_on,
                )

        return InputFilterContext(
            node_types=node_types,
            operation_structure=op_struct,
            join_keys_lineage=join_keys_lineage,
        )

    def _get_event_table_timestamp_filter(
        self, groupby_node: GroupByNode
    ) -> Optional[EventTableTimestampFilter]:
        # Check there is no lag operation, otherwise cannot apply timestamp filter directly
        input_filter_context = self._get_input_filter_context(groupby_node)
        if input_filter_context.has_lag_operation:
            return None

        # Identify which EventTable can be filtered. This is based on the timestamp column used in
        # the groupby node.
        for column in input_filter_context.operation_structure.columns:
            if (
                column.name == groupby_node.parameters.timestamp
                and isinstance(column, SourceDataColumn)
                and column.table_id is not None
            ):
                assert isinstance(column, SourceDataColumn), "SourceDataColumn expected"
                timestamp_dtype_metadata = (
                    input_filter_context.operation_structure.get_dtype_metadata(column.name)
                )
                return EventTableTimestampFilter(
                    timestamp_column_name=column.name,
                    timestamp_schema=timestamp_dtype_metadata.timestamp_schema
                    if timestamp_dtype_metadata
                    else None,
                    event_table_id=column.table_id,
                )

        return None

    def _get_on_demand_entity_filters(
        self, groupby_node: GroupByNode
    ) -> Optional[OnDemandEntityFilters]:
        input_filter_context = self._get_input_filter_context(groupby_node)

        # Filter cannot be applied when there are lag operations
        if input_filter_context.has_lag_operation:
            return None

        # No additional filtering on simple views
        complex_view_node_types = {NodeType.JOIN, NodeType.TRACK_CHANGES}
        if not complex_view_node_types.intersection(input_filter_context.node_types):
            return None

        # Determine input nodes that can be filtered
        op_struct, join_keys_lineage = (
            input_filter_context.operation_structure,
            input_filter_context.join_keys_lineage,
        )
        entity_columns = set(groupby_node.parameters.keys)
        on_demand_entity_filters = OnDemandEntityFilters(entity_columns=list(entity_columns))
        for column in op_struct.columns:
            if (
                column.name in entity_columns
                and isinstance(column, SourceDataColumn)
                and column.table_id is not None
            ):
                assert isinstance(column, SourceDataColumn), "SourceDataColumn expected"
                on_demand_entity_filters.add_entity_column(
                    table_id=column.table_id,
                    entity_column_name=column.name,
                    table_column_name=column.name,
                )
                for other_column in join_keys_lineage.get_other_columns(
                    column.table_id, column.name
                ):
                    on_demand_entity_filters.add_entity_column(
                        table_id=other_column[0],
                        entity_column_name=column.name,
                        table_column_name=other_column[1],
                    )
        return on_demand_entity_filters

    def make_one_tile_sql(
        self,
        groupby_node: GroupByNode,
        event_table_timestamp_filter: Optional[EventTableTimestampFilter],
        on_demand_entity_filters: Optional[OnDemandEntityFilters],
        partition_column_filters: Optional[PartitionColumnFilters],
    ) -> TileGenSql:
        """Construct tile building SQL for a specific groupby query graph node

        Parameters
        ----------
        groupby_node: GroupByNode
            Groupby query graph node
        event_table_timestamp_filter: Optional[EventTableTimestampFilter]
            Event table timestamp filter to apply if applicable
        on_demand_entity_filters: Optional[OnDemandEntityFilters]
            On demand entity filters to apply if applicable
        partition_column_filters: Optional[PartitionColumnFilters]
            Partition column filters to apply if applicable

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
            source_info=self.source_info,
            event_table_timestamp_filter=event_table_timestamp_filter,
            on_demand_entity_filters=on_demand_entity_filters,
            partition_column_filters=partition_column_filters,
        ).build(groupby_node)
        tile_table_id = groupby_node.parameters.tile_id
        aggregation_id = groupby_node.parameters.aggregation_id
        entity_columns = groupby_sql_node.keys
        tile_value_columns = [spec.tile_column_name for spec in groupby_sql_node.tile_specs]
        tile_value_types = [spec.tile_column_type for spec in groupby_sql_node.tile_specs]
        assert tile_table_id is not None, "Tile table ID is required"
        assert aggregation_id is not None, "Aggregation ID is required"
        fjs = groupby_node.parameters.feature_job_setting
        info = TileGenSql(
            tile_table_id=tile_table_id,
            tile_id_version=groupby_node.parameters.tile_id_version,
            aggregation_id=aggregation_id,
            tile_compute_spec=groupby_sql_node.get_tile_compute_spec(),
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
    source_info: SourceInfo
        Data source type information
    """

    def construct_tile_gen_sql(
        self,
        starting_node: Node,
        is_on_demand: bool,
        partition_column_filters: Optional[PartitionColumnFilters] = None,
    ) -> list[TileGenSql]:
        """Construct a list of tile building SQLs for the given Query Graph

        Parameters
        ----------
        starting_node : Node
            Starting node (typically corresponding to selected features) to search from
        is_on_demand : bool
            Whether the SQL is for on-demand tile building for historical features
        partition_column_filters : Optional[PartitionColumnFilters]
            Optional partition column filters to apply to the tile building SQLs

        Returns
        -------
        List[TileGenSql]
        """
        flat_starting_node = self.get_flattened_node(starting_node.name)
        generator = TileSQLGenerator(
            self.query_graph, is_on_demand=is_on_demand, source_info=self.source_info
        )
        return generator.construct_tile_gen_sql(
            flat_starting_node, partition_column_filters=partition_column_filters
        )
