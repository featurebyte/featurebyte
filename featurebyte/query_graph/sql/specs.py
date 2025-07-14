"""
Module for data structures that describe different types of aggregations that form features
"""

from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Type, TypeVar, cast

import pandas as pd
from bson import ObjectId
from sqlglot.expressions import Expression, Select

from featurebyte.enum import AggFunc, DBVarType, StrEnum
from featurebyte.models.column_statistics import ColumnStatisticsInfo
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    ForwardAggregateNode,
    ForwardAggregateParameters,
    GroupByNode,
    ItemGroupbyNode,
    ItemGroupbyParameters,
)
from featurebyte.query_graph.node.mixin import BaseGroupbyParameters
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import (
    EventTableTimestampFilter,
    PartitionColumnFilters,
    apply_serving_names_mapping,
)
from featurebyte.query_graph.sql.query_graph_util import get_parent_dtype
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.tiling import InputColumn, get_aggregator
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.transform.pruning import prune_query_graph

NonTileBasedAggregationSpecT = TypeVar(
    "NonTileBasedAggregationSpecT", bound="NonTileBasedAggregationSpec"
)

FB_INTERNAL_COLUMN_PREFIX = "_fb_internal"


class AggregationType(StrEnum):
    """
    Enum for different aggregation types. Will be used as the dictionary key in a container for
    aggregators held by FeatureExecutionPlan.
    """

    LATEST = "latest"
    LOOKUP = "lookup"
    LOOKUP_TARGET = "lookup_target"
    WINDOW = "window"
    ITEM = "item"
    AS_AT = "as_at"
    NON_TILE_WINDOW = "non_tile_window"
    FORWARD = "forward"
    FORWARD_AS_AT = "forward_as_at"
    TIME_SERIES = "time_series"


@dataclass
class AggregationSpec(ABC):
    """
    Base class of all aggregation specifications
    """

    node_name: str
    feature_name: str
    entity_ids: list[ObjectId] | None  # DEV-556: should not be None for new features
    serving_names: list[str]
    serving_names_mapping: Optional[dict[str, str]]
    agg_result_name_include_serving_names: bool

    def __post_init__(self) -> None:
        self.original_serving_names = self.serving_names[:]
        self.original_agg_result_name: str = self.agg_result_name
        if self.serving_names_mapping is not None:
            self.serving_names = apply_serving_names_mapping(
                self.serving_names, self.serving_names_mapping
            )

    @property
    @abstractmethod
    def agg_result_name(self) -> str:
        """
        Column name of the aggregated result

        Returns
        -------
        str
            Column names of the aggregated result
        """

    @property
    @abstractmethod
    def aggregation_type(self) -> AggregationType:
        """
        Aggregation type of this AggregationSpec

        Returns
        -------
        AggregationType
        """

    def construct_agg_result_name(self, *args: Any) -> str:
        """
        Helper function to construct the aggregation result name

        Parameters
        ----------
        *args: Any
            Tags to be included in the aggregation result name

        Returns
        -------
        str
            Aggregation result name
        """
        parts = [FB_INTERNAL_COLUMN_PREFIX]
        if self.agg_result_name_include_serving_names:
            parts.extend(self.serving_names)
        parts.append(self.aggregation_type)
        parts.extend([f"{arg}" for arg in args])
        return "_".join(parts)


@dataclass
class TileBasedAggregationSpec(AggregationSpec):
    """
    Window aggregation specification
    """

    window: int | None
    offset: int | None
    frequency: int
    blind_spot: int
    time_modulo_frequency: int
    tile_table_id: str
    aggregation_id: str
    keys: list[str]
    value_by: str | None
    merge_expr: Expression
    feature_name: str
    is_order_dependent: bool
    tile_value_columns: list[str]
    dtype: DBVarType
    pruned_graph: QueryGraphModel
    pruned_node: Node
    agg_func: AggFunc

    @property
    def agg_result_name(self) -> str:
        """Column name of the aggregated result

        Returns
        -------
        str
            Column names of the aggregated result
        """
        if self.window is not None:
            args = [f"w{self.window}", self.aggregation_id]
        else:
            # In this case, this is latest aggregation without time window. The aggregation_id would
            # have a "latest_" prefix already.
            args = [self.aggregation_id.replace("latest_", "")]
        if self.offset is not None:
            args.append(f"o{self.offset}")
        return self.construct_agg_result_name(*args)

    @property
    def aggregation_type(self) -> AggregationType:
        if self.window is None:
            return AggregationType.LATEST
        return AggregationType.WINDOW

    @classmethod
    def from_groupby_query_node(
        cls,
        graph: QueryGraphModel,
        groupby_node: Node,
        adapter: BaseAdapter,
        agg_result_name_include_serving_names: bool,
        serving_names_mapping: dict[str, str] | None = None,
        on_demand_tile_tables_mapping: dict[str, str] | None = None,
    ) -> list[TileBasedAggregationSpec]:
        """Construct an AggregationSpec from a query graph and groupby node

        Parameters
        ----------
        graph : QueryGraphModel
            Query graph
        groupby_node : Node
            Query graph node with groupby type
        adapter : BaseAdapter
            Instance of BaseAdapter
        serving_names_mapping : dict[str, str]
            Mapping from original serving name to new serving name
        agg_result_name_include_serving_names: bool
            Whether to include serving names in the aggregation result names
        on_demand_tile_tables_mapping: dict[str, str]
            Optional mapping from tile table id to on-demand tile table name

        Returns
        -------
        list[TileBasedAggregationSpec]
            List of AggregationSpec
        """

        assert isinstance(groupby_node, GroupByNode)
        groupby_node_params = groupby_node.parameters
        tile_table_id = groupby_node_params.tile_id
        aggregation_id = groupby_node.parameters.aggregation_id
        assert tile_table_id is not None
        assert aggregation_id is not None

        # When tile tables are generated as temporary on-demand tables, the aggregation specs use
        # the on-demand table name instead of the tile_id in the groupby node parameters.
        if on_demand_tile_tables_mapping is not None:
            tile_table_id = on_demand_tile_tables_mapping[tile_table_id]

        aggregation_specs = []
        parent_dtype = None
        parent_column = None
        parent_column_name = groupby_node_params.parent
        if parent_column_name:
            parent_dtype = get_parent_dtype(parent_column_name, graph, query_node=groupby_node)
            parent_column = InputColumn(name=parent_column_name, dtype=parent_dtype)
        aggregator = get_aggregator(
            groupby_node_params.agg_func, adapter=adapter, parent_dtype=parent_dtype
        )
        tile_value_columns = [
            spec.tile_column_name for spec in aggregator.tile(parent_column, aggregation_id)
        ]
        if groupby_node_params.offset is not None:
            offset_secs = int(pd.Timedelta(groupby_node_params.offset).total_seconds())
        else:
            offset_secs = None
        for window, feature_name in zip(groupby_node_params.windows, groupby_node_params.names):
            window_secs = int(pd.Timedelta(window).total_seconds()) if window is not None else None
            pruned_graph, pruned_node, dtype = cls._get_aggregation_column_type(
                graph=graph,
                groupby_node=groupby_node,
                feature_name=feature_name,
            )
            fjs = groupby_node_params.feature_job_setting
            agg_spec = cls(
                node_name=groupby_node.name,
                feature_name=feature_name,
                window=window_secs,
                frequency=fjs.period_seconds,
                time_modulo_frequency=fjs.offset_seconds,
                blind_spot=fjs.blind_spot_seconds,
                offset=offset_secs,
                tile_table_id=tile_table_id,
                aggregation_id=aggregation_id,
                keys=groupby_node_params.keys,  # type: ignore[arg-type]
                serving_names=groupby_node_params.serving_names,
                serving_names_mapping=serving_names_mapping,
                value_by=groupby_node_params.value_by,
                merge_expr=aggregator.merge(aggregation_id),
                is_order_dependent=aggregator.is_order_dependent,
                tile_value_columns=tile_value_columns,
                entity_ids=groupby_node_params.entity_ids,
                dtype=dtype,
                pruned_graph=pruned_graph,
                pruned_node=pruned_node,
                agg_func=groupby_node_params.agg_func,
                agg_result_name_include_serving_names=agg_result_name_include_serving_names,
            )
            aggregation_specs.append(agg_spec)

        return aggregation_specs

    @classmethod
    def _get_aggregation_column_type(
        cls,
        graph: QueryGraphModel,
        groupby_node: Node,
        feature_name: str,
    ) -> Tuple[QueryGraphModel, Node, DBVarType]:
        """Get the column type of the aggregation

        Parameters
        ----------
        graph : QueryGraphModel
            Query graph
        groupby_node : Node
            Groupby node
        feature_name : str
            Feature name of interest. Should be one of the features generated by the groupby node.

        Returns
        -------
        DBVarType
        """
        project_node = graph.add_operation(
            NodeType.PROJECT,
            node_params={"columns": [feature_name]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[groupby_node],
        )
        pruned_graph, node_name_map, _ = prune_query_graph(graph=graph, node=project_node)
        pruned_node = pruned_graph.get_node_by_name(node_name_map[project_node.name])
        op_struct = (
            OperationStructureExtractor(graph=pruned_graph)
            .extract(node=pruned_node)
            .operation_structure_map[pruned_node.name]
        )
        aggregations = op_struct.aggregations
        assert (
            len(aggregations) == 1
        ), f"Expect exactly one aggregation but got: {[agg.name for agg in aggregations]}"
        aggregation = aggregations[0]
        return pruned_graph, pruned_node, aggregation.dtype


@dataclass
class AggregationSource:
    """
    Represents the source of an aggregation. The aggregation is to be done via lookup,
    aggregate_asat, etc.
    """

    expr: Select
    query_node_name: str
    is_scd_filtered_by_current_flag: Optional[bool] = None


@dataclass
class NonTileBasedAggregationSpec(AggregationSpec):
    """
    Represents an aggregation that is performed directly on the source without tile based
    pre-aggregation
    """

    aggregation_source: AggregationSource

    @classmethod
    def get_aggregation_source(
        cls,
        graph: QueryGraphModel,
        node: Node,
        source_info: SourceInfo,
        to_filter_scd_by_current_flag: bool,
        event_table_timestamp_filter: Optional[EventTableTimestampFilter],
        partition_column_filters: Optional[PartitionColumnFilters],
    ) -> AggregationSource:
        """
        Get the expression of the input view to be aggregated

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph
        node: Node
            Query graph node
        source_info: SourceInfo
            Source information
        to_filter_scd_by_current_flag: bool
            Whether to filter SCD by current flag
        event_table_timestamp_filter: EventTableTimestampFilter
            Event table timestamp filter to apply if applicable
        partition_column_filters: Optional[PartitionColumnFilters]
            Partition column filters to apply if applicable

        Returns
        -------
        AggregationSource
            An AggregationSource object representing the source table
        """

        from featurebyte.query_graph.sql.ast.aggregate import Aggregate
        from featurebyte.query_graph.sql.builder import SQLOperationGraph
        from featurebyte.query_graph.sql.common import SQLType

        sql_node = SQLOperationGraph(
            graph,
            sql_type=SQLType.AGGREGATION,
            source_info=source_info,
            to_filter_scd_by_current_flag=to_filter_scd_by_current_flag,
            event_table_timestamp_filter=event_table_timestamp_filter,
            partition_column_filters=partition_column_filters,
        ).build(node)

        sql_node = cast(Aggregate, sql_node)
        return sql_node.to_aggregation_source()  # type: ignore

    def construct_agg_result_name(self, *args: Any) -> str:
        return super().construct_agg_result_name(*args, self.aggregation_source.query_node_name)

    def get_agg_result_name_from_groupby_parameters(
        self,
        parameters: BaseGroupbyParameters,
        *args: Any,
    ) -> str:
        """
        Get the name of the aggregation result column from groupby parameters. The name should
        include the parameters that can affect the result of the groupby operation: aggregation
        function, parent variable, groupby keys and category key (optional).

        Parameters
        ----------
        parameters: BaseGroupbyParameters
            Groupby parameters
        *args: Any
            Any additional parameters to include in the result name

        Returns
        -------
        str
        """
        groupby_args = [
            parameters.agg_func,
            parameters.parent,
            *(parameters.keys or [None]),  # type: ignore
            parameters.value_by or None,
        ]
        return self.construct_agg_result_name(*groupby_args, *args)

    @classmethod
    def should_filter_scd_by_current_flag(cls, graph: QueryGraphModel, node: Node) -> bool:
        """
        Whether the SCD table should be filtered by current flag during online serving

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph
        node: Node
            Query graph node

        Returns
        -------
        bool
        """
        _ = graph
        _ = node
        return False

    @property
    def source_expr(self) -> Select:
        """
        Get the expression of the input view to be aggregated

        Returns
        -------
        Select
        """
        return self.aggregation_source.expr

    @property
    def source_hash(self) -> str:
        """
        Get a hash that uniquely identifies the source an aggregation (for the purpose of grouping
        aggregations that can be performed in the same subquery)

        Returns
        -------
        str
        """
        hasher = hashlib.shake_128()
        params = self.get_source_hash_parameters()
        hasher.update(json.dumps(params, sort_keys=True).encode("utf-8"))
        return hasher.hexdigest(8)

    @abstractmethod
    def get_source_hash_parameters(self) -> dict[str, Any]:
        """
        Get parameters that uniquely identifies the source of an aggregation (for the purpose of
        grouping aggregations that can be performed in the same subquery)

        Returns
        -------
        dict[str, Any]
        """

    @classmethod
    @abstractmethod
    def construct_specs(
        cls: Type[NonTileBasedAggregationSpecT],
        node: Node,
        aggregation_source: AggregationSource,
        serving_names_mapping: Optional[dict[str, str]],
        graph: Optional[QueryGraphModel],
        agg_result_name_include_serving_names: bool,
        column_statistics_info: Optional[ColumnStatisticsInfo],
    ) -> list[NonTileBasedAggregationSpecT]:
        """
        Construct the list of specifications

        Parameters
        ----------
        node: Node
            Query graph node
        aggregation_source: AggregationSource
            Source of the aggregation
        serving_names_mapping: Optional[dict[str, str]]
            Serving names mapping
        graph: Optional[QueryGraphModel]
            Query graph
        agg_result_name_include_serving_names: bool
            Whether to include serving names in the aggregation result names
        column_statistics_info: Optional[ColumnStatisticsInfo]
            Column statistics information
        """

    @classmethod
    def get_parent_dtype_from_graph(
        cls, graph: Optional[QueryGraphModel], parent: Optional[str], node: Node
    ) -> Optional[DBVarType]:
        """
        Helper method to get the dtype of the parent variable

        Parameters
        ----------
        graph: Optional[QueryGraphModel]
            Query graph
        parent: Optional[str]
            Parent variable name
        node: Node
            Query graph node

        Returns
        -------
        Optional[DBVarType]
        """
        if not parent:
            return None
        assert graph is not None
        return get_parent_dtype(parent, graph, node)

    @classmethod
    def from_query_graph_node(
        cls: Type[NonTileBasedAggregationSpecT],
        node: Node,
        graph: QueryGraphModel,
        agg_result_name_include_serving_names: bool = True,
        aggregation_source: Optional[AggregationSource] = None,
        source_info: Optional[SourceInfo] = None,
        serving_names_mapping: Optional[dict[str, str]] = None,
        is_online_serving: Optional[bool] = None,
        event_table_timestamp_filter: Optional[EventTableTimestampFilter] = None,
        column_statistics_info: Optional[ColumnStatisticsInfo] = None,
        partition_column_filters: Optional[PartitionColumnFilters] = None,
    ) -> list[NonTileBasedAggregationSpecT]:
        """Construct NonTileBasedAggregationSpec objects given a query graph node

        Parameters
        ----------
        node : Node
            Query graph node
        graph: QueryGraphModel
            Query graph. Mandatory if aggregation_source is not provided
        agg_result_name_include_serving_names: bool
            Whether to include serving names in the aggregation result names
        aggregation_source: Optional[AggregationSource]
            Source of the aggregation
        source_info: Optional[SourceInfo]
            Source type information. Mandatory if aggregation_source is not provided
        serving_names_mapping: Optional[dict[str, str]]
            Serving names mapping
        is_online_serving: bool
            Whether the query is for online serving
        event_table_timestamp_filter: Optional[EventTableTimestampFilter]
            Event table timestamp filter to apply if applicable
        column_statistics_info: Optional[ColumnStatisticsInfo]
            Column statistics information
        partition_column_filters: Optional[PartitionColumnFilters]
            Partition column filters to apply if applicable

        Returns
        -------
        NonTileBasedAggregationSpecT
        """
        if aggregation_source is None:
            assert graph is not None
            assert source_info is not None
            to_filter_scd_by_current_flag = (
                is_online_serving is True
                and cls.should_filter_scd_by_current_flag(graph=graph, node=node)
            )
            aggregation_source = cls.get_aggregation_source(
                graph=graph,
                node=node,
                source_info=source_info,
                to_filter_scd_by_current_flag=to_filter_scd_by_current_flag,
                event_table_timestamp_filter=event_table_timestamp_filter,
                partition_column_filters=partition_column_filters,
            )

        return cls.construct_specs(
            node=node,
            aggregation_source=aggregation_source,
            serving_names_mapping=serving_names_mapping,
            graph=graph,
            agg_result_name_include_serving_names=agg_result_name_include_serving_names,
            column_statistics_info=column_statistics_info,
        )


@dataclass
class ItemAggregationSpec(NonTileBasedAggregationSpec):
    """
    Non-time aware aggregation specification
    """

    parameters: ItemGroupbyParameters
    parent_dtype: Optional[DBVarType]

    @property
    def agg_result_name(self) -> str:
        """Column name of the aggregated result

        Returns
        -------
        str
            Column name of the aggregated result
        """
        return self.get_agg_result_name_from_groupby_parameters(self.parameters)

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.ITEM

    def get_source_hash_parameters(self) -> dict[str, Any]:
        params: dict[str, Any] = {"source_expr": self.source_expr.sql()}
        if self.parameters.value_by is None:
            parameters_dict = self.parameters.model_dump(exclude={"parent", "agg_func", "name"})
        else:
            parameters_dict = self.parameters.model_dump(exclude={"name"})
        if parameters_dict.get("entity_ids") is not None:
            parameters_dict["entity_ids"] = [
                str(entity_id) for entity_id in parameters_dict["entity_ids"]
            ]
        params["parameters"] = parameters_dict
        return params

    @classmethod
    def construct_specs(
        cls,
        node: Node,
        aggregation_source: AggregationSource,
        serving_names_mapping: Optional[dict[str, str]],
        graph: Optional[QueryGraphModel],
        agg_result_name_include_serving_names: bool,
        column_statistics_info: Optional[ColumnStatisticsInfo],
    ) -> list[ItemAggregationSpec]:
        assert isinstance(node, ItemGroupbyNode)
        return [
            ItemAggregationSpec(
                node_name=node.name,
                feature_name=node.parameters.name,
                entity_ids=cast(List[ObjectId], node.parameters.entity_ids),
                serving_names=node.parameters.serving_names,
                serving_names_mapping=serving_names_mapping,
                parameters=node.parameters,
                aggregation_source=aggregation_source,
                parent_dtype=cls.get_parent_dtype_from_graph(graph, node.parameters.parent, node),
                agg_result_name_include_serving_names=agg_result_name_include_serving_names,
            )
        ]


@dataclass
class ForwardAggregateSpec(NonTileBasedAggregationSpec):
    """
    ForwardAggregateSpec contains all information required to generate sql for a forward aggregate target.
    """

    parameters: ForwardAggregateParameters
    parent_dtype: Optional[DBVarType]

    @property
    def agg_result_name(self) -> str:
        return self.get_agg_result_name_from_groupby_parameters(self.parameters)

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.FORWARD

    def get_source_hash_parameters(self) -> dict[str, Any]:
        params: dict[str, Any] = {"source_expr": self.source_expr.sql()}
        if self.parameters.value_by is None:
            parameters_dict = self.parameters.model_dump(exclude={"parent", "agg_func", "name"})
        else:
            parameters_dict = self.parameters.model_dump(exclude={"name"})
        if parameters_dict.get("entity_ids") is not None:
            parameters_dict["entity_ids"] = [
                str(entity_id) for entity_id in parameters_dict["entity_ids"]
            ]
        if parameters_dict.get("timestamp_metadata") is None:
            parameters_dict.pop("timestamp_metadata", None)
        params["parameters"] = parameters_dict
        return params

    @classmethod
    def construct_specs(
        cls: Type[NonTileBasedAggregationSpecT],
        node: Node,
        aggregation_source: AggregationSource,
        serving_names_mapping: Optional[dict[str, str]],
        graph: Optional[QueryGraphModel],
        agg_result_name_include_serving_names: bool,
        column_statistics_info: Optional[ColumnStatisticsInfo],
    ) -> list[ForwardAggregateSpec]:
        assert isinstance(node, ForwardAggregateNode)
        return [
            ForwardAggregateSpec(
                node_name=node.name,
                feature_name=node.parameters.name,
                parameters=node.parameters,
                aggregation_source=aggregation_source,
                entity_ids=cast(List[ObjectId], node.parameters.entity_ids),
                serving_names=node.parameters.serving_names,
                serving_names_mapping=serving_names_mapping,
                parent_dtype=cls.get_parent_dtype_from_graph(graph, node.parameters.parent, node),
                agg_result_name_include_serving_names=agg_result_name_include_serving_names,
            )
        ]


@dataclass
class FeatureSpec:
    """
    Feature specification
    """

    feature_name: str
    feature_expr: Expression
    feature_dtype: Optional[DBVarType]
