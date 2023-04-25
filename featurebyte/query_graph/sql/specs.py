"""
Module for data structures that describe different types of aggregations that form features
"""
from __future__ import annotations

from typing import Any, List, Optional, Type, TypeVar, cast

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd
from bson import ObjectId
from sqlglot.expressions import Expression, Select

from featurebyte.enum import DBVarType, SourceType, StrEnum
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    AggregateAsAtNode,
    AggregateAsAtParameters,
    EventLookupParameters,
    GroupByNode,
    ItemGroupbyNode,
    ItemGroupbyParameters,
    LookupNode,
    SCDLookupParameters,
)
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import apply_serving_names_mapping
from featurebyte.query_graph.sql.tiling import InputColumn, get_aggregator

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
    WINDOW = "window"
    ITEM = "item"
    AS_AT = "as_at"


@dataclass  # type: ignore[misc]
class AggregationSpec(ABC):
    """
    Base class of all aggregation specifications
    """

    entity_ids: list[ObjectId] | None  # DEV-556: should not be None for new features
    serving_names: list[str]
    serving_names_mapping: Optional[dict[str, str]]

    def __post_init__(self) -> None:
        self.original_serving_names = self.serving_names[:]
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
        parts = [FB_INTERNAL_COLUMN_PREFIX, self.aggregation_type]
        parts.extend([f"{arg}" for arg in args])
        return "_".join(parts)


@dataclass
class TileBasedAggregationSpec(AggregationSpec):
    """
    Window aggregation specification
    """

    # pylint: disable=too-many-instance-attributes

    window: int | None
    frequency: int
    blind_spot: int
    time_modulo_frequency: int
    tile_table_id: str
    aggregation_id: str
    keys: list[str]
    value_by: str | None
    merge_expr: str
    feature_name: str
    is_order_dependent: bool
    tile_value_columns: list[str]

    @property
    def agg_result_name(self) -> str:
        """Column name of the aggregated result

        Returns
        -------
        str
            Column names of the aggregated result
        """
        if self.window is None:
            # In this case, this is latest aggregation without time window. The aggregation_id would
            # have a "latest_" prefix already.
            return self.construct_agg_result_name(self.aggregation_id.replace("latest_", ""))
        return self.construct_agg_result_name(f"w{self.window}", self.aggregation_id)

    @property
    def aggregation_type(self) -> AggregationType:
        if self.window is None:
            return AggregationType.LATEST
        return AggregationType.WINDOW

    @classmethod
    def from_groupby_query_node(
        cls,
        groupby_node: Node,
        adapter: BaseAdapter,
        serving_names_mapping: dict[str, str] | None = None,
    ) -> list[TileBasedAggregationSpec]:
        """Construct an AggregationSpec from a query graph and groupby node

        Parameters
        ----------
        groupby_node : Node
            Query graph node with groupby type
        adapter : BaseAdapter
            Instance of BaseAdapter
        serving_names_mapping : dict[str, str]
            Mapping from original serving name to new serving name

        Returns
        -------
        list[TileBasedAggregationSpec]
            List of AggregationSpec
        """
        assert isinstance(groupby_node, GroupByNode)
        tile_table_id = groupby_node.parameters.tile_id
        aggregation_id = groupby_node.parameters.aggregation_id
        params = groupby_node.parameters.dict()
        assert tile_table_id is not None
        assert aggregation_id is not None

        serving_names = params["serving_names"]
        aggregation_specs = []
        aggregator = get_aggregator(params["agg_func"], adapter=adapter)
        if params["parent"]:
            # Note: here, we only need to retrive tile column names. Ideally the dtype should be set
            # as the parent column's dtype, but in this case a dummy dtype is passed since that
            # doesn't affect the tile column names.
            parent_column = InputColumn(name=params["parent"], dtype=DBVarType.FLOAT)
        else:
            parent_column = None
        tile_value_columns = [
            spec.tile_column_name
            for spec in aggregator.tile(parent_column, params["aggregation_id"])
        ]
        for window, feature_name in zip(params["windows"], params["names"]):
            params = groupby_node.parameters.dict()
            if window is not None:
                window = int(pd.Timedelta(window).total_seconds())
            agg_spec = cls(
                window=window,
                frequency=params["frequency"],
                time_modulo_frequency=params["time_modulo_frequency"],
                blind_spot=params["blind_spot"],
                tile_table_id=tile_table_id,
                aggregation_id=aggregation_id,
                keys=params["keys"],
                serving_names=serving_names,
                serving_names_mapping=serving_names_mapping,
                value_by=params["value_by"],
                merge_expr=aggregator.merge(aggregation_id),
                feature_name=feature_name,
                is_order_dependent=aggregator.is_order_dependent,
                tile_value_columns=tile_value_columns,
                entity_ids=params["entity_ids"],
            )
            aggregation_specs.append(agg_spec)

        return aggregation_specs


@dataclass
class AggregationSource:
    """
    Represents the source of an aggregation. The aggregation is to be done via lookup,
    aggregate_asat, etc.
    """

    expr: Select
    query_node_name: str
    is_scd_filtered_by_current_flag: Optional[bool] = None


@dataclass  # type: ignore[misc]
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
        source_type: SourceType,
        to_filter_scd_by_current_flag: bool,
    ) -> AggregationSource:
        """
        Get the expression of the input view to be aggregated

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph
        node: Node
            Query graph node
        source_type: SourceType
            Source type information
        to_filter_scd_by_current_flag: bool
            Whether to filter SCD by current flag

        Returns
        -------
        AggregationSource
            An AggregationSource object representing the source table
        """
        # pylint: disable=import-outside-toplevel,cyclic-import
        from featurebyte.query_graph.sql.ast.aggregate import Aggregate
        from featurebyte.query_graph.sql.builder import SQLOperationGraph
        from featurebyte.query_graph.sql.common import SQLType

        sql_node = SQLOperationGraph(
            graph,
            sql_type=SQLType.AGGREGATION,
            source_type=source_type,
            to_filter_scd_by_current_flag=to_filter_scd_by_current_flag,
        ).build(node)

        sql_node = cast(Aggregate, sql_node)
        return sql_node.to_aggregation_source()  # type: ignore

    def construct_agg_result_name(self, *args: Any) -> str:
        return super().construct_agg_result_name(*args, self.aggregation_source.query_node_name)

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
        """

    @classmethod
    def from_query_graph_node(
        cls: Type[NonTileBasedAggregationSpecT],
        node: Node,
        aggregation_source: Optional[AggregationSource] = None,
        graph: Optional[QueryGraphModel] = None,
        source_type: Optional[SourceType] = None,
        serving_names_mapping: Optional[dict[str, str]] = None,
        is_online_serving: Optional[bool] = None,
    ) -> list[NonTileBasedAggregationSpecT]:
        """Construct NonTileBasedAggregationSpec objects given a query graph node

        Parameters
        ----------
        node : Node
            Query graph node
        aggregation_source: Optional[AggregationSource]
            Source of the aggregation
        graph: Optional[QueryGraphModel]
            Query graph. Mandatory if aggregation_source is not provided
        source_type: Optional[SourceType]
            Source type information. Mandatory if aggregation_source is not provided
        serving_names_mapping: Optional[dict[str, str]]
            Serving names mapping
        is_online_serving: bool
            Whether the query is for online serving

        Returns
        -------
        NonTileBasedAggregationSpecT
        """
        if aggregation_source is None:
            assert graph is not None
            assert source_type is not None
            to_filter_scd_by_current_flag = (
                is_online_serving is True
                and cls.should_filter_scd_by_current_flag(graph=graph, node=node)
            )
            aggregation_source = cls.get_aggregation_source(
                graph=graph,
                node=node,
                source_type=source_type,
                to_filter_scd_by_current_flag=to_filter_scd_by_current_flag,
            )

        return cls.construct_specs(
            node=node,
            aggregation_source=aggregation_source,
            serving_names_mapping=serving_names_mapping,
        )


@dataclass
class ItemAggregationSpec(NonTileBasedAggregationSpec):
    """
    Non-time aware aggregation specification
    """

    parameters: ItemGroupbyParameters

    @property
    def agg_result_name(self) -> str:
        """Column name of the aggregated result

        Returns
        -------
        str
            Column name of the aggregated result
        """
        return self.construct_agg_result_name(self.parameters.agg_func, self.parameters.parent)

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.ITEM

    def get_source_hash_parameters(self) -> dict[str, Any]:
        params: dict[str, Any] = {"source_expr": self.source_expr.sql()}
        parameters_dict = self.parameters.dict(exclude={"parent", "agg_func", "name"})
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
    ) -> list[ItemAggregationSpec]:
        assert isinstance(node, ItemGroupbyNode)
        return [
            ItemAggregationSpec(
                entity_ids=cast(List[ObjectId], node.parameters.entity_ids),
                serving_names=node.parameters.serving_names,
                serving_names_mapping=serving_names_mapping,
                parameters=node.parameters,
                aggregation_source=aggregation_source,
            )
        ]


@dataclass
class AggregateAsAtSpec(NonTileBasedAggregationSpec):
    """
    As-at aggregation specification
    """

    parameters: AggregateAsAtParameters

    @property
    def agg_result_name(self) -> str:
        """Column name of the aggregated result

        Returns
        -------
        str
            Column name of the aggregated result
        """
        return self.construct_agg_result_name(self.parameters.agg_func, self.parameters.parent)

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.AS_AT

    def get_source_hash_parameters(self) -> dict[str, Any]:
        # Input to be aggregated
        params: dict[str, Any] = {"source_expr": self.source_expr.sql()}

        # Parameters that affect whether aggregation can be done together (e.g. same groupby keys)
        parameters_dict = self.parameters.dict(exclude={"parent", "agg_func", "name"})
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
    ) -> list[AggregateAsAtSpec]:
        assert isinstance(node, AggregateAsAtNode)
        return [
            AggregateAsAtSpec(
                parameters=node.parameters,
                aggregation_source=aggregation_source,
                entity_ids=cast(List[ObjectId], node.parameters.entity_ids),
                serving_names=node.parameters.serving_names,
                serving_names_mapping=serving_names_mapping,
            )
        ]


@dataclass
class LookupSpec(NonTileBasedAggregationSpec):
    """
    LookupSpec contains all information required to generate sql for a lookup feature
    """

    input_column_name: str
    feature_name: str
    entity_column: str
    serving_names: list[str]
    scd_parameters: Optional[SCDLookupParameters]
    event_parameters: Optional[EventLookupParameters]
    is_parent_lookup: bool = False

    @property
    def agg_result_name(self) -> str:
        if self.is_parent_lookup:
            return self.feature_name
        return self.construct_agg_result_name(self.input_column_name)

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.LOOKUP

    def get_source_hash_parameters(self) -> dict[str, Any]:
        params: dict[str, Any] = {
            "source_expr": self.source_expr.sql(),
            "entity_column": self.entity_column,
        }
        if self.scd_parameters is not None:
            params["scd_parameters"] = self.scd_parameters.dict()
        if self.event_parameters is not None:
            params["event_parameters"] = self.event_parameters.dict()
        return params

    @classmethod
    def should_filter_scd_by_current_flag(cls, graph: QueryGraphModel, node: Node) -> bool:
        assert isinstance(node, LookupNode)
        scd_parameters = node.parameters.scd_parameters
        if scd_parameters is not None:
            return cls.get_scd_filter_flag_from_scd_parameters(scd_parameters)
        return False

    @staticmethod
    def get_scd_filter_flag_from_scd_parameters(scd_parameters: SCDLookupParameters) -> bool:
        """
        Returns whether the current flag should be used to filter SCD join

        Parameters
        ----------
        scd_parameters: SCDLookupParameters
            SCD related parameters in the lookup node

        Returns
        -------
        bool
        """
        # Online serving might not have to use SCD join if current flag is applicable
        current_flag_usable_for_online_serving = (
            scd_parameters.current_flag_column is not None and scd_parameters.offset is None
        )
        return current_flag_usable_for_online_serving

    @classmethod
    def construct_specs(
        cls,
        node: Node,
        aggregation_source: AggregationSource,
        serving_names_mapping: Optional[dict[str, str]],
    ) -> list[LookupSpec]:
        assert isinstance(node, LookupNode)
        params = node.parameters
        specs = []
        for input_column_name, feature_name in zip(params.input_column_names, params.feature_names):
            spec = LookupSpec(
                input_column_name=input_column_name,
                feature_name=feature_name,
                entity_column=params.entity_column,
                entity_ids=[params.entity_id],
                serving_names=[params.serving_name],
                serving_names_mapping=serving_names_mapping,
                aggregation_source=aggregation_source,
                scd_parameters=params.scd_parameters,
                event_parameters=params.event_parameters,
            )
            specs.append(spec)
        return specs


@dataclass
class FeatureSpec:
    """
    Feature specification
    """

    feature_name: str
    feature_expr: Expression
