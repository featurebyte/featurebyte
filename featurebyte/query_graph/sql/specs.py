"""
Module for data structures that describe different types of aggregations that form features
"""
from __future__ import annotations

from typing import Any, Optional, cast

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd
from sqlglot.expressions import Select

from featurebyte.enum import SourceType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    GroupbyNode,
    ItemGroupbyNode,
    LookupNode,
    SCDLookupParameters,
)
from featurebyte.query_graph.sql.common import apply_serving_names_mapping
from featurebyte.query_graph.sql.tiling import get_aggregator


@dataclass  # type: ignore[misc]
class AggregationSpec(ABC):
    """
    Base class of all aggregation specifications
    """

    serving_names: list[str]
    serving_names_mapping: Optional[dict[str, str]]

    def __post_init__(self) -> None:
        if self.serving_names_mapping is not None:
            self.serving_names = apply_serving_names_mapping(
                self.serving_names, self.serving_names_mapping
            )

    @property
    @abstractmethod
    def agg_result_name(self) -> str:
        """Column name of the aggregated result

        Returns
        -------
        str
            Column names of the aggregated result
        """


@dataclass
class WindowAggregationSpec(AggregationSpec):
    """
    Window aggregation specification
    """

    # pylint: disable=too-many-instance-attributes

    window: int
    frequency: int
    blind_spot: int
    time_modulo_frequency: int
    tile_table_id: str
    aggregation_id: str
    keys: list[str]
    value_by: str | None
    merge_expr: str
    feature_name: str

    @property
    def agg_result_name(self) -> str:
        """Column name of the aggregated result

        Returns
        -------
        str
            Column names of the aggregated result
        """
        return f"agg_w{self.window}_{self.aggregation_id}"

    @classmethod
    def from_groupby_query_node(
        cls,
        groupby_node: Node,
        serving_names_mapping: dict[str, str] | None = None,
    ) -> list[WindowAggregationSpec]:
        """Construct an AggregationSpec from a query graph and groupby node

        Parameters
        ----------
        groupby_node : Node
            Query graph node with groupby type
        serving_names_mapping : dict[str, str]
            Mapping from original serving name to new serving name

        Returns
        -------
        list[WindowAggregationSpec]
            List of AggregationSpec
        """
        assert isinstance(groupby_node, GroupbyNode)
        tile_table_id = groupby_node.parameters.tile_id
        aggregation_id = groupby_node.parameters.aggregation_id
        params = groupby_node.parameters.dict()
        assert tile_table_id is not None
        assert aggregation_id is not None

        serving_names = params["serving_names"]
        aggregation_specs = []
        for window, feature_name in zip(params["windows"], params["names"]):
            params = groupby_node.parameters.dict()
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
                merge_expr=get_aggregator(params["agg_func"]).merge(aggregation_id),
                feature_name=feature_name,
            )
            aggregation_specs.append(agg_spec)

        return aggregation_specs


@dataclass
class ItemAggregationSpec(AggregationSpec):
    """
    Non-time aware aggregation specification
    """

    keys: list[str]
    serving_names: list[str]
    feature_name: str
    agg_expr: Select | None

    @property
    def agg_result_name(self) -> str:
        """Column name of the aggregated result

        Returns
        -------
        str
            Column name of the aggregated result
        """
        # Note: Ideally, this internal aggregated result name should be based on a unique identifier
        # that uniquely identifies the aggregation, instead of directly using the feature_name.
        # Should be fixed when aggregation_id is added to the parameters of ItemGroupby query node.
        return self.feature_name

    @classmethod
    def from_item_groupby_query_node(
        cls,
        node: Node,
        agg_expr: Select | None = None,
        serving_names_mapping: dict[str, str] | None = None,
    ) -> ItemAggregationSpec:
        """Construct a ItemAggregationSpec object given a query graph node

        Parameters
        ----------
        node : Node
            Query graph node
        agg_expr : Select | None
            The item groupby aggregation expression
        serving_names_mapping : dict[str, str]
            Mapping from original serving name to new serving name

        Returns
        -------
        ItemAggregationSpec
        """
        assert isinstance(node, ItemGroupbyNode)
        params = node.parameters.dict()
        serving_names = params["serving_names"]
        out = ItemAggregationSpec(
            keys=params["keys"],
            serving_names=serving_names,
            serving_names_mapping=serving_names_mapping,
            feature_name=params["name"],
            agg_expr=agg_expr,
        )
        return out


@dataclass
class LookupSpec(AggregationSpec):
    """
    LookupSpec contains all information required to generate sql for a lookup feature
    """

    input_column_name: str
    feature_name: str
    entity_column: str
    serving_names: list[str]
    source_expr: Select
    scd_parameters: Optional[SCDLookupParameters]

    @property
    def agg_result_name(self) -> str:
        name = f"{self.input_column_name}_{self.source_hash}"
        return name

    @property
    def source_hash(self) -> str:
        """
        Returns a unique identifier derived from source_expr and entity column

        Returns
        -------
        str
        """
        hasher = hashlib.shake_128()
        params: dict[str, Any] = {
            "source_expr": self.source_expr.sql(),
            "entity_column": self.entity_column,
        }
        if self.scd_parameters is not None:
            params["scd_parameters"] = self.scd_parameters.dict()
        hasher.update(json.dumps(params, sort_keys=True).encode("utf-8"))
        return hasher.hexdigest(8)

    @classmethod
    def _get_source_sql_expr(
        cls, graph: QueryGraphModel, node: Node, source_type: SourceType
    ) -> Select:
        # pylint: disable=import-outside-toplevel
        from featurebyte.query_graph.sql.builder import SQLOperationGraph
        from featurebyte.query_graph.sql.common import SQLType

        sql_node = SQLOperationGraph(
            graph, sql_type=SQLType.AGGREGATION, source_type=source_type
        ).build(node)
        return cast(Select, sql_node.sql)

    @classmethod
    def from_lookup_query_node(
        cls,
        node: Node,
        source_expr: Optional[Select] = None,
        graph: Optional[QueryGraphModel] = None,
        source_type: Optional[SourceType] = None,
        serving_names_mapping: Optional[dict[str, str]] = None,
    ) -> list[LookupSpec]:
        """
        Construct a list of LookupSpec given a lookup query graph node

        Parameters
        ----------
        node: Node
            Query graph node
        source_expr: Optional[Select]
            Select statement that represents the source to lookup from. If not provided, it will be
            inferred from the node and graph.
        graph: Optional[QueryGraphModel]
            Query graph. Mandatory if source_expr is not provided
        source_type: Optional[SourceType]
            Source type information. Mandatory if source_expr is not provided
        serving_names_mapping: Optional[dict[str, str]]
            Serving names mapping

        Returns
        -------
        list[LookupSpec]
        """
        assert isinstance(node, LookupNode)

        if source_expr is None:
            assert graph is not None
            assert source_type is not None
            source_expr = cls._get_source_sql_expr(graph=graph, node=node, source_type=source_type)

        params = node.parameters
        specs = []
        for input_column_name, feature_name in zip(params.input_column_names, params.feature_names):
            spec = LookupSpec(
                input_column_name=input_column_name,
                feature_name=feature_name,
                entity_column=params.entity_column,
                serving_names=[params.serving_name],
                serving_names_mapping=serving_names_mapping,
                source_expr=source_expr,
                scd_parameters=params.scd_parameters,
            )
            specs.append(spec)
        return specs


@dataclass
class FeatureSpec:
    """
    Feature specification
    """

    feature_name: str
    feature_expr: str
