from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlglot import Select

from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupbyNode, ItemGroupbyNode
from featurebyte.query_graph.sql.common import apply_serving_names_mapping
from featurebyte.query_graph.sql.tiling import get_aggregator


@dataclass
class AggregationSpec:
    """Aggregation specification"""

    # pylint: disable=too-many-instance-attributes

    window: int
    frequency: int
    blind_spot: int
    time_modulo_frequency: int
    tile_table_id: str
    aggregation_id: str
    keys: list[str]
    serving_names: list[str]
    value_by: str | None
    merge_expr: str
    feature_name: str

    @property
    def agg_result_name(self) -> str:
        """Column name of the aggregated result

        Returns
        -------
        str
            Column name of the aggregated result
        """
        return f"agg_w{self.window}_{self.aggregation_id}"

    @classmethod
    def from_groupby_query_node(
        cls,
        groupby_node: Node,
        serving_names_mapping: dict[str, str] | None = None,
    ) -> list[AggregationSpec]:
        """Construct an AggregationSpec from a query graph and groupby node

        Parameters
        ----------
        groupby_node : Node
            Query graph node with groupby type
        serving_names_mapping : dict[str, str]
            Mapping from original serving name to new serving name

        Returns
        -------
        list[AggregationSpec]
            List of AggregationSpec
        """
        assert isinstance(groupby_node, GroupbyNode)
        tile_table_id = groupby_node.parameters.tile_id
        aggregation_id = groupby_node.parameters.aggregation_id
        params = groupby_node.parameters.dict()
        assert tile_table_id is not None
        assert aggregation_id is not None

        serving_names = params["serving_names"]
        if serving_names_mapping is not None:
            serving_names = apply_serving_names_mapping(serving_names, serving_names_mapping)

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
                value_by=params["value_by"],
                merge_expr=get_aggregator(params["agg_func"]).merge(aggregation_id),
                feature_name=feature_name,
            )
            aggregation_specs.append(agg_spec)

        return aggregation_specs


@dataclass
class ItemAggregationSpec:

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
        assert isinstance(node, ItemGroupbyNode)
        params = node.parameters.dict()
        serving_names = params["serving_names"]
        if serving_names_mapping is not None:
            serving_names = apply_serving_names_mapping(serving_names, serving_names_mapping)
        out = ItemAggregationSpec(
            keys=params["keys"],
            serving_names=serving_names,
            feature_name=params["names"][0],
            agg_expr=agg_expr,
        )
        return out


@dataclass
class FeatureSpec:
    """Feature specification"""

    feature_name: str
    feature_expr: str
