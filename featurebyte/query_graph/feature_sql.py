from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.query_graph.tiling import get_aggregator, get_tile_table_identifier


@dataclass
class AggregationSpec:
    """Aggregation specification"""

    window: int
    frequency: int
    blind_spot: int
    time_modulo_frequency: int
    tile_table_id: str
    entity_ids: list[str]
    merge_expr: str
    feature_name: str

    @property
    def agg_result_name(self):
        """Column name of the aggregated result"""
        return f"agg_w{self.window}_{self.tile_table_id}"

    @classmethod
    def from_groupby_query_node(
        cls,
        graph: QueryGraph,
        groupby_node: Node,
    ) -> list[AggregationSpec]:
        tile_table_id = get_tile_table_identifier(graph, groupby_node)
        params = groupby_node.parameters
        aggregation_specs = []
        for window, feature_name in zip(params["windows"], params["names"]):
            params = groupby_node.parameters
            window = int(pd.Timedelta(window).total_seconds())
            agg_spec = cls(
                window=window,
                frequency=params["frequency"],
                time_modulo_frequency=params["time_modulo_frequency"],
                blind_spot=params["blind_spot"],
                tile_table_id=tile_table_id,
                entity_ids=params["keys"],
                merge_expr=get_aggregator(params["agg_func"]).merge(),
                feature_name=feature_name,
            )
            aggregation_specs.append(agg_spec)
        return aggregation_specs


@dataclass
class FeatureSpec:
    """Feature specification"""

    feature_name: str
    feature_expr: str
