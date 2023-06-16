"""
Forward aggregator module
"""
from typing import Any, List, Optional, Type

from featurebyte import ChangeView, EventView, ItemView
from featurebyte.api.aggregator.base_aggregator import BaseAggregator
from featurebyte.api.target import Target
from featurebyte.api.view import View
from featurebyte.common.model_util import parse_duration_string
from featurebyte.query_graph.enum import NodeOutputType, NodeType


class ForwardAggregator(BaseAggregator):
    """
    ForwardAggregator implements the forward_aggregate method for GroupBy
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [EventView, ItemView, ChangeView]

    @property
    def aggregation_method_name(self) -> str:
        return "forward_aggregate"

    def forward_aggregate(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        horizon: Optional[str] = None,
        blind_spot: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> Target:
        # Validation
        self._validate_parameters(
            value_column=value_column,
            method=method,
            horizon=horizon,
            blind_spot=blind_spot,
            target_name=target_name,
        )
        # Create new node parameters
        node_params = self._prepare_node_parameters(
            value_column=value_column,
            method=method,
            horizon=horizon,
            blind_spot=blind_spot,
            target_name=target_name,
        )
        # Add forward aggregate node to graph.
        groupby_node = self.view.graph.add_operation(
            node_type=NodeType.FORWARD_AGGREGATE,
            node_params=node_params,
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[self.view.node],
        )
        # Project target node.
        target_node = self.view.graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [target_name]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[groupby_node],
        )
        # Build and return Target
        return Target(
            name=target_name,
            entity_ids=self.entity_ids,
            horizon=horizon,
            blind_spot=blind_spot,
            graph=self.view.graph,
            node_name=target_node.name,
        )

    def _prepare_node_parameters(
        self,
        value_column: Optional[str],
        method: Optional[str],
        horizon: Optional[str],
        blind_spot: Optional[str],
        target_name: Optional[str],
    ) -> dict[str, Any]:
        return {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "horizon": horizon,
            "blind_spot": blind_spot,
            "name": target_name,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
            "table_details": self.view.tabular_source.table_details,
        }

    def _validate_parameters(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        horizon: Optional[str] = None,
        blind_spot: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> None:
        self._validate_method_and_value_column(method=method, value_column=value_column)

        if not target_name:
            raise ValueError("Target name must be specified")

        if horizon:
            parse_duration_string(horizon)

        if blind_spot:
            parse_duration_string(blind_spot)
