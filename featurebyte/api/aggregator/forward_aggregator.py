"""
Forward aggregator module
"""
from __future__ import annotations

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
        value_column: str,
        method: str,
        horizon: Optional[str] = None,
        blind_spot: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> Target:
        """
        Aggregate given value_column for each group specified in keys over a time horizon.

        Parameters
        ----------
        value_column: str
            Column to be aggregated
        method: str
            Aggregation method
        horizon: str
            Horizon of the aggregation
        blind_spot: str
            Blind spot of the aggregation
        target_name: str
            Name of the target column

        Returns
        -------
        Target
        """
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
            timestamp_col=self.view.timestamp_column,
        )
        # Add forward aggregate node to graph.
        forward_aggregate_node = self.view.graph.add_operation(
            node_type=NodeType.FORWARD_AGGREGATE,
            node_params=node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.view.node],
        )
        # Project target node.
        target_node = self.view.graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [target_name]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[forward_aggregate_node],
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
        value_column: str,
        method: str,
        horizon: Optional[str],
        blind_spot: Optional[str],
        target_name: Optional[str],
        timestamp_col: str,
    ) -> dict[str, Any]:
        """
        Helper function to prepare node parameters.

        Parameters
        ----------
        value_column: str
            Column to be aggregated
        method: str
            Aggregation method
        horizon: str
            Horizon of the aggregation
        blind_spot: str
            Blind spot of the aggregation
        target_name: str
            Name of the target column

        Returns
        -------
        dict[str, Any]
        """
        return {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "horizon": horizon,
            "blind_spot": blind_spot,
            "name": target_name,
            "serving_names": self.serving_names,
            "value_by": value_column,
            "entity_ids": self.entity_ids,
            "table_details": self.view.tabular_source.table_details,
            "timestamp_col": timestamp_col,
        }

    def _validate_parameters(
        self,
        value_column: str,
        method: Optional[str] = None,
        horizon: Optional[str] = None,
        blind_spot: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> None:
        """
        Helper function to validate parameters.

        Parameters
        ----------
        value_column: str
            Column to be aggregated
        method: str
            Aggregation method
        horizon: str
            Horizon of the aggregation
        blind_spot: str
            Blind spot of the aggregation
        target_name: str
            Name of the target column

        Raises
        ------
        ValueError
            raised when target is not specified
        """
        self._validate_method_and_value_column(method=method, value_column=value_column)

        if not target_name:
            raise ValueError("Target name must be specified")

        if horizon:
            parse_duration_string(horizon)

        if blind_spot:
            parse_duration_string(blind_spot)
