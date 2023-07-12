"""
Forward aggregator module
"""
from __future__ import annotations

from typing import Any, List, Optional, Type, cast

from featurebyte import AggFunc, ChangeView, EventView, ItemView
from featurebyte.api.aggregator.base_aggregator import BaseAggregator
from featurebyte.api.target import Target
from featurebyte.api.view import View
from featurebyte.common.model_util import parse_duration_string
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.agg_func import construct_agg_func


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
        window: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> Target:
        """
        Aggregate given value_column for each group specified in keys over a time window.

        Parameters
        ----------
        value_column: str
            Column to be aggregated
        method: str
            Aggregation method
        window: str
            Window of the aggregation
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
            window=window,
            target_name=target_name,
        )
        # Create new node parameters
        assert value_column is not None
        node_params = self._prepare_node_parameters(
            value_column=value_column,
            method=method,
            window=window,
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
        agg_method = construct_agg_func(agg_func=cast(AggFunc, method))
        output_var_type = self.get_output_var_type(agg_method, method, value_column)
        return Target(
            name=target_name,
            entity_ids=self.entity_ids,
            graph=self.view.graph,
            node_name=target_node.name,
            tabular_source=self.view.tabular_source,
            feature_store=self.view.feature_store,
            dtype=output_var_type,
        )

    def _prepare_node_parameters(
        self,
        value_column: str,
        method: str,
        window: Optional[str],
        target_name: Optional[str],
        timestamp_col: Optional[str],
    ) -> dict[str, Any]:
        """
        Helper function to prepare node parameters.

        Parameters
        ----------
        value_column: str
            Column to be aggregated
        method: str
            Aggregation method
        window: str
            Window of the aggregation
        target_name: str
            Name of the target column
        timestamp_col: str
            Timestamp column

        Returns
        -------
        dict[str, Any]
        """
        return {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "window": window,
            "name": target_name,
            "serving_names": self.serving_names,
            "value_by": self.category,
            "entity_ids": self.entity_ids,
            "timestamp_col": timestamp_col,
        }

    def _validate_parameters(
        self,
        value_column: str,
        method: Optional[str] = None,
        window: Optional[str] = None,
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
        window: str
            Window of the aggregation
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

        if window:
            parse_duration_string(window)
