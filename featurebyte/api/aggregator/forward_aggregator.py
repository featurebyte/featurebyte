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
from featurebyte.common.typing import OptionalScalar
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
        fill_value: OptionalScalar = None,
        skip_fill_na: bool = False,
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
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: bool
            Whether to skip filling NaN values

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
        self._validate_fill_value_and_skip_fill_na(fill_value=fill_value, skip_fill_na=skip_fill_na)
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
        agg_method = construct_agg_func(agg_func=cast(AggFunc, method))
        output_var_type = self.get_output_var_type(agg_method, method, value_column)
        # Project, build and return Target
        assert target_name is not None
        target = self.view.project_target_from_node(
            forward_aggregate_node, target_name, output_var_type
        )
        if not skip_fill_na:
            return self._fill_feature_or_target(target, method, target_name, fill_value)  # type: ignore[return-value]
        return target

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
