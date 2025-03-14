"""
Forward aggregator module
"""

from __future__ import annotations

from typing import Any, List, Optional, Type, cast

from featurebyte.api.aggregator.base_aggregator import BaseAggregator
from featurebyte.api.aggregator.util import conditional_set_skip_fill_na
from featurebyte.api.change_view import ChangeView
from featurebyte.api.event_view import EventView
from featurebyte.api.item_view import ItemView
from featurebyte.api.target import Target
from featurebyte.api.view import View
from featurebyte.common.model_util import parse_duration_string
from featurebyte.common.validator import validate_target_type
from featurebyte.enum import AggFunc, TargetType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.typing import OptionalScalar


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

    @property
    def not_supported_aggregation_methods(self) -> Optional[List[AggFunc]]:
        return [AggFunc.LATEST]

    def forward_aggregate(
        self,
        value_column: Optional[str],
        method: str,
        window: str,
        target_name: str,
        fill_value: OptionalScalar = None,
        skip_fill_na: Optional[bool] = None,
        offset: Optional[str] = None,
        target_type: Optional[TargetType] = None,
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
        skip_fill_na: Optional[bool]
            Whether to skip filling NaN values
        offset: Optional[str]
            Offset duration to apply to the window, such as '1d'. If specified, the windows will be
            shifted forward by the offset duration
        target_type: Optional[TargetType]
            Type of the Target used to indicate the modeling type of the target

        Returns
        -------
        Target
        """
        # Validation
        skip_fill_na = conditional_set_skip_fill_na(skip_fill_na, fill_value)
        self._validate_parameters(
            value_column=value_column,
            method=method,
            window=window,
            target_name=target_name,
            offset=offset,
        )
        self._validate_fill_value_and_skip_fill_na(fill_value=fill_value, skip_fill_na=skip_fill_na)
        # Create new node parameters
        node_params = self._prepare_node_parameters(
            value_column=value_column,
            method=method,
            window=window,
            target_name=target_name,
            timestamp_col=self.view.timestamp_column,
            offset=offset,
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
        if target_type:
            validate_target_type(target_type, target.dtype)
            target.update_target_type(target_type)
        if not skip_fill_na:
            return self._fill_feature_or_target(target, method, target_name, fill_value)  # type: ignore[return-value]
        return target

    def _prepare_node_parameters(
        self,
        value_column: Optional[str],
        method: str,
        window: Optional[str],
        target_name: Optional[str],
        timestamp_col: Optional[str],
        offset: Optional[str],
    ) -> dict[str, Any]:
        """
        Helper function to prepare node parameters.

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: str
            Aggregation method
        window: str
            Window of the aggregation
        target_name: str
            Name of the target column
        timestamp_col: str
            Timestamp column
        offset: Optional[str]
            Offset for the window

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
            "timestamp_metadata": self.view.operation_structure.get_dtype_metadata(timestamp_col),
            "offset": offset,
        }

    def _validate_parameters(
        self,
        value_column: Optional[str],
        method: str,
        window: Optional[str] = None,
        target_name: Optional[str] = None,
        offset: Optional[str] = None,
    ) -> None:
        """
        Helper function to validate parameters.

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: str
            Aggregation method
        window: str
            Window of the aggregation
        target_name: str
            Name of the target column
        offset: Optional[str]
            Offset for the window

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

        if offset:
            parse_duration_string(offset)
