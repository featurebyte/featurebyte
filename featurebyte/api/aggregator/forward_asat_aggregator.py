"""
This module contains forward as at aggregator related class
"""

from __future__ import annotations

from typing import List, Optional, Type, cast

from typeguard import typechecked

from featurebyte.api.aggregator.base_asat_aggregator import BaseAsAtAggregator
from featurebyte.api.aggregator.util import conditional_set_skip_fill_na
from featurebyte.api.scd_view import SCDView
from featurebyte.api.target import Target
from featurebyte.api.view import View
from featurebyte.common.validator import validate_target_type
from featurebyte.enum import AggFunc, TargetType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.typing import OptionalScalar


class ForwardAsAtAggregator(BaseAsAtAggregator):
    """
    AsAtAggregator implements the forward_aggregate_asat method for GroupBy
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [SCDView]

    @property
    def aggregation_method_name(self) -> str:
        return "forward_aggregate_asat"

    @property
    def output_name_parameter(self) -> str:
        return "target_name"

    @typechecked
    def forward_aggregate_asat(
        self,
        value_column: Optional[str],
        method: str,
        target_name: str,
        offset: Optional[str] = None,
        fill_value: OptionalScalar = None,
        skip_fill_na: Optional[bool] = None,
        target_type: Optional[TargetType] = None,
    ) -> Target:
        """
        Aggregate a column in SlowlyChangingView as at a point in time to produce a Target

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: str
            Aggregation method
        target_name: str
            Output target name
        offset: Optional[str]
            Optional offset to apply to the point in time column in the feature request. The
            aggregation result will be as at the point in time adjusted by this offset. Format of
            offset is "{size}{unit}", where size is a positive integer and unit is one of the
            following:

            "ns": nanosecond
            "us": microsecond
            "ms": millisecond
            "s": second
            "m": minute
            "h": hour
            "d": day
            "w": week
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: bool
            Whether to skip filling na values
        target_type: Optional[TargetType]
            Type of the Target used to indicate the modeling type of the target

        Returns
        -------
        Feature
        """
        skip_fill_na = conditional_set_skip_fill_na(skip_fill_na, fill_value)
        self._validate_parameters(
            method=method,
            value_column=value_column,
            offset=offset,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )

        view = cast(SCDView, self.view)
        node_params = {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "value_by": self.category,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
            "name": target_name,
            "offset": offset,
            **view.get_common_scd_parameters().model_dump(),
        }
        node = self.view.graph.add_operation(
            node_type=NodeType.FORWARD_AGGREGATE_AS_AT,
            node_params=node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.view.node],
        )

        assert method is not None
        assert target_name is not None
        agg_method = construct_agg_func(agg_func=cast(AggFunc, method))
        output_var_type = self.get_output_var_type(agg_method, method, value_column)
        target = self.view.project_target_from_node(node, target_name, output_var_type)
        if target_type:
            validate_target_type(target_type, target.dtype)
            target.update_target_type(target_type)
        if not skip_fill_na:
            return self._fill_feature_or_target(target, method, target_name, fill_value)  # type: ignore[return-value]
        return target
