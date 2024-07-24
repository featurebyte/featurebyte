"""
This module contains as at aggregator related class
"""

from __future__ import annotations

from typing import List, Optional, Type, cast

from typeguard import typechecked

from featurebyte.api.aggregator.base_asat_aggregator import BaseAsAtAggregator
from featurebyte.api.aggregator.util import conditional_set_skip_fill_na
from featurebyte.api.feature import Feature
from featurebyte.api.scd_view import SCDView
from featurebyte.api.view import View
from featurebyte.enum import AggFunc
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.typing import OptionalScalar


class AsAtAggregator(BaseAsAtAggregator):
    """
    AsAtAggregator implements the aggregate_asat method for GroupBy
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [SCDView]

    @property
    def aggregation_method_name(self) -> str:
        return "aggregate_asat"

    @property
    def output_name_parameter(self) -> str:
        return "feature_name"

    @typechecked
    def aggregate_asat(
        self,
        value_column: Optional[str],
        method: str,
        feature_name: str,
        offset: Optional[str] = None,
        backward: bool = True,
        fill_value: OptionalScalar = None,
        skip_fill_na: Optional[bool] = None,
    ) -> Feature:
        """
        Aggregate a column in SlowlyChangingView as at a point in time

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: str
            Aggregation method
        feature_name: str
            Output feature name
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

        backward: bool
            Whether the offset should be applied backward or forward
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: Optional[bool]
            Whether to skip filling na values

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
            "name": feature_name,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
            "offset": offset,
            "backward": backward,
            **view.get_common_scd_parameters().model_dump(),
        }
        asat_node = self.view.graph.add_operation(
            node_type=NodeType.AGGREGATE_AS_AT,
            node_params=node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.view.node],
        )

        assert method is not None
        assert feature_name is not None
        agg_method = construct_agg_func(agg_func=cast(AggFunc, method))

        return self._project_feature_from_aggregation_node(
            agg_method=agg_method,
            feature_name=feature_name,
            aggregation_node=asat_node,
            method=method,
            value_column=value_column,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )
