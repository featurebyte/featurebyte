"""
This module contains forward as at aggregator related class
"""

from __future__ import annotations

from typing import Any, List, Optional, Type, cast

from typeguard import typechecked

from featurebyte.api.aggregator.base_asat_aggregator import BaseAsAtAggregator
from featurebyte.api.aggregator.util import conditional_set_skip_fill_na
from featurebyte.api.scd_view import SCDView
from featurebyte.api.snapshots_view import SnapshotsView
from featurebyte.api.target import Target
from featurebyte.api.time_series_view import TimeSeriesView
from featurebyte.api.view import View
from featurebyte.common.validator import validate_target_type
from featurebyte.enum import AggFunc, TargetType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.node.generic import SnapshotsLookupParameters
from featurebyte.typing import OffsetType, OptionalScalar


class ForwardAsAtAggregator(BaseAsAtAggregator):
    """
    AsAtAggregator implements the forward_aggregate_asat method for GroupBy
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [SCDView, SnapshotsView, TimeSeriesView]

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
        offset: Optional[OffsetType] = None,
        fill_value: OptionalScalar = None,
        skip_fill_na: Optional[bool] = None,
        target_type: Optional[TargetType] = None,
    ) -> Target:
        """
        Aggregate a column as at a point in time to produce a Target

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: str
            Aggregation method
        target_name: str
            Output target name
        offset: Optional[OffsetType]
            Optional offset to apply to the point in time column in the feature request. The
            aggregation result will be as at the point in time adjusted by this offset.

            For SCDView, the format is "{size}{unit}", where size is a positive integer and unit
            is one of: "ns", "us", "ms", "s", "m", "h", "d", "w".

            For SnapshotsView and TimeSeriesView, the offset should be an integer specifying the
            number of time interval steps.
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: bool
            Whether to skip filling na values
        target_type: Optional[TargetType]
            Type of the Target used to indicate the modeling type of the target

        Returns
        -------
        Target
        """
        skip_fill_na = conditional_set_skip_fill_na(skip_fill_na, fill_value)
        self._validate_parameters(
            method=method,
            value_column=value_column,
            offset=offset,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )

        node_params: dict[str, Any] = {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "value_by": self.category,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
            "name": target_name,
            "offset": offset if isinstance(offset, str) else None,
        }
        if isinstance(self.view, SCDView):
            node_params.update(**self.view.get_common_scd_parameters().model_dump())
        elif isinstance(self.view, (SnapshotsView, TimeSeriesView)):
            if offset is not None:
                assert isinstance(offset, int)
                offset_size = offset
            else:
                offset_size = None

            if isinstance(self.view, SnapshotsView):
                snapshot_datetime_column = self.view.snapshot_datetime_column
                snapshot_datetime_schema = self.view.snapshot_datetime_schema
            else:
                snapshot_datetime_column = self.view.reference_datetime_column
                snapshot_datetime_schema = self.view.reference_datetime_schema

            snapshots_lookup_parameters = SnapshotsLookupParameters(
                snapshot_datetime_column=snapshot_datetime_column,
                time_interval=self.view.time_interval,
                snapshot_datetime_metadata=DBVarTypeMetadata(
                    timestamp_schema=snapshot_datetime_schema,
                ),
                offset_size=offset_size,
            ).model_dump()
            node_params["effective_timestamp_column"] = snapshot_datetime_column
            node_params["snapshots_parameters"] = snapshots_lookup_parameters

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
