"""
This module contains forecast as at aggregator related class
"""

from __future__ import annotations

from typing import Any, List, Optional, Type, cast

from typeguard import typechecked

from featurebyte.api.aggregator.base_asat_aggregator import BaseAsAtAggregator
from featurebyte.api.aggregator.util import conditional_set_skip_fill_na
from featurebyte.api.context import Context
from featurebyte.api.scd_view import SCDView
from featurebyte.api.snapshots_view import SnapshotsView
from featurebyte.api.target import Target
from featurebyte.api.view import View
from featurebyte.common.validator import validate_target_type
from featurebyte.enum import AggFunc, TargetType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.node.generic import SnapshotsLookupParameters
from featurebyte.typing import OffsetType, OptionalScalar


class ForecastAsAtAggregator(BaseAsAtAggregator):
    """
    ForecastAsAtAggregator implements the forecast_aggregate_asat method for GroupBy.
    This aggregator uses FORECAST_POINT instead of POINT_IN_TIME for temporal lookups.
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [SCDView, SnapshotsView]

    @property
    def aggregation_method_name(self) -> str:
        return "forecast_aggregate_asat"

    @property
    def output_name_parameter(self) -> str:
        return "target_name"

    @typechecked
    def forecast_aggregate_asat(
        self,
        value_column: Optional[str],
        method: str,
        target_name: str,
        context: "Context",
        offset: Optional[OffsetType] = None,
        fill_value: OptionalScalar = None,
        skip_fill_na: Optional[bool] = None,
        target_type: Optional[TargetType] = None,
    ) -> Target:
        """
        Aggregate a column as at the FORECAST_POINT to produce a Target.

        Similar to forward_aggregate_asat, but uses FORECAST_POINT (converted to UTC) instead of
        POINT_IN_TIME for the temporal aggregation. The aggregation is performed on rows that are
        valid as at the FORECAST_POINT in the observation table.

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: str
            Aggregation method
        target_name: str
            Output target name
        context: Context
            The context with forecast_point_schema defining the forecast configuration
        offset: Optional[OffsetType]
            Optional offset to apply to the forecast point. The aggregation result will be as at
            the forecast point adjusted by this offset. For SCD views, format is "{size}{unit}".
            For Snapshots views, this is an integer representing number of time intervals.
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: Optional[bool]
            Whether to skip filling na values
        target_type: Optional[TargetType]
            Type of the Target used to indicate the modeling type of the target

        Returns
        -------
        Target

        Raises
        ------
        ValueError
            If context doesn't have forecast_point_schema
        """
        # Validate context has forecast_point_schema
        if context.forecast_point_schema is None:
            raise ValueError(
                f"Context '{context.name}' does not have a forecast_point_schema. "
                "Please create a Context with forecast_point_schema to use forecast_aggregate_asat()."
            )

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
            "use_forecast_point": True,
            "forecast_point_schema": context.forecast_point_schema.model_dump(),
        }

        if isinstance(self.view, SCDView):
            node_params["offset"] = offset if isinstance(offset, str) else None
            node_params.update(**self.view.get_common_scd_parameters().model_dump())
        else:
            assert isinstance(self.view, SnapshotsView)
            if offset is not None:
                assert isinstance(offset, int)
                offset_size = offset
            else:
                offset_size = None
            snapshots_lookup_parameters = SnapshotsLookupParameters(
                snapshot_datetime_column=self.view.snapshot_datetime_column,
                time_interval=self.view.time_interval,
                snapshot_datetime_metadata=DBVarTypeMetadata(
                    timestamp_schema=self.view.snapshot_datetime_schema
                ),
                feature_job_setting=self.view.default_feature_job_setting,
                offset_size=offset_size,
            ).model_dump()
            node_params["effective_timestamp_column"] = self.view.snapshot_datetime_column
            node_params["snapshots_parameters"] = snapshots_lookup_parameters

        node = self.view.graph.add_operation(
            node_type=NodeType.FORECAST_AGGREGATE_AS_AT,
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
