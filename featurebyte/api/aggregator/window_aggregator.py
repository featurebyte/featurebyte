"""
This module contains window aggregator related class
"""

from __future__ import annotations

import os
from typing import Any, List, Literal, Optional, Type, cast

from featurebyte.api.aggregator.base_aggregator import BaseAggregator
from featurebyte.api.change_view import ChangeView
from featurebyte.api.event_view import EventView
from featurebyte.api.feature_group import FeatureGroup
from featurebyte.api.item_view import ItemView
from featurebyte.api.time_series_view import TimeSeriesView
from featurebyte.api.view import View
from featurebyte.api.window_validator import validate_window
from featurebyte.enum import AggFunc, TimeIntervalUnit
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.feature_job_setting import (
    CronFeatureJobSetting,
    FeatureJobSetting,
)
from featurebyte.query_graph.model.window import CalendarWindow
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.transform.reconstruction import (
    GroupByNode,
    add_pruning_sensitive_operation,
)
from featurebyte.typing import OptionalScalar


class WindowAggregator(BaseAggregator):
    """
    WindowAggregator implements the aggregate_over method for GroupBy
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [EventView, ItemView, ChangeView, TimeSeriesView]

    @property
    def is_time_series_aggregation(self) -> bool:
        return isinstance(self.view, TimeSeriesView)

    @property
    def aggregation_method_name(self) -> str:
        return "aggregate_over"

    def aggregate_over(
        self,
        value_column: Optional[str],
        method: str,
        windows: List[Optional[str] | CalendarWindow],
        feature_names: List[str],
        timestamp_column: Optional[str] = None,
        feature_job_setting: Optional[FeatureJobSetting | CronFeatureJobSetting] = None,
        fill_value: OptionalScalar = None,
        skip_fill_na: Optional[bool] = None,
        offset: Optional[str | CalendarWindow] = None,
    ) -> FeatureGroup:
        """
        Aggregate given value_column for each group specified in keys over a list of time windows

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: str
            Aggregation method
        windows: List[str | None]
            List of aggregation window sizes. Use None to indicated unbounded window size (only
            applicable to "latest" method)
        feature_names: List[str]
            Output feature names
        timestamp_column: Optional[str]
            Timestamp column used to specify the window (if not specified, event table timestamp is used)
        feature_job_setting: Optional[FeatureJobSetting]
            Dictionary contains `blind_spot`, `period` and `offset` keys which are feature job setting parameters
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: Optional[bool]
            Whether to skip filling NaN values
        offset: Optional[str]
            Offset duration to apply to the window, such as '1d'. If specified, the windows will be
            shifted backward by the offset duration

        Returns
        -------
        FeatureGroup
        """
        if skip_fill_na is None:
            skip_fill_na = fill_value is None

        if value_column and self.view[value_column].associated_timezone_column_name:
            # If the value column has an associated timezone column
            new_value_column = f"__{value_column}_zip_timezone"
            self.view[new_value_column] = self.view[value_column].zip_timestamp_timezone_columns()  # type: ignore
            value_column = new_value_column

        self._validate_parameters(
            value_column=value_column,
            method=method,
            windows=windows,
            feature_names=feature_names,
            feature_job_setting=feature_job_setting,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
            offset=offset,
        )
        self.view.validate_aggregate_over_parameters(
            keys=self.keys,
            value_column=value_column,
        )

        node_params = self._prepare_node_parameters(
            value_column=value_column,
            method=method,
            windows=windows,
            feature_names=feature_names,
            timestamp_column=timestamp_column,
            value_by_column=self.category,
            feature_job_setting=feature_job_setting,
            offset=offset,
        )
        assert method is not None
        agg_method = construct_agg_func(agg_func=cast(AggFunc, method))
        aggregation_node = self._add_aggregation_node(agg_method.type, node_params)
        assert isinstance(feature_names, list)

        items = []
        for feature_name in feature_names:
            feature = self._project_feature_from_aggregation_node(
                agg_method=agg_method,
                feature_name=feature_name,
                aggregation_node=aggregation_node,
                method=method,
                value_column=value_column,
                fill_value=fill_value,
                skip_fill_na=skip_fill_na,
            )
            items.append(feature)
        feature_group = FeatureGroup(items)
        return feature_group

    def _validate_parameters(
        self,
        value_column: Optional[str],
        method: str,
        windows: list[Optional[str] | CalendarWindow],
        feature_names: list[str],
        feature_job_setting: Optional[FeatureJobSetting | CronFeatureJobSetting],
        fill_value: OptionalScalar,
        skip_fill_na: bool,
        offset: Optional[str | CalendarWindow],
    ) -> None:
        self._validate_method_and_value_column(method=method, value_column=value_column)
        self._validate_fill_value_and_skip_fill_na(fill_value=fill_value, skip_fill_na=skip_fill_na)

        if not isinstance(windows, list) or len(windows) == 0:
            raise ValueError(f"windows is required and should be a non-empty list; got {windows}")

        if not isinstance(feature_names, list):
            raise ValueError(
                f"feature_names is required and should be a non-empty list; got {feature_names}"
            )

        if len(windows) != len(feature_names):
            raise ValueError(
                "Window length must be the same as the number of output feature names."
            )

        if len(windows) != len(set(feature_names)) or len(set(windows)) != len(feature_names):
            raise ValueError("Window sizes or feature names contains duplicated value(s).")

        number_of_unbounded_windows = len([w for w in windows if w is None])

        if self.is_time_series_aggregation:
            if number_of_unbounded_windows > 0:
                raise ValueError("Unbounded window is not supported for time series aggregation")
            if offset is not None:
                offset_unit = self._validate_time_series_window("offset", offset)
            else:
                offset_unit = None
            for window in windows:
                assert window is not None  # due to the above check
                self._validate_time_series_window("windows", window, compatible_unit=offset_unit)
            if feature_job_setting is not None and not isinstance(
                feature_job_setting, CronFeatureJobSetting
            ):
                raise ValueError(
                    "feature_job_setting must be CronFeatureJobSetting for TimeSeriesView"
                )
        else:
            if number_of_unbounded_windows > 0:
                if method != AggFunc.LATEST:
                    raise ValueError('Unbounded window is only supported for the "latest" method')

                if self.category is not None:
                    raise ValueError(
                        "category is not supported for aggregation with unbounded window"
                    )

            if feature_job_setting is not None and not isinstance(
                feature_job_setting, FeatureJobSetting
            ):
                raise ValueError(
                    "feature_job_setting must be FeatureJobSetting for non-TimeSeriesView"
                )
            parsed_feature_job_setting = FeatureJobSetting(
                **self._get_job_setting_params(feature_job_setting)
            )

            if windows is not None:
                for window in windows:
                    if window is not None:
                        if isinstance(window, CalendarWindow):
                            raise ValueError("CalendarWindow is only supported for TimeSeriesView")
                        validate_window(window, parsed_feature_job_setting.period)

            if offset is not None:
                if isinstance(offset, CalendarWindow):
                    raise ValueError("CalendarWindow is only supported for TimeSeriesView")
                validate_window(offset, parsed_feature_job_setting.period)

    def _validate_time_series_window(
        self,
        param_type: Literal["windows", "offset"],
        window: str | CalendarWindow,
        compatible_unit: Optional[TimeIntervalUnit] = None,
    ) -> TimeIntervalUnit:
        if not isinstance(window, CalendarWindow):
            if param_type == "windows":
                raise ValueError(
                    f"Please specify {param_type} as a list of CalendarWindow for TimeSeriesView"
                )
            else:
                raise ValueError(
                    f"Please specify {param_type} as CalendarWindow for TimeSeriesView"
                )
        view = self.view
        assert isinstance(view, TimeSeriesView)
        unit = TimeIntervalUnit(window.unit)
        if compatible_unit is not None and unit != compatible_unit:
            raise ValueError(
                f"Window unit ({window.unit}) must be the same as the offset unit ({compatible_unit})"
            )
        if unit < TimeIntervalUnit(view.time_interval.unit):
            raise ValueError(
                f"Window unit ({window.unit}) cannot be smaller than the table's time interval unit ({view.time_interval.unit})"
            )
        return unit

    def _get_job_setting_params(
        self, feature_job_setting: Optional[FeatureJobSetting | CronFeatureJobSetting]
    ) -> dict[str, Any]:
        if feature_job_setting is not None:
            return feature_job_setting.model_dump()

        # Return default if no feature_job_setting is provided.
        default_setting = self.view.default_feature_job_setting
        if default_setting is None:
            raise ValueError(
                f"feature_job_setting is required as the {type(self.view).__name__} does not "
                "have a default feature job setting"
            )
        return default_setting.model_dump()  # type: ignore[no-any-return]

    def _prepare_node_parameters(
        self,
        value_column: Optional[str],
        method: str,
        windows: list[Optional[str] | CalendarWindow],
        offset: Optional[str | CalendarWindow],
        feature_names: Optional[list[str]],
        timestamp_column: Optional[str] = None,
        value_by_column: Optional[str] = None,
        feature_job_setting: Optional[FeatureJobSetting | CronFeatureJobSetting] = None,
    ) -> dict[str, Any]:
        feature_job_setting_dict = self._get_job_setting_params(feature_job_setting)
        params: dict[str, Any] = {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "value_by": value_by_column,
            "windows": windows,
            "offset": offset,
            "feature_job_setting": feature_job_setting_dict,
            "names": feature_names,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
        }
        if self.is_time_series_aggregation:
            assert isinstance(self.view, TimeSeriesView)
            params.update({
                "reference_datetime_column": self.view.reference_datetime_column,
                "reference_datetime_metadata": self.view.operation_structure.get_dtype_metadata(
                    self.view.reference_datetime_column
                ),
                "time_interval": self.view.time_interval.model_dump(),
            })
        else:
            tile_id_version = int(os.environ.get("FEATUREBYTE_TILE_ID_VERSION", "2"))
            timestamp_column = timestamp_column or self.view.timestamp_column
            params.update({
                "timestamp": timestamp_column,
                "timestamp_metadata": self.view.operation_structure.get_dtype_metadata(
                    timestamp_column
                ),
                "tile_id_version": tile_id_version,
            })
        return params

    def _add_aggregation_node(self, agg_func: AggFunc, node_params: dict[str, Any]) -> Node:
        if self.is_time_series_aggregation:
            return self.view.graph.add_operation(
                node_type=NodeType.TIME_SERIES_WINDOW_AGGREGATE,
                node_params=node_params,
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.view.node],
            )
        if agg_func == AggFunc.COUNT_DISTINCT:
            return self.view.graph.add_operation(
                node_type=NodeType.NON_TILE_WINDOW_AGGREGATE,
                node_params=node_params,
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.view.node],
            )
        return add_pruning_sensitive_operation(
            graph=self.view.graph,
            node_cls=GroupByNode,
            node_params=node_params,
            input_node=self.view.node,
            operation_structure_info=self.view.operation_structure_info,
        )
