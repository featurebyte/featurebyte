"""
This module contains window aggregator related class
"""

from __future__ import annotations

from typing import Any, List, Optional, Type, cast

import os

from featurebyte.api.aggregator.base_aggregator import BaseAggregator
from featurebyte.api.change_view import ChangeView
from featurebyte.api.event_view import EventView
from featurebyte.api.feature_group import FeatureGroup
from featurebyte.api.item_view import ItemView
from featurebyte.api.view import View
from featurebyte.api.window_validator import validate_window
from featurebyte.enum import AggFunc
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
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
        return [EventView, ItemView, ChangeView]

    @property
    def aggregation_method_name(self) -> str:
        return "aggregate_over"

    def aggregate_over(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        windows: Optional[List[Optional[str]]] = None,
        feature_names: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        feature_job_setting: Optional[FeatureJobSetting] = None,
        fill_value: OptionalScalar = None,
        skip_fill_na: Optional[bool] = None,
        offset: Optional[str] = None,
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
        groupby_node = add_pruning_sensitive_operation(
            graph=self.view.graph,
            node_cls=GroupByNode,
            node_params=node_params,
            input_node=self.view.node,
            operation_structure_info=self.view.operation_structure_info,
        )
        assert isinstance(feature_names, list)
        assert method is not None
        agg_method = construct_agg_func(agg_func=cast(AggFunc, method))

        items = []
        for feature_name in feature_names:
            feature = self._project_feature_from_groupby_node(
                agg_method=agg_method,
                feature_name=feature_name,
                groupby_node=groupby_node,
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
        method: Optional[str],
        windows: Optional[list[Optional[str]]],
        feature_names: Optional[list[str]],
        feature_job_setting: Optional[FeatureJobSetting],
        fill_value: OptionalScalar,
        skip_fill_na: bool,
        offset: Optional[str],
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

        if number_of_unbounded_windows > 0:
            if method != AggFunc.LATEST:
                raise ValueError('Unbounded window is only supported for the "latest" method')

            if self.category is not None:
                raise ValueError("category is not supported for aggregation with unbounded window")

        parsed_feature_job_setting = self._get_job_setting_params(feature_job_setting)
        if windows is not None:
            for window in windows:
                if window is not None:
                    validate_window(window, parsed_feature_job_setting.period)

        if offset is not None:
            validate_window(offset, parsed_feature_job_setting.period)

    def _get_job_setting_params(
        self, feature_job_setting: Optional[FeatureJobSetting]
    ) -> FeatureJobSetting:
        if feature_job_setting is not None:
            return feature_job_setting

        # Return default if no feature_job_setting is provided.
        default_setting = self.view.default_feature_job_setting
        if default_setting is None:
            raise ValueError(
                f"feature_job_setting is required as the {type(self.view).__name__} does not "
                "have a default feature job setting"
            )
        return cast(FeatureJobSetting, default_setting)

    def _prepare_node_parameters(
        self,
        value_column: Optional[str],
        method: Optional[str],
        windows: Optional[list[Optional[str]]],
        offset: Optional[str],
        feature_names: Optional[list[str]],
        timestamp_column: Optional[str] = None,
        value_by_column: Optional[str] = None,
        feature_job_setting: Optional[FeatureJobSetting] = None,
    ) -> dict[str, Any]:
        parsed_feature_job_setting = self._get_job_setting_params(feature_job_setting)
        tile_id_version = int(os.environ.get("FEATUREBYTE_TILE_ID_VERSION", "2"))
        return {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "value_by": value_by_column,
            "windows": windows,
            "offset": offset,
            "timestamp": timestamp_column or self.view.timestamp_column,
            "feature_job_setting": parsed_feature_job_setting.dict(),
            "names": feature_names,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
            "tile_id_version": tile_id_version,
        }
