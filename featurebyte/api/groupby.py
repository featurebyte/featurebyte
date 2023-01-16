"""
This module contains groupby related class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Type, Union, cast

from abc import ABC, abstractmethod

from typeguard import typechecked

from featurebyte.api.change_view import ChangeView
from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import FeatureGroup
from featurebyte.api.item_view import ItemView
from featurebyte.api.scd_view import SlowlyChangingView
from featurebyte.api.view import View
from featurebyte.api.window_validator import validate_window
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.model_util import validate_offset_string
from featurebyte.common.typing import OptionalScalar, get_or_default
from featurebyte.enum import AggFunc, DBVarType
from featurebyte.exception import AggregationNotSupportedForViewError
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.agg_func import AggFuncType, construct_agg_func
from featurebyte.query_graph.transform.reconstruction import (
    GroupbyNode,
    ItemGroupbyNode,
    add_pruning_sensitive_operation,
)


class BaseAggregator(ABC):
    """
    BaseAggregator is the base class for aggregators in groupby
    """

    def __init__(
        self,
        view: View,
        category: Optional[str],
        entity_ids: list[PydanticObjectId],
        keys: list[str],
        serving_names: list[str],
    ):
        self.view = view
        self.category = category
        self.entity_ids = entity_ids
        self.keys = keys
        self.serving_names = serving_names
        if not isinstance(self.view, tuple(self.supported_views)):
            supported_views_formatted = ", ".join(
                [view_cls.__name__ for view_cls in self.supported_views]
            )
            raise AggregationNotSupportedForViewError(
                f"{self.aggregation_method_name}() is only available for {supported_views_formatted}"
            )

    @property
    @abstractmethod
    def supported_views(self) -> List[Type[View]]:
        """
        Views that support this type of aggregation

        Returns
        -------
        List[Type[View]]
        """

    @property
    @abstractmethod
    def aggregation_method_name(self) -> str:
        """
        Aggregation method name for readable error message

        Returns
        -------
        str
        """

    def _validate_method_and_value_column(
        self, method: Optional[str], value_column: Optional[str]
    ) -> None:
        if method is None:
            raise ValueError("method is required")

        if method not in AggFunc.all():
            raise ValueError(f"Aggregation method not supported: {method}")

        if method == AggFunc.COUNT:
            if value_column is not None:
                raise ValueError(
                    "Specifying value column is not allowed for COUNT aggregation;"
                    " try setting None as the value_column"
                )
        else:
            if value_column is None:
                raise ValueError("value_column is required")
            if value_column not in self.view.columns:
                raise KeyError(f'Column "{value_column}" not found in {self.view}!')

    @staticmethod
    def _validate_fill_value_and_skip_fill_na(
        fill_value: OptionalScalar, skip_fill_na: bool
    ) -> None:
        if fill_value is not None and skip_fill_na:
            raise ValueError(
                "Specifying both fill_value and skip_fill_na is not allowed;"
                " try setting fill_value to None or skip_fill_na to False"
            )

    def _project_feature_from_groupby_node(
        self,
        agg_method: AggFuncType,
        feature_name: str,
        groupby_node: Node,
        method: str,
        value_column: Optional[str],
        fill_value: OptionalScalar,
        skip_fill_na: bool,
    ) -> Feature:

        # value_column is None for count-like aggregation method
        input_var_type = self.view.column_var_type_map.get(value_column, DBVarType.FLOAT)  # type: ignore
        if not agg_method.is_var_type_supported(input_var_type):
            raise ValueError(
                f'Aggregation method "{method}" does not support "{input_var_type}" input variable'
            )

        var_type = agg_method.derive_output_var_type(
            input_var_type=input_var_type, category=self.category
        )

        feature = self.view._project_feature_from_node(  # pylint: disable=protected-access
            node=groupby_node,
            feature_name=feature_name,
            feature_dtype=var_type,
            entity_ids=self.entity_ids,
        )
        if not skip_fill_na:
            self._fill_feature(feature, method, feature_name, fill_value)
        return feature

    def _fill_feature(
        self,
        feature: Feature,
        method: str,
        feature_name: str,
        fill_value: OptionalScalar,
    ) -> Feature:
        """
        Fill feature values as needed.

        Parameters
        ----------
        feature: Feature
            feature
        method: str
            aggregation method
        feature_name: str
            feature name
        fill_value: OptionalScalar
            value to fill

        Returns
        -------
        Feature

        Raises
        ------
        ValueError
            If both fill_value and category parameters are specified
        """
        if fill_value is not None and self.category is not None:
            raise ValueError("fill_value is not supported for aggregation per category")

        if method in {AggFunc.COUNT, AggFunc.NA_COUNT} and self.category is None:
            # Count features should be 0 instead of NaN when there are no records
            value_to_fill = get_or_default(fill_value, 0)
            feature.fillna(value_to_fill)
            feature.name = feature_name
        elif fill_value is not None:
            feature.fillna(fill_value)
            feature.name = feature_name

        return feature


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
        feature_job_setting: Optional[Dict[str, str]] = None,
        fill_value: OptionalScalar = None,
        skip_fill_na: bool = False,
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
            Timestamp column used to specify the window (if not specified, event data timestamp is used)
        feature_job_setting: Optional[Dict[str, str]]
            Dictionary contains `blind_spot`, `frequency` and `time_modulo_frequency` keys which are
            feature job setting parameters
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: bool
            Whether to skip filling NaN values

        Returns
        -------
        FeatureGroup
        """

        self._validate_parameters(
            value_column=value_column,
            method=method,
            windows=windows,
            feature_names=feature_names,
            feature_job_setting=feature_job_setting,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
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
        )
        groupby_node = add_pruning_sensitive_operation(
            graph=self.view.graph,
            node_cls=GroupbyNode,
            node_params=node_params,
            input_node=self.view.node,
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
        feature_job_setting: Optional[Dict[str, str]],
        fill_value: OptionalScalar,
        skip_fill_na: bool,
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

        if windows is not None:
            parsed_feature_job_setting = self._get_job_setting_params(feature_job_setting)
            for window in windows:
                if window is not None:
                    validate_window(window, parsed_feature_job_setting.frequency)

    def _get_job_setting_params(
        self, feature_job_setting: Optional[dict[str, str]]
    ) -> FeatureJobSetting:
        feature_job_setting = feature_job_setting or {}
        frequency = feature_job_setting.get("frequency")
        time_modulo_frequency = feature_job_setting.get("time_modulo_frequency")
        blind_spot = feature_job_setting.get("blind_spot")

        # Check that feature job setting is not specified partially (must be all or nothing)
        settings_overridden = [
            frequency is not None,
            time_modulo_frequency is not None,
            blind_spot is not None,
        ]
        is_settings_provided = any(settings_overridden)
        if is_settings_provided and not all(settings_overridden):
            raise ValueError(
                "All of frequency, time_modulo_frequency and blind_spot must be specified in"
                " feature_job_setting"
            )

        if not is_settings_provided:
            default_setting = self.view.default_feature_job_setting
            if default_setting is None:
                raise ValueError(
                    f"feature_job_setting is required as the {type(self.view).__name__} does not "
                    "have a default feature job setting"
                )
            frequency = default_setting.frequency
            time_modulo_frequency = default_setting.time_modulo_frequency
            blind_spot = default_setting.blind_spot

        return FeatureJobSetting(
            frequency=frequency,
            time_modulo_frequency=time_modulo_frequency,
            blind_spot=blind_spot,
        )

    def _prepare_node_parameters(
        self,
        value_column: Optional[str],
        method: Optional[str],
        windows: Optional[list[Optional[str]]],
        feature_names: Optional[list[str]],
        timestamp_column: Optional[str] = None,
        value_by_column: Optional[str] = None,
        feature_job_setting: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:

        parsed_feature_job_setting = self._get_job_setting_params(feature_job_setting)
        return {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "value_by": value_by_column,
            "windows": windows,
            "timestamp": timestamp_column or self.view.timestamp_column,
            "blind_spot": parsed_feature_job_setting.blind_spot_seconds,
            "time_modulo_frequency": parsed_feature_job_setting.time_modulo_frequency_seconds,
            "frequency": parsed_feature_job_setting.frequency_seconds,
            "names": feature_names,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
        }


class AsAtAggregator(BaseAggregator):
    """
    AsAtAggregator implements the aggregate_asat method for GroupBy
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [SlowlyChangingView]

    @property
    def aggregation_method_name(self) -> str:
        return "aggregate_asat"

    @typechecked
    def aggregate_asat(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        feature_name: Optional[str] = None,
        offset: Optional[str] = None,
        backward: bool = True,
        fill_value: OptionalScalar = None,
        skip_fill_na: bool = False,
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
        skip_fill_na: bool
            Whether to skip filling na values

        Returns
        -------
        Feature
        """
        self._validate_parameters(
            method=method,
            value_column=value_column,
            feature_name=feature_name,
            offset=offset,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )

        view = cast(SlowlyChangingView, self.view)
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
            **view.get_common_scd_parameters().dict(),
        }
        groupby_node = self.view.graph.add_operation(
            node_type=NodeType.AGGREGATE_AS_AT,
            node_params=node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.view.node],
        )

        assert method is not None
        assert feature_name is not None
        agg_method = construct_agg_func(agg_func=cast(AggFunc, method))

        return self._project_feature_from_groupby_node(
            agg_method=agg_method,
            feature_name=feature_name,
            groupby_node=groupby_node,
            method=method,
            value_column=value_column,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )

    def _validate_parameters(
        self,
        method: Optional[str],
        feature_name: Optional[str],
        value_column: Optional[str],
        offset: Optional[str],
        fill_value: OptionalScalar,
        skip_fill_na: bool,
    ) -> None:

        self._validate_method_and_value_column(method=method, value_column=value_column)
        self._validate_fill_value_and_skip_fill_na(fill_value=fill_value, skip_fill_na=skip_fill_na)

        if method == AggFunc.LATEST:
            raise ValueError("latest aggregation method is not supported for aggregated_asat")

        if feature_name is None:
            raise ValueError("feature_name is required")

        if self.category is not None:
            raise ValueError("category is not supported for aggregate_asat")

        view = cast(SlowlyChangingView, self.view)
        for key in self.keys:
            if key == view.natural_key_column:
                raise ValueError(
                    "Natural key column cannot be used as a groupby key in aggregate_asat"
                )

        if offset is not None:
            validate_offset_string(offset)
            raise NotImplementedError("offset support is not yet implemented")


class SimpleAggregator(BaseAggregator):
    """
    SimpleAggregator implements the aggregate method for GroupBy
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [ItemView]

    @property
    def aggregation_method_name(self) -> str:
        return "aggregate"

    def aggregate(
        self,
        value_column: Optional[str] = None,
        method: Optional[AggFunc] = None,
        feature_name: Optional[str] = None,
        fill_value: OptionalScalar = None,
        skip_fill_na: bool = False,
    ) -> Feature:
        """
        Aggregate given value_column for each group specified in keys, without time windows

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: Optional[AggFunc]
            Aggregation method
        feature_name: str
            Output feature name
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: bool
            Whether to skip filling NaN values

        Returns
        -------
        Feature
        """
        self._validate_method_and_value_column(method=method, value_column=value_column)
        self._validate_fill_value_and_skip_fill_na(fill_value=fill_value, skip_fill_na=skip_fill_na)
        self.view.validate_simple_aggregate_parameters(
            keys=self.keys,
            value_column=value_column,
        )

        node_params = {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "value_by": self.category,
            "name": feature_name,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
        }
        groupby_node = add_pruning_sensitive_operation(
            graph=self.view.graph,
            node_cls=ItemGroupbyNode,
            node_params=node_params,
            input_node=self.view.node,
        )

        assert method is not None
        assert feature_name is not None
        agg_method = construct_agg_func(agg_func=method)
        feature = self._project_feature_from_groupby_node(
            agg_method=agg_method,
            feature_name=feature_name,
            groupby_node=groupby_node,
            method=method,
            value_column=value_column,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )
        return feature


class GroupBy:
    """
    GroupBy class that is applicable to EventView, ItemView, and ChangeView
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["GroupBy"])

    @typechecked
    def __init__(
        self,
        obj: Union[EventView, ItemView, ChangeView, SlowlyChangingView],
        keys: Union[str, List[str]],
        category: Optional[str] = None,
    ):
        keys_value = []
        if isinstance(keys, str):
            keys_value.append(keys)
        elif isinstance(keys, list):
            keys_value = keys

        # construct column name entity mapping
        columns_info = obj.columns_info
        column_entity_map = {col.name: col.entity_id for col in columns_info if col.entity_id}

        # construct serving_names
        serving_names = []
        entity_ids = []
        for key in keys_value:
            if key not in obj.columns:
                raise KeyError(f'Column "{key}" not found!')
            if key not in column_entity_map:
                raise ValueError(f'Column "{key}" is not an entity!')

            entity = Entity.get_by_id(column_entity_map[key])
            serving_names.append(entity.serving_name)
            entity_ids.append(entity.id)

        if category is not None and category not in obj.columns:
            raise KeyError(f'Column "{category}" not found!')

        self.view_obj = obj
        self.keys = keys_value
        self.category = category
        self.serving_names = serving_names
        self.entity_ids = entity_ids

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.view_obj}, keys={self.keys})"

    def __str__(self) -> str:
        return repr(self)

    @typechecked
    def aggregate_over(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        windows: Optional[List[Optional[str]]] = None,
        feature_names: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        feature_job_setting: Optional[Dict[str, str]] = None,
        fill_value: OptionalScalar = None,
        skip_fill_na: bool = False,
    ) -> FeatureGroup:
        """
        Aggregate given value_column for each group specified in keys over a list of time windows

        This aggregation is available to EventView, ItemView, and ChangeView.

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: str
            Aggregation method
        windows: List[str]
            List of aggregation window sizes. Use `None` to indicated unbounded window size (only
            applicable to "latest" method). Format of a window size is "{size}{unit}",
            where size is a positive integer and unit is one of the following:

            "ns": nanosecond
            "us": microsecond
            "ms": millisecond
            "s": second
            "m": minute
            "h": hour
            "d": day
            "w": week

            **Note**: Window sizes must be multiples of feature job frequency

        feature_names: List[str]
            Output feature names
        timestamp_column: Optional[str]
            Timestamp column used to specify the window (if not specified, event data timestamp is used)
        feature_job_setting: Optional[Dict[str, str]]
            Dictionary contains `blind_spot`, `frequency` and `time_modulo_frequency` keys which are
            feature job setting parameters
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: bool
            Whether to skip filling NaN values

        Returns
        -------
        FeatureGroup

        Examples
        --------
        >>> import featurebyte as fb
        >>> features = cc_transactions.groupby("AccountID").aggregate_over(  # doctest: +SKIP
        ...    "Amount",
        ...    method=fb.AggFunc.SUM,
        ...    windows=["7d", "30d"],
        ...    feature_names=["Total spend 7d", "Total spend 30d"],
        ... )
        >>> features.feature_names  # doctest: +SKIP
        ['Total spend 7d', 'Total spend 30d']

        See Also
        --------
        - [FeatureGroup](/reference/featurebyte.api.feature_list.FeatureGroup/): FeatureGroup object
        - [Feature](/reference/featurebyte.api.feature.Feature/): Feature object
        """
        return WindowAggregator(
            self.view_obj, self.category, self.entity_ids, self.keys, self.serving_names
        ).aggregate_over(
            value_column=value_column,
            method=method,
            windows=windows,
            feature_names=feature_names,
            timestamp_column=timestamp_column,
            feature_job_setting=feature_job_setting,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )

    @typechecked
    def aggregate_asat(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        feature_name: Optional[str] = None,
        offset: Optional[str] = None,
        backward: bool = True,
        fill_value: OptionalScalar = None,
        skip_fill_na: bool = False,
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
        skip_fill_na: bool
            Whether to skip filling NaN values

        Returns
        -------
        Feature

        Examples
        --------
        >>> import featurebyte as fb
        >>> feature = credit_card_accounts.groupby("CustomerID").aggregate_asat(  # doctest: +SKIP
        ...    method=fb.AggFunc.COUNT,
        ...    feature_name="Number of Credit Cards",
        ... )
        >>> feature  # doctest: +SKIP
        Feature[FLOAT](name=Number of Credit Cards, node_name=alias_1)
        """
        return AsAtAggregator(
            self.view_obj, self.category, self.entity_ids, self.keys, self.serving_names
        ).aggregate_asat(
            value_column=value_column,
            method=method,
            feature_name=feature_name,
            offset=offset,
            backward=backward,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )

    @typechecked
    def aggregate(
        self,
        value_column: Optional[str] = None,
        method: Optional[AggFunc] = None,
        feature_name: Optional[str] = None,
        fill_value: OptionalScalar = None,
        skip_fill_na: bool = False,
    ) -> Feature:
        """
        Aggregate given value_column for each group specified in keys, without time windows

        This aggregation is available to ItemView.

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: Optional[AggFunc]
            Aggregation method
        feature_name: Optional[str]
            Output feature name
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: bool
            Whether to skip filling NaN values

        Returns
        -------
        Feature
        """
        return SimpleAggregator(
            self.view_obj, self.category, self.entity_ids, self.keys, self.serving_names
        ).aggregate(
            value_column=value_column,
            method=method,
            feature_name=feature_name,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )
