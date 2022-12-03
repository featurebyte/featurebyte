"""
This module contains groupby related class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Type, Union

from abc import ABC, abstractmethod

from typeguard import typechecked

from featurebyte.api.agg_func import AggFuncType, construct_agg_func
from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import BaseFeatureGroup, FeatureGroup
from featurebyte.api.item_view import ItemView
from featurebyte.api.view import View
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.core.mixin import OpsMixin
from featurebyte.enum import AggFunc, DBVarType
from featurebyte.exception import AggregationNotSupportedForViewError
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.transformation import GraphReconstructor, GroupbyNode, ItemGroupbyNode


class BaseAggregator(ABC):
    """
    BaseAggregator is the base class for aggregators in groupby
    """

    def __init__(self, groupby_obj: "GroupBy"):
        self.groupby_obj = groupby_obj
        if not isinstance(self.view, tuple(self.supported_views)):
            supported_views_formatted = ", ".join(
                [view_cls.__name__ for view_cls in self.supported_views]
            )
            raise AggregationNotSupportedForViewError(
                f"{self.aggregation_method_name}() is only available for {supported_views_formatted}"
            )

    @property
    def view(self) -> View:
        """
        Returns the underlying View object that the groupby operates on

        Returns
        -------
        View
        """
        return self.groupby_obj.obj

    @property
    def groupby(self) -> "GroupBy":
        """
        Returns the associated GroupBy object

        Returns
        -------
        GroupBy
        """
        return self.groupby_obj

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

    def _project_feature_from_groupby_node(
        self,
        agg_method: AggFuncType,
        feature_name: str,
        groupby_node: Node,
        method: str,
        value_column: Optional[str],
    ) -> Feature:
        feature_node = self.view.graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [feature_name]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[groupby_node],
        )
        # value_column is None for count-like aggregation method
        input_var_type = self.view.column_var_type_map.get(value_column, DBVarType.FLOAT)  # type: ignore
        if self.groupby.category:
            var_type = DBVarType.OBJECT
        else:
            if input_var_type not in agg_method.input_output_var_type_map:
                raise ValueError(
                    f'Aggregation method "{method}" does not support "{input_var_type}" input variable'
                )
            var_type = agg_method.input_output_var_type_map[input_var_type]
        feature = Feature(
            name=feature_name,
            feature_store=self.view.feature_store,
            tabular_source=self.view.tabular_source,
            node_name=feature_node.name,
            dtype=var_type,
            row_index_lineage=(groupby_node.name,),
            tabular_data_ids=self.view.tabular_data_ids,
            entity_ids=self.groupby.entity_ids,
        )
        # Count features should be 0 instead of NaN when there are no records
        if method in {AggFunc.COUNT, AggFunc.NA_COUNT} and self.groupby.category is None:
            feature.fillna(0)
            feature.name = feature_name
        return feature


class WindowAggregator(BaseAggregator):
    """
    WindowAggregator implements the aggregate_over method for GroupBy
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [EventView, ItemView]

    @property
    def aggregation_method_name(self) -> str:
        return "aggregate_over"

    def aggregate_over(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        windows: Optional[List[str]] = None,
        feature_names: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        feature_job_setting: Optional[Dict[str, str]] = None,
    ) -> FeatureGroup:
        """
        Aggregate given value_column for each group specified in keys over a list of time windows

        This aggregation is available to EventView and ItemView.

        Parameters
        ----------
        value_column: str
            Column to be aggregated
        method: Optional[AggFunc]
            Aggregation method
        windows: List[str]
            List of aggregation window sizes
        feature_names: List[str]
            Output feature names
        timestamp_column: Optional[str]
            Timestamp column used to specify the window (if not specified, event data timestamp is used)
        feature_job_setting: Optional[Dict[str, str]]
            Dictionary contains `blind_spot`, `frequency` and `time_modulo_frequency` keys which are
            feature job setting parameters

        Returns
        -------
        FeatureGroup
        """

        self._validate_parameters(
            value_column=value_column, method=method, windows=windows, feature_names=feature_names
        )
        self.view.validate_aggregation_parameters(
            groupby_obj=self.groupby,
            value_column=value_column,
        )

        node_params = self._prepare_node_parameters(
            value_column=value_column,
            method=method,
            windows=windows,
            feature_names=feature_names,
            timestamp_column=timestamp_column,
            value_by_column=self.groupby.category,
            feature_job_setting=feature_job_setting,
        )
        groupby_node = GraphReconstructor.add_pruning_sensitive_operation(
            graph=self.view.graph,
            node_cls=GroupbyNode,
            node_params=node_params,
            input_node=self.view.node,
        )
        assert isinstance(feature_names, list)
        assert method is not None
        agg_method = construct_agg_func(agg_func=method)

        items: List[Union[Feature, BaseFeatureGroup]] = []
        for feature_name in feature_names:
            feature = self._project_feature_from_groupby_node(
                agg_method=agg_method,
                feature_name=feature_name,
                groupby_node=groupby_node,
                method=method,
                value_column=value_column,
            )
            items.append(feature)
        feature_group = FeatureGroup(items)
        return feature_group

    def _validate_parameters(
        self,
        value_column: Optional[str],
        method: Optional[str],
        windows: Optional[list[str]],
        feature_names: Optional[list[str]],
    ) -> None:

        self._validate_method_and_value_column(method=method, value_column=value_column)

        if not isinstance(windows, list):
            raise ValueError(f"windows is required and should be a list; got {windows}")

        if not isinstance(feature_names, list):
            raise ValueError(f"feature_names is required and should be a list; got {feature_names}")

        if len(windows) != len(feature_names):
            raise ValueError(
                "Window length must be the same as the number of output feature names."
            )

        if len(windows) != len(set(feature_names)) or len(set(windows)) != len(feature_names):
            raise ValueError("Window sizes or feature names contains duplicated value(s).")

    def _prepare_node_parameters(
        self,
        value_column: Optional[str],
        method: Optional[str],
        windows: Optional[list[str]],
        feature_names: Optional[list[str]],
        timestamp_column: Optional[str] = None,
        value_by_column: Optional[str] = None,
        feature_job_setting: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:

        feature_job_setting = feature_job_setting or {}
        frequency = feature_job_setting.get("frequency")
        time_modulo_frequency = feature_job_setting.get("time_modulo_frequency")
        blind_spot = feature_job_setting.get("blind_spot")
        default_setting = self.view.default_feature_job_setting
        if default_setting:
            frequency = frequency or default_setting.frequency
            time_modulo_frequency = time_modulo_frequency or default_setting.time_modulo_frequency
            blind_spot = blind_spot or default_setting.blind_spot

        if frequency is None or time_modulo_frequency is None or blind_spot is None:
            raise ValueError(
                "frequency, time_module_frequency and blind_spot parameters should not be None!"
            )

        parsed_seconds = validate_job_setting_parameters(
            frequency=frequency,
            time_modulo_frequency=time_modulo_frequency,
            blind_spot=blind_spot,
        )
        frequency_seconds, time_modulo_frequency_seconds, blind_spot_seconds = parsed_seconds
        return {
            "keys": self.groupby.keys,
            "parent": value_column,
            "agg_func": method,
            "value_by": value_by_column,
            "windows": windows,
            "timestamp": timestamp_column or self.view.timestamp_column,
            "blind_spot": blind_spot_seconds,
            "time_modulo_frequency": time_modulo_frequency_seconds,
            "frequency": frequency_seconds,
            "names": feature_names,
            "serving_names": self.groupby.serving_names,
            "entity_ids": self.groupby.entity_ids,
        }


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
        method: Optional[str] = None,
        feature_name: Optional[str] = None,
    ) -> Feature:
        """
        Aggregate given value_column for each group specified in keys, without time windows

        This aggregation is available to ItemView.

        Parameters
        ----------
        value_column: str
            Column to be aggregated
        method: Optional[AggFunc]
            Aggregation method
        feature_name: List[str]
            Output feature name

        Returns
        -------
        Feature
        """
        self._validate_method_and_value_column(method=method, value_column=value_column)
        self.view.validate_aggregation_parameters(
            groupby_obj=self.groupby,
            value_column=value_column,
        )

        node_params = {
            "keys": self.groupby.keys,
            "parent": value_column,
            "agg_func": method,
            "value_by": self.groupby.category,
            "names": [feature_name],
            "serving_names": self.groupby.serving_names,
            "entity_ids": self.groupby.entity_ids,
        }
        groupby_node = GraphReconstructor.add_pruning_sensitive_operation(
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
        )
        return feature


class GroupBy(OpsMixin):
    """
    GroupBy class that is applicable to EventView and ItemView
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["GroupBy"])

    @typechecked
    def __init__(
        self,
        obj: Union[EventView, ItemView],
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

        self.obj = obj
        self.keys = keys_value
        self.category = category
        self.serving_names = serving_names
        self.entity_ids = entity_ids

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.obj}, keys={self.keys})"

    def __str__(self) -> str:
        return repr(self)

    @typechecked
    def aggregate_over(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        windows: Optional[List[str]] = None,
        feature_names: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        feature_job_setting: Optional[Dict[str, str]] = None,
    ) -> FeatureGroup:
        """
        Aggregate given value_column for each group specified in keys over a list of time windows

        This aggregation is available to EventView and ItemView.

        Parameters
        ----------
        value_column: str
            Column to be aggregated
        method: Optional[AggFunc]
            Aggregation method
        windows: List[str]
            List of aggregation window sizes
        feature_names: List[str]
            Output feature names
        timestamp_column: Optional[str]
            Timestamp column used to specify the window (if not specified, event data timestamp is used)
        feature_job_setting: Optional[Dict[str, str]]
            Dictionary contains `blind_spot`, `frequency` and `time_modulo_frequency` keys which are
            feature job setting parameters

        Returns
        -------
        FeatureGroup
        """
        return WindowAggregator(self).aggregate_over(
            value_column=value_column,
            method=method,
            windows=windows,
            feature_names=feature_names,
            timestamp_column=timestamp_column,
            feature_job_setting=feature_job_setting,
        )

    @typechecked
    def aggregate(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        feature_name: Optional[str] = None,
    ) -> Feature:
        """
        Aggregate given value_column for each group specified in keys, without time windows

        This aggregation is available to ItemView.

        Parameters
        ----------
        value_column: str
            Column to be aggregated
        method: Optional[AggFunc]
            Aggregation method
        feature_name: List[str]
            Output feature name

        Returns
        -------
        Feature
        """
        return SimpleAggregator(self).aggregate(
            value_column=value_column,
            method=method,
            feature_name=feature_name,
        )
