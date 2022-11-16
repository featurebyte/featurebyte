"""
This module contains groupby related class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from typeguard import typechecked

from featurebyte.api.agg_func import construct_agg_func
from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import BaseFeatureGroup, FeatureGroup
from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.core.mixin import OpsMixin
from featurebyte.enum import AggFunc, DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


class EventViewGroupBy(OpsMixin):
    """
    EventViewGroupBy class
    """

    @typechecked
    def __init__(self, obj: EventView, keys: Union[str, List[str]], category: Optional[str] = None):
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

    def _validate_parameters(
        self,
        value_column: Optional[str],
        method: Optional[str],
        windows: Optional[list[str]],
        feature_names: Optional[list[str]],
    ) -> None:
        if method is None:
            raise ValueError("method is required")

        if method not in AggFunc.all():
            raise ValueError(f"Aggregation method not supported: {method}")

        if method == AggFunc.COUNT:
            if value_column is not None:
                raise ValueError(
                    "Specifying value column is not allowed for COUNT aggregation;"
                    ' try aggregate(method="count", ...)'
                )
        else:
            if value_column is None:
                raise ValueError("value_column is required")
            if value_column not in self.obj.columns:
                raise KeyError(f'Column "{value_column}" not found in {self.obj}!')

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
        # pylint: disable=too-many-locals
        self._validate_parameters(
            value_column=value_column, method=method, windows=windows, feature_names=feature_names
        )

        feature_job_setting = feature_job_setting or {}
        frequency = feature_job_setting.get("frequency")
        time_modulo_frequency = feature_job_setting.get("time_modulo_frequency")
        blind_spot = feature_job_setting.get("blind_spot")
        default_setting = self.obj.default_feature_job_setting
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
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "value_by": value_by_column,
            "windows": windows,
            "timestamp": timestamp_column or self.obj.timestamp_column,
            "blind_spot": blind_spot_seconds,
            "time_modulo_frequency": time_modulo_frequency_seconds,
            "frequency": frequency_seconds,
            "names": feature_names,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
        }

    @typechecked
    def aggregate(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        windows: Optional[List[str]] = None,
        feature_names: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        feature_job_setting: Optional[Dict[str, str]] = None,
    ) -> FeatureGroup:
        """
        Aggregate given value_column for each group specified in keys

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

        Raises
        ------
        ValueError
            When the aggregation method does not support input variable type
        """
        # pylint: disable=too-many-locals
        node_params = self._prepare_node_parameters(
            method=method,
            value_column=value_column,
            windows=windows,
            feature_names=feature_names,
            timestamp_column=timestamp_column,
            value_by_column=self.category,
            feature_job_setting=feature_job_setting,
        )
        groupby_node = self.obj.graph.add_groupby_operation(
            node_params=node_params, input_node=self.obj.node
        )
        items: list[Feature | BaseFeatureGroup] = []
        assert isinstance(feature_names, list)
        assert method is not None
        agg_method = construct_agg_func(agg_func=method)
        for feature_name in feature_names:
            feature_node = self.obj.graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": [feature_name]},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[groupby_node],
            )
            # value_column is None for count-like aggregation method
            input_var_type = self.obj.column_var_type_map.get(value_column, DBVarType.FLOAT)  # type: ignore
            if self.category:
                var_type = DBVarType.OBJECT
            else:
                if input_var_type not in agg_method.input_output_var_type_map:
                    raise ValueError(
                        f'Aggregation method "{method}" does not support "{input_var_type}" input variable'
                    )
                var_type = agg_method.input_output_var_type_map[input_var_type]

            feature = Feature(
                name=feature_name,
                feature_store=self.obj.feature_store,
                tabular_source=self.obj.tabular_source,
                node_name=feature_node.name,
                dtype=var_type,
                row_index_lineage=(groupby_node.name,),
                tabular_data_ids=self.obj.tabular_data_ids,
                entity_ids=self.entity_ids,
            )
            # Count features should be 0 instead of NaN when there are no records
            if method in {AggFunc.COUNT, AggFunc.NA_COUNT} and self.category is None:
                feature.fillna(0)
                feature.name = feature_name
            items.append(feature)
        feature_group = FeatureGroup(items)
        return feature_group
