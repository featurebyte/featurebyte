"""
This module contains groupby related class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from typeguard import typechecked

from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import BaseFeatureGroup, FeatureGroup
from featurebyte.api.util import get_entity_by_id
from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.core.mixin import OpsMixin
from featurebyte.enum import AggFunc, DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, Node
from featurebyte.query_graph.util import get_aggregation_identifier, get_tile_table_identifier


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

        serving_names = []
        for key in keys_value:
            if key not in obj.columns:
                raise KeyError(f'Column "{key}" not found!')
            if key not in (obj.column_entity_map or {}):
                raise ValueError(f'Column "{key}" is not an entity!')
            assert obj.column_entity_map is not None
            entity_id = obj.column_entity_map[key]
            entity = get_entity_by_id(str(entity_id))
            serving_names.append(entity["serving_names"][0])

        if category is not None and category not in obj.columns:
            raise KeyError(f'Column "{category}" not found!')

        self.obj = obj
        self.keys = keys_value
        self.category = category
        self.serving_names = serving_names

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.obj}, keys={self.keys})"

    def __str__(self) -> str:
        return repr(self)

    def _prepare_node_parameters(
        self,
        value_column: str,
        method: str,
        windows: list[str],
        feature_names: list[str],
        timestamp_column: str | None = None,
        value_by_column: str | None = None,
        feature_job_setting: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        # pylint: disable=too-many-locals
        if method not in AggFunc.all():
            raise ValueError(f"Aggregation method not supported: {method}")

        if value_column not in self.obj.columns:
            raise KeyError(f'Column "{value_column}" not found in {self.obj}!')

        if len(windows) != len(feature_names):
            raise ValueError(
                "Window length must be the same as the number of output feature names."
            )

        if len(windows) != len(set(feature_names)) or len(set(windows)) != len(feature_names):
            raise ValueError("Window sizes or feature names contains duplicated value(s).")

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
        }

    def _prepare_node_and_column_metadata(
        self, node_params: dict[str, Any], tile_id: str | None, aggregation_id: str | None
    ) -> tuple[Node, dict[str, DBVarType], dict[str, tuple[str, ...]]]:
        """
        Insert a groupby node into global graph & return the node and some column metadata

        Parameters
        ----------
        node_params: dict[str, Any]
            Groupby node parameters
        tile_id: str | None
            Tile ID
        aggregation_id: str | None
            Aggregation ID

        Returns
        -------
        Node
            Newly created groupby node
        dict[str, DBVarType]
            Column to DBVarType mapping
        dict[str, tuple[str, ...]]
            Column to lineage mapping
        """
        node = self.obj.graph.add_operation(
            node_type=NodeType.GROUPBY,
            node_params={**node_params, "tile_id": tile_id, "aggregation_id": aggregation_id},
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.obj.node],
        )
        column_var_type_map = {}
        column_lineage_map: dict[str, tuple[str, ...]] = {}
        for key in self.keys:
            column_var_type_map[key] = self.obj.column_var_type_map[key]
            column_lineage_map[key] = (node.name,)
        for column in node_params["names"]:
            column_var_type_map[column] = DBVarType.FLOAT
            column_lineage_map[column] = (node.name,)
        return node, column_var_type_map, column_lineage_map

    @typechecked
    def aggregate(
        self,
        value_column: str,
        method: str,
        windows: List[str],
        feature_names: List[str],
        timestamp_column: Optional[str] = None,
        feature_job_setting: Optional[Dict[str, str]] = None,
    ) -> FeatureGroup:
        """
        Aggregate given value_column for each group specified in keys

        Parameters
        ----------
        value_column: str
            Column to be aggregated
        method: str
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
        # pylint: disable=too-many-locals
        node_params = self._prepare_node_parameters(
            value_column,
            method,
            windows,
            feature_names,
            timestamp_column,
            self.category,
            feature_job_setting,
        )
        # To generate a consistent tile_id before & after pruning, insert a groupby node to
        # global query graph first, then used the inserted groupby node to prune the graph &
        # generate updated tile id. Finally, insert a new groupby node into the graph (actual
        # groupby node to be used).
        temp_groupby_node, column_var_type_map, _ = self._prepare_node_and_column_metadata(
            node_params, None, None
        )
        pruned_graph, node_name_map = GlobalQueryGraph().prune(
            target_node=temp_groupby_node,
            target_columns=set(column_var_type_map.keys()),
        )
        input_nodes = pruned_graph.backward_edges[node_name_map[temp_groupby_node.name]]
        table_details_dict = self.obj.tabular_source[1].dict()
        tile_id = get_tile_table_identifier(
            table_details_dict=table_details_dict,
            parameters=node_params,
        )
        aggregation_id = get_aggregation_identifier(
            transformations_hash=pruned_graph.node_name_to_ref[input_nodes[0]],
            parameters=node_params,
        )
        (
            groupby_node,
            column_var_type_map,
            column_lineage_map,
        ) = self._prepare_node_and_column_metadata(node_params, tile_id, aggregation_id)

        items: list[Feature | BaseFeatureGroup] = []
        for feature_name in feature_names:
            feature_node = self.obj.graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": [feature_name]},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[groupby_node],
            )
            if method in {AggFunc.COUNT, AggFunc.NA_COUNT} and self.category is not None:
                var_type = DBVarType.OBJECT
            else:
                var_type = column_var_type_map[feature_name]
            feature = Feature(
                name=feature_name,
                feature_store=self.obj.feature_store,
                tabular_source=self.obj.tabular_source,
                node=feature_node,
                var_type=var_type,
                lineage=self._append_to_lineage(
                    column_lineage_map[feature_name], feature_node.name
                ),
                row_index_lineage=(groupby_node.name,),
                event_data_ids=[self.obj.event_data_id],
            )
            # Count features should be 0 instead of NaN when there are no records
            if method in {AggFunc.COUNT, AggFunc.NA_COUNT} and self.category is None:
                feature.fillna(0)
                feature.name = feature_name
            items.append(feature)
        feature_group = FeatureGroup(items)
        return feature_group
