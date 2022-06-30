"""
This module contains groupby related class
"""
from __future__ import annotations

from featurebyte.api.event_view import EventView
from featurebyte.api.feature import FeatureGroup
from featurebyte.common.feature_job_setting_validation import validate_job_setting_parameters
from featurebyte.core.mixin import OpsMixin
from featurebyte.enum import AggFunc, DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.util import get_tile_table_identifier


class EventViewGroupBy(OpsMixin):
    """
    EventViewGroupBy class
    """

    def __init__(self, obj: EventView, keys: str | list[str]):
        if not isinstance(obj, EventView):
            raise TypeError(f"Expect {EventView} object type!")

        keys_value = []
        if isinstance(keys, str):
            keys_value.append(keys)
        elif isinstance(keys, list):
            keys_value = keys
        else:
            raise TypeError(f'Grouping {obj} by "{keys}" is not supported!')

        for key in keys_value:
            if key not in obj.columns:
                raise KeyError(f'Column "{key}" not found!')
            if key not in obj.column_entity_map:
                raise ValueError(f'Column "{key}" is not an entity!')

        self.obj = obj
        self.keys = keys_value

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.obj}, keys={self.keys})"

    def __str__(self) -> str:
        return repr(self)

    def aggregate(
        self,
        value_column: str,
        method: str,
        windows: list[str],
        blind_spot: str,
        frequency: str,
        time_modulo_frequency: str,
        feature_names: list[str],
        timestamp_column: str | None = None,
        value_by_column: str | None = None,
    ) -> FeatureGroup:
        """
        Aggregate given value_column for each group specified in keys

        Parameters
        ----------
        value_column: str
            column to be aggregated
        method: str
            aggregation method
        windows: list[str]
            list of aggregation window sizes
        blind_spot: str
            historical gap introduced to the aggregation
        frequency: str
            frequency of the feature job
        time_modulo_frequency: str
            offset of when the feature job will be run, should be smaller than frequency
        feature_names: list[str]
            output feature names
        timestamp_column: str | None
            timestamp column used to specify the window (if not specified, event source timestamp is used)
        value_by_column: str | None
            use this column to further split the data within a group

        Returns
        -------
        FeatureGroup

        Raises
        ------
        ValueError
            If provided aggregation method is not supported
        KeyError
            If column to be aggregated does not exist
        """
        # pylint: disable=too-many-locals
        if method not in AggFunc.all():
            raise ValueError(f"Aggregation method not supported: {method}")

        parsed_seconds = validate_job_setting_parameters(
            frequency=frequency,
            time_modulo_frequency=time_modulo_frequency,
            blind_spot=blind_spot,
        )
        frequency_seconds, time_modulo_frequency_seconds, blind_spot_seconds = parsed_seconds

        if value_column not in self.obj.columns:
            raise KeyError(f'Column "{value_column}" not found in {self.obj}!')

        node_params = {
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
        }
        node = self.obj.graph.add_operation(
            node_type=NodeType.GROUPBY,
            node_params={
                **node_params,
                "tile_id": get_tile_table_identifier(
                    transformations_hash=self.obj.graph.node_name_to_ref[self.obj.node.name],
                    parameters=node_params,
                ),
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.obj.node],
        )
        column_var_type_map = {}
        column_lineage_map: dict[str, tuple[str, ...]] = {}
        for key in self.keys:
            column_var_type_map[key] = self.obj.column_var_type_map[key]
            column_lineage_map[key] = (node.name,)
        for column in feature_names:
            column_var_type_map[column] = DBVarType.FLOAT
            column_lineage_map[column] = (node.name,)

        return FeatureGroup(
            tabular_source=self.obj.tabular_source,
            node=node,
            column_var_type_map=column_var_type_map,
            column_lineage_map=column_lineage_map,
            row_index_lineage=(node.name,),
        )
