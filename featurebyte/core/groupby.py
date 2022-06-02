"""
This module contains groupby related class
"""
from __future__ import annotations

from featurebyte.core.event_source import EventSource
from featurebyte.core.feature import FeatureList
from featurebyte.core.operation import OpsMixin
from featurebyte.query_graph.enum import NodeOutputType, NodeType


class EventSourceGroupBy(OpsMixin):
    """
    EventSourceGroupBy class
    """

    def __init__(self, obj: EventSource, keys: str | list[str] | None):
        if not isinstance(obj, EventSource):
            raise TypeError(f"Expect {EventSource} object type!")

        keys_value = []
        if keys is None:
            if obj.entity_identifiers:
                keys_value = obj.entity_identifiers
            else:
                raise ValueError(f"Not able to infer keys from {obj}!")
        elif isinstance(keys, str):
            keys_value.append(keys)
        elif isinstance(keys, list):
            keys_value = keys
        else:
            raise TypeError(f"Grouping {obj} by '{keys}' is not supported!")

        for key in keys_value:
            if key not in obj.columns:
                raise KeyError(f"Column '{key}' not found in {obj}!")

        self.obj = obj
        self.keys = keys_value

    def __repr__(self) -> str:
        return f"EventSourceGroupBy({self.obj}, keys={self.keys})"

    def aggregate(
        self,
        value_column: str,
        method: str,
        value_by_column: str | None,
        windows: list[str],
        timestamp_column: str | None,
        blind_spot: int,
        window_end: str,
        schedule: str,
        feature_names: list[str],
    ) -> FeatureList:
        """

        Parameters
        ----------
        value_column
        method
        value_by_column
        windows
        timestamp_column
        blind_spot
        window_end
        schedule
        feature_names

        Returns
        -------

        """

        assert len(self.keys) == 1  # multi-keys not implemented
        assert value_by_column is None  # value by column not implemented
        assert len(windows) == len(feature_names)

        timestamp_column = timestamp_column or self.obj.timestamp_column

        node = self.obj.graph.add_operation(
            node_type=NodeType.GROUPBY,
            node_params={
                "key": self.keys[0],
                "parent": value_column,
                "agg_func": method,
                "window_end": window_end,
                "blind_spot": blind_spot,
                "timestamp": timestamp_column,
                "schedule": schedule,
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.obj.node],
        )

        return FeatureList(
            node=node,
            column_var_type_map=self.obj.column_var_type_map,  # fix me
            column_lineage_map=self.obj.column_lineage_map,  # fix me
            row_index_lineage=self._append_to_lineage(self.obj.row_index_lineage, node.name),
        )
