"""
EventView class
"""
from __future__ import annotations

from typing import Any, List, Optional, Union, cast

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.event_data import EventData
from featurebyte.api.validator import validate_view
from featurebyte.api.view import GroupByMixin, View, ViewColumn
from featurebyte.enum import TableDataType
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.generic import InputNode


class EventViewColumn(ViewColumn):
    """
    EventViewColumn class
    """

    # documentation metadata
    __fbautodoc__: List[str] = ["Column"]

    @typechecked
    def lag(self, entity_columns: Union[str, List[str]], offset: int = 1) -> EventViewColumn:
        """
        Lag operation

        Parameters
        ----------
        entity_columns : str | list[str]
            Entity columns used when retrieving the lag value
        offset : int
            The number of rows backward from which to retrieve a value. Default is 1.

        Returns
        -------
        EventViewColumn

        Raises
        ------
        ValueError
            If a lag operation has already been applied to the column
        """
        if not isinstance(entity_columns, list):
            entity_columns = [entity_columns]
        if NodeType.LAG in self.node_types_lineage:
            raise ValueError("lag can only be applied once per column")
        assert self._parent is not None

        timestamp_column = self._parent.timestamp_column
        required_columns = entity_columns + [timestamp_column]
        input_nodes = [self.node]
        for col in required_columns:
            input_nodes.append(self._parent[col].node)

        node = self.graph.add_operation(
            node_type=NodeType.LAG,
            node_params={
                "entity_columns": entity_columns,
                "timestamp_column": self._parent.timestamp_column,
                "offset": offset,
            },
            node_output_type=NodeOutputType.SERIES,
            input_nodes=input_nodes,
        )
        return type(self)(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            node_name=node.name,
            name=None,
            dtype=self.dtype,
            row_index_lineage=self.row_index_lineage,
            **self.unary_op_series_params(),
        )


class EventView(View, GroupByMixin):
    """
    EventView class
    """

    # documentation metadata
    __fbautodoc__: List[str] = ["View"]

    _series_class = EventViewColumn

    default_feature_job_setting: Optional[FeatureJobSetting] = Field(allow_mutation=False)
    event_id_column: Optional[str] = Field(allow_mutation=False)

    @property
    def timestamp_column(self) -> str:
        """
        Timestamp column of the event data

        Returns
        -------
        str
        """
        input_node = next(
            node
            for node in self.graph.iterate_nodes(target_node=self.node, node_type=NodeType.INPUT)
            if cast(InputNode, node).parameters.type == TableDataType.EVENT_DATA
        )
        return input_node.parameters.timestamp  # type: ignore

    @property
    def inherited_columns(self) -> set[str]:
        """
        Special columns set which will be automatically added to the object of same class
        derived from current object

        Returns
        -------
        set[str]
        """
        return {self.timestamp_column}

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return super().protected_attributes + ["timestamp_column"]

    @classmethod
    @typechecked
    def from_event_data(cls, event_data: EventData) -> EventView:
        """
        Construct an EventView object

        Parameters
        ----------
        event_data: EventData
            EventData object used to construct EventView object

        Returns
        -------
        EventView
            constructed EventView object
        """
        return cls.from_data(
            event_data,
            default_feature_job_setting=event_data.default_feature_job_setting,
            event_id_column=event_data.event_id_column,
        )

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        params = super()._getitem_frame_params
        params.update(
            {
                "default_feature_job_setting": self.default_feature_job_setting,
                "event_id_column": self.event_id_column,
            }
        )
        return params

    def validate_join(self, other_view: View) -> None:
        validate_view(other_view)
