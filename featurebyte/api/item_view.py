"""
ItemView class
"""
from __future__ import annotations

from typing import Any, Optional

import copy

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.event_view import EventData, EventView
from featurebyte.api.item_data import ItemData
from featurebyte.api.view import View, ViewColumn
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.enum import NodeOutputType, NodeType


class ItemViewColumn(ViewColumn):
    """
    ItemViewColumn class
    """


class ItemView(View):
    """
    ItemView class
    """

    _series_class = ItemViewColumn

    event_id_column: str = Field(allow_mutation=False)
    item_id_column: str = Field(allow_mutation=False)
    event_data_id: PydanticObjectId = Field(allow_mutation=False)
    default_feature_job_setting: Optional[FeatureJobSetting] = Field(allow_mutation=False)
    event_view: EventView = Field(allow_mutation=False)

    @classmethod
    @typechecked
    def from_item_data(cls, item_data: ItemData) -> ItemView:
        """
        Construct an ItemView object

        Parameters
        ----------
        item_data : ItemData
            ItemData object used to construct ItemView object

        Returns
        -------
        ItemView
            constructed ItemView object
        """
        event_data = EventData.get_by_id(item_data.event_data_id)
        event_view = EventView.from_event_data(event_data)
        item_view = cls.from_data(
            item_data,
            event_id_column=item_data.event_id_column,
            item_id_column=item_data.item_id_column,
            event_data_id=item_data.event_data_id,
            default_feature_job_setting=item_data.default_feature_job_setting,
            event_view=event_view,
        )
        item_view.join_event_data_attributes(
            [event_view.timestamp_column] + event_view.entity_columns
        )
        return item_view

    def join_event_data_attributes(self, columns: list[str]) -> None:
        """
        Join additional attributes from the related EventData

        Parameters
        ----------
        columns : list[str]
            List of column names to include from the EventData

        Raises
        ------
        ValueError
            If the any of the provided columns does not exist in the EventData
        """
        for col in columns:
            if col not in self.event_view.columns:
                raise ValueError(f"Column does not exist in EventView: {col}")

        left_on = self.event_view.event_id_column
        left_input_columns = columns
        left_output_columns = columns

        right_on = self.event_id_column
        right_input_columns = self.columns
        right_output_columns = self.columns

        node = self.graph.add_operation(
            node_type=NodeType.JOIN,
            node_params={
                "left_on": left_on,
                "right_on": right_on,
                "left_input_columns": left_input_columns,
                "left_output_columns": left_output_columns,
                "right_input_columns": right_input_columns,
                "right_output_columns": right_output_columns,
                "join_type": "left",
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.event_view.node, self.node],
        )

        # Construct new columns_info
        columns_set = set(columns)
        joined_columns_info = copy.deepcopy(self.columns_info)
        for column_info in self.event_view.columns_info:
            if column_info.name in columns_set:
                joined_columns_info.append(column_info)

        # Construct new column_lineage_map
        joined_column_lineage_map = copy.deepcopy(self.column_lineage_map)
        for col in columns:
            joined_column_lineage_map[col] = self.event_view.column_lineage_map[col]
        for col, lineage in joined_column_lineage_map.items():
            joined_column_lineage_map[col] = self._append_to_lineage(lineage, node.name)

        # Construct new row_index_lineage
        joined_row_index_lineage = self._append_to_lineage(self.row_index_lineage, node.name)

        # Construct new tabular_data_ids
        joined_tabular_data_ids = sorted(
            set(self.tabular_data_ids + self.event_view.tabular_data_ids)
        )

        self.node_name = node.name
        self.columns_info = joined_columns_info
        self.column_lineage_map = joined_column_lineage_map
        self.row_index_lineage = joined_row_index_lineage
        self.__dict__.update({"tabular_data_ids": joined_tabular_data_ids})

    @property
    def timestamp_column(self) -> str:
        """
        Event timestamp column

        Returns
        -------
        """
        return self.event_view.timestamp_column

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return super().protected_attributes + [
            "event_id_column",
            "item_id_column",
            "timestamp_column",
        ]

    @property
    def inherited_columns(self) -> set[str]:
        """
        Special columns set which will be automatically added to the object of same class
        derived from current object

        Returns
        -------
        set[str]
        """
        return set()

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
                "event_id_column": self.event_id_column,
                "item_id_column": self.item_id_column,
                "event_data_id": self.event_data_id,
                "event_view": self.event_view,
            }
        )
        return params
