"""
ItemView class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, TypeVar

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
from featurebyte.api.item_data import ItemData
from featurebyte.api.join_utils import (
    combine_column_info_of_views,
    join_column_lineage_map,
    join_tabular_data_ids,
)
from featurebyte.api.validator import validate_view
from featurebyte.api.view import GroupByMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.metadata.operation import DerivedDataColumn

if TYPE_CHECKING:
    from featurebyte.api.groupby import GroupBy
else:
    GroupBy = TypeVar("GroupBy")


class ItemViewColumn(ViewColumn):
    """
    ItemViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class ItemView(View, GroupByMixin):
    """
    ItemView class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["View"],
        proxy_class="featurebyte.ItemView",
    )

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
                raise ValueError(f"Column does not exist in EventData: {col}")

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
                "join_type": "inner",
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.event_view.node, self.node],
        )

        # Construct new columns_info
        joined_columns_info = combine_column_info_of_views(
            self.columns_info, self.event_view.columns_info, filter_set=set(columns)
        )

        # Construct new column_lineage_map
        joined_column_lineage_map = join_column_lineage_map(
            self.column_lineage_map, self.event_view.column_lineage_map, columns, node.name
        )

        # Construct new tabular_data_ids
        joined_tabular_data_ids = join_tabular_data_ids(
            self.tabular_data_ids, self.event_view.tabular_data_ids
        )

        # Update metadata
        self._update_metadata(
            node.name, joined_columns_info, joined_column_lineage_map, joined_tabular_data_ids
        )

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
                "default_feature_job_setting": self.default_feature_job_setting,
                "event_view": self.event_view,
            }
        )
        return params

    def validate_aggregation_parameters(
        self, groupby_obj: GroupBy, value_column: Optional[str]
    ) -> None:
        """
        Check whether aggregation parameters are valid for ItemView

        Columns imported from the EventData or their derivatives can not be aggregated per an entity
        inherited from the EventData. Those features should be engineered directly from the
        EventData.

        Parameters
        ----------
        groupby_obj: GroupBy
            GroupBy object
        value_column: Optional[str]
            Column to be aggregated

        Raises
        ------
        ValueError
            If aggregation is using an EventData derived column and the groupby key is an Entity
            from EventData
        """
        if value_column is None:
            return

        keys = groupby_obj.keys
        groupby_keys_from_event_data = all(
            self._is_column_derived_only_from_event_data(key) for key in keys
        )
        if groupby_keys_from_event_data and self._is_column_derived_only_from_event_data(
            value_column
        ):
            raise ValueError(
                "Columns imported from EventData and their derivatives should be aggregated in"
                " EventView"
            )

    def _is_column_derived_only_from_event_data(self, column_name: str) -> bool:
        """
        Check if column is derived using only EventData's columns

        Parameters
        ----------
        column_name : str
            Name of the column in ItemView to check

        Returns
        -------
        bool
        """
        operation_structure = self.graph.extract_operation_structure(self.node)
        column_structure = next(
            column for column in operation_structure.columns if column.name == column_name
        )
        if isinstance(column_structure, DerivedDataColumn):
            return all(
                input_column.tabular_data_type == TableDataType.EVENT_DATA
                for input_column in column_structure.columns
            )
        # column_structure is a SourceDataColumn
        return column_structure.tabular_data_type == TableDataType.EVENT_DATA

    def validate_join(self, other_view: View) -> None:
        validate_view(other_view)

    def get_join_column(self) -> str:
        return self.item_id_column
