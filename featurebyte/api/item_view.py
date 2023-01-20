"""
ItemView class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional, TypeVar, cast

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
from featurebyte.api.item_data import ItemData
from featurebyte.api.join_utils import (
    append_rsuffix_to_column_info,
    append_rsuffix_to_columns,
    combine_column_info_of_views,
    filter_join_key_from_column,
    join_tabular_data_ids,
)
from featurebyte.api.view import GroupByMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.exception import RepeatedColumnNamesError
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
    timestamp_column_name: str = Field(allow_mutation=False)

    @classmethod
    @typechecked
    def from_item_data(cls, item_data: ItemData, event_suffix: Optional[str] = None) -> ItemView:
        """
        Construct an ItemView object

        Parameters
        ----------
        item_data : ItemData
            ItemData object used to construct ItemView object
        event_suffix : Optional[str]
            A suffix to append on to the columns from the EventData

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
            timestamp_column_name=event_view.timestamp_column,
        )
        item_view.join_event_data_attributes(
            [event_view.timestamp_column] + event_view.entity_columns, event_suffix=event_suffix
        )
        return item_view

    def join_event_data_attributes(
        self,
        columns: list[str],
        event_suffix: Optional[str] = None,
    ) -> None:
        """
        Join additional attributes from the related EventData

        Parameters
        ----------
        columns : list[str]
            List of column names to include from the EventData
        event_suffix : Optional[str]
            A suffix to append on to the columns from the EventData

        Raises
        ------
        ValueError
            If the any of the provided columns does not exist in the EventData
        RepeatedColumnNamesError
            raised when there are overlapping columns, but no event_suffix has been provided
        """
        for col in columns:
            if col not in self.event_view.columns:
                raise ValueError(f"Column does not exist in EventData: {col}")

        # ItemData columns
        right_on = self.event_id_column
        right_input_columns = self.columns
        right_output_columns = self.columns

        # EventData columns
        left_on = cast(str, self.event_view.event_id_column)
        # EventData's event_id_column will be excluded from the result since that would be same as
        # ItemView's event_id_column. There is no need to specify event_suffix if the
        # event_id_column is the only common column name between EventData and ItemView.
        columns_excluding_event_id = filter_join_key_from_column(columns, left_on)
        renamed_event_view_columns = append_rsuffix_to_columns(
            columns_excluding_event_id, event_suffix
        )
        left_input_columns = []
        left_output_columns = []
        for input_col, output_col in zip(columns_excluding_event_id, renamed_event_view_columns):
            if input_col == self.event_view.timestamp_column:
                self.__dict__.update({"timestamp_column_name": output_col})
            left_input_columns.append(input_col)
            left_output_columns.append(output_col)

        repeated_column_names = sorted(set(right_output_columns).intersection(left_output_columns))
        if repeated_column_names:
            raise RepeatedColumnNamesError(
                f"Duplicate column names {repeated_column_names} found between EventData and"
                f" ItemView. Consider setting the event_suffix parameter to disambiguate the"
                f" resulting columns."
            )

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
            self.columns_info,
            append_rsuffix_to_column_info(self.event_view.columns_info, rsuffix=event_suffix),
            filter_set=set(renamed_event_view_columns),
        )

        # Construct new tabular_data_ids
        joined_tabular_data_ids = join_tabular_data_ids(
            self.tabular_data_ids, self.event_view.tabular_data_ids
        )

        # Update metadata
        self._update_metadata(node.name, joined_columns_info, joined_tabular_data_ids)

    @property
    def timestamp_column(self) -> str:
        """
        Event timestamp column

        Returns
        -------
        """
        return self.timestamp_column_name

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
                "timestamp_column_name": self.timestamp_column_name,
            }
        )
        return params

    def validate_aggregate_over_parameters(
        self, groupby_obj: GroupBy, value_column: Optional[str]
    ) -> None:
        """
        Check whether aggregate_over parameters are valid for ItemView.

        Parameters
        ----------
        groupby_obj: GroupBy
            GroupBy object
        value_column: Optional[str]
            Column to be aggregated

        Raises
        ------
        ValueError
            raised when groupby keys contains the event ID column
        """
        if self.event_id_column in groupby_obj.keys:
            raise ValueError(
                f"GroupBy keys must NOT contain the event ID column ({self.event_id_column}) when performing "
                "aggregate_over functions."
            )

        self._assert_not_all_columns_are_from_event_data(groupby_obj, value_column)

    def validate_simple_aggregate_parameters(
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
        if self.event_id_column not in groupby_obj.keys:
            raise ValueError(
                f"GroupBy keys must contain the event ID column ({self.event_id_column}) to prevent time leakage "
                "when performing simple aggregates."
            )

        self._assert_not_all_columns_are_from_event_data(groupby_obj, value_column)

    def _assert_not_all_columns_are_from_event_data(
        self, groupby_obj: GroupBy, value_column: Optional[str]
    ) -> None:
        """
        Helper method to validate whether columns are from event data.

        Parameters
        ----------
        groupby_obj: GroupBy
            GroupBy object
        value_column: Optional[str]
            Column to be aggregated

        Raises
        ------
        ValueError
            raised when all columns are found in event data
        """
        if value_column is None:
            return

        keys = groupby_obj.keys
        columns_to_check = [*keys, value_column]
        if self._are_columns_derived_only_from_event_data(columns_to_check):
            raise ValueError(
                "Columns imported from EventData and their derivatives should be aggregated in"
                " EventView"
            )

    def _are_columns_derived_only_from_event_data(self, column_names: List[str]) -> bool:
        """
        Check if column is derived using only EventData's columns

        Parameters
        ----------
        column_names : List[str]
            Column names in ItemView to check

        Returns
        -------
        bool
        """
        operation_structure = self.graph.extract_operation_structure(self.node)
        for column_name in column_names:
            column_structure = next(
                column for column in operation_structure.columns if column.name == column_name
            )
            if isinstance(column_structure, DerivedDataColumn):
                if not all(
                    input_column.tabular_data_type == TableDataType.EVENT_DATA
                    for input_column in column_structure.columns
                ):
                    return False
                continue
            # column_structure is a SourceDataColumn
            if column_structure.tabular_data_type != TableDataType.EVENT_DATA:
                return False
        return True

    def get_join_column(self) -> str:
        return self.item_id_column
