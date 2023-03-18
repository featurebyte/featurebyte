"""
ItemView class
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Optional

from pydantic import Field

from featurebyte.api.event_view import EventView
from featurebyte.api.view import GroupByMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.join_utils import join_tabular_data_ids
from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import ItemTableData
from featurebyte.query_graph.node.metadata.operation import DerivedDataColumn


class ItemViewColumn(ViewColumn):
    """
    ItemViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class ItemView(View, GroupByMixin):
    """
    ItemViews allow users to transform ItemTable to support the table preparation necessary before creating features.

    When an ItemView is created, the event_timestamp and the entities of the event table the item table is associated
    with are automatically added. Users can join more columns from the event table if desired.

    Transformations supported are the same as for EventView except for:\n
    - lag (and inter event time) can be computed only for entities that are not inherited from the event table

    Features can be easily created from ItemViews in a similar way as for features created from EventViews, with a
    few differences:\n
    - features for the event_id and its children (item_id) are not time based. Features for other entities are time
      based like for EventViews.\n
    - columns imported from the event table or their derivatives can not be aggregated per an entity inherited from
      the event table. Those features should be engineered directly from the event table.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["View"],
        proxy_class="featurebyte.ItemView",
    )

    # class variables
    _series_class = ItemViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.ITEM_VIEW

    # pydantic instance variables
    event_id_column: str = Field(allow_mutation=False)
    item_id_column: str = Field(allow_mutation=False)
    event_table_id: PydanticObjectId = Field(allow_mutation=False)
    default_feature_job_setting: Optional[FeatureJobSetting] = Field(allow_mutation=False)
    event_view: EventView = Field(allow_mutation=False)
    timestamp_column_name: str = Field(allow_mutation=False)

    def join_event_table_attributes(
        self,
        columns: list[str],
        event_suffix: Optional[str] = None,
    ) -> None:
        """
        Join additional attributes from the related EventTable

        Parameters
        ----------
        columns : list[str]
            List of column names to include from the EventTable
        event_suffix : Optional[str]
            A suffix to append on to the columns from the EventTable
        """
        assert self.event_view.event_id_column, "event_id_column is not set"
        (node, joined_columns_info, join_parameters,) = ItemTableData.join_event_view_columns(
            graph=self.graph,
            item_view_node=self.node,
            item_view_columns_info=self.columns_info,
            item_view_event_id_column=self.event_id_column,
            event_view_node=self.event_view.node,
            event_view_columns_info=self.event_view.columns_info,
            event_view_event_id_column=self.event_view.event_id_column,
            event_suffix=event_suffix,
            columns_to_join=columns,
        )

        # Update timestamp_column_name if event view's timestamp column is joined
        metadata_kwargs = {}
        for left_in_col, left_out_col in zip(
            join_parameters.left_input_columns, join_parameters.left_output_columns
        ):
            if left_in_col == self.event_view.timestamp_column:
                metadata_kwargs["timestamp_column_name"] = left_out_col

        # Update metadata only after validation is done & join node is inserted
        joined_tabular_data_ids = join_tabular_data_ids(
            self.tabular_data_ids, self.event_view.tabular_data_ids
        )
        self._update_metadata(
            node.name, joined_columns_info, joined_tabular_data_ids, **metadata_kwargs
        )

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
                "event_table_id": self.event_table_id,
                "default_feature_job_setting": self.default_feature_job_setting,
                "event_view": self.event_view,
                "timestamp_column_name": self.timestamp_column_name,
            }
        )
        return params

    def validate_aggregate_over_parameters(
        self, keys: list[str], value_column: Optional[str]
    ) -> None:
        """
        Check whether aggregate_over parameters are valid for ItemView.

        Parameters
        ----------
        keys: list[str]
            keys
        value_column: Optional[str]
            Column to be aggregated

        Raises
        ------
        ValueError
            raised when groupby keys contains the event ID column
        """
        if self.event_id_column in keys:
            raise ValueError(
                f"GroupBy keys must NOT contain the event ID column ({self.event_id_column}) when performing "
                "aggregate_over functions."
            )

        self._assert_not_all_columns_are_from_event_table(keys, value_column)

    def validate_simple_aggregate_parameters(
        self, keys: list[str], value_column: Optional[str]
    ) -> None:
        """
        Check whether aggregation parameters are valid for ItemView

        Columns imported from the EventTable or their derivatives can not be aggregated per an entity
        inherited from the EventTable. Those features should be engineered directly from the
        EventTable.

        Parameters
        ----------
        keys: list[str]
            keys
        value_column: Optional[str]
            Column to be aggregated

        Raises
        ------
        ValueError
            If aggregation is using an EventTable derived column and the groupby key is an Entity
            from EventTable
        """
        if self.event_id_column not in keys:
            raise ValueError(
                f"GroupBy keys must contain the event ID column ({self.event_id_column}) to prevent time leakage "
                "when performing simple aggregates."
            )

        self._assert_not_all_columns_are_from_event_table(keys, value_column)

    def _assert_not_all_columns_are_from_event_table(
        self, keys: list[str], value_column: Optional[str]
    ) -> None:
        """
        Helper method to validate whether columns are from event table.

        Parameters
        ----------
        keys: list[str]
            keys
        value_column: Optional[str]
            Column to be aggregated

        Raises
        ------
        ValueError
            raised when all columns are found in event table
        """
        if value_column is None:
            return

        columns_to_check = [*keys, value_column]
        if self._are_columns_derived_only_from_event_table(columns_to_check):
            raise ValueError(
                "Columns imported from EventTable and their derivatives should be aggregated in"
                " EventView"
            )

    def _are_columns_derived_only_from_event_table(self, column_names: List[str]) -> bool:
        """
        Check if column is derived using only EventTable's columns

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
                    input_column.tabular_data_type == TableDataType.EVENT_TABLE
                    for input_column in column_structure.columns
                ):
                    return False
                continue
            # column_structure is a SourceDataColumn
            if column_structure.tabular_data_type != TableDataType.EVENT_TABLE:
                return False
        return True

    def get_join_column(self) -> str:
        return self.item_id_column

    def _get_as_feature_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        _ = offset
        return {
            "event_parameters": {
                "event_timestamp_column": self.timestamp_column,
            }
        }
