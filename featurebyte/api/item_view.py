"""
ItemView class
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Literal, Optional, cast

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
from featurebyte.api.item_data import ItemData
from featurebyte.api.view import GroupByMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.join_utils import join_tabular_data_ids
from featurebyte.enum import TableDataType, ViewMode
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import ItemTableData
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.metadata.operation import DerivedDataColumn
from featurebyte.query_graph.node.nested import ColumnCleaningOperation, ItemViewMetadata


class ItemViewColumn(ViewColumn):
    """
    ItemViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class ItemView(View, GroupByMixin):
    """
    ItemViews allow users to transform ItemData to support the data preparation necessary before creating features.

    When an ItemView is created, the event_timestamp and the entities of the event data the item data is associated
    with are automatically added. Users can join more columns from the event data if desired.

    Transformations supported are the same as for EventView except for:\n
    - lag (and inter event time) can be computed only for entities that are not inherited from the event data

    Features can be easily created from ItemViews in a similar way as for features created from EventViews, with a
    few differences:\n
    - features for the event_id and its children (item_id) are not time based. Features for other entities are time
      based like for EventViews.\n
    - columns imported from the event data or their derivatives can not be aggregated per an entity inherited from
      the event data. Those features should be engineered directly from the event data.
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
    event_data_id: PydanticObjectId = Field(allow_mutation=False)
    default_feature_job_setting: Optional[FeatureJobSetting] = Field(allow_mutation=False)
    event_view: EventView = Field(allow_mutation=False)
    timestamp_column_name: str = Field(allow_mutation=False)

    @classmethod
    @typechecked
    def from_item_data(
        cls,
        item_data: ItemData,
        event_suffix: Optional[str] = None,
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL] = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
        event_drop_column_names: Optional[List[str]] = None,
        event_column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
        event_join_column_names: Optional[List[str]] = None,
    ) -> ItemView:
        """
        Construct an ItemView object

        Parameters
        ----------
        item_data : ItemData
            ItemData object used to construct ItemView object
        event_suffix : Optional[str]
            A suffix to append on to the columns from the EventData
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto), when auto, the view will be constructed with cleaning operations
            from the data, the record creation date column will be dropped and the columns to join from the
            EventView will be automatically selected
        drop_column_names: Optional[List[str]]
            List of column names to drop for the ItemView (manual mode only)
        column_cleaning_operations: Optional[List[featurebyte.query_graph.node.nested.ColumnCleaningOperation]]
            Column cleaning operations to apply to the ItemView (manual mode only)
        event_drop_column_names: Optional[List[str]]
            List of column names to drop for the EventView (manual mode only)
        event_column_cleaning_operations: Optional[List[featurebyte.query_graph.node.nested.ColumnCleaningOperation]]
            Column cleaning operations to apply to the EventView (manual mode only)
        event_join_column_names: Optional[List[str]]
            List of column names to join from the EventView (manual mode only)

        Returns
        -------
        ItemView
            constructed ItemView object
        """
        cls._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
            event_drop_column_names=event_drop_column_names,
            event_column_cleaning_operations=event_column_cleaning_operations,
            event_join_column_names=event_join_column_names,
        )

        event_data = EventData.get_by_id(item_data.event_data_id)
        event_view = EventView.from_event_data(
            event_data=event_data,
            drop_column_names=event_drop_column_names,
            column_cleaning_operations=event_column_cleaning_operations,
            view_mode=view_mode,
        )
        assert event_view.event_id_column, "event_id_column is not set"

        # construct view graph node for item data, the final graph looks like:
        #     +-----------------------+    +----------------------------+
        #     | InputNode(type:event) | -->| GraphNode(type:event_view) | ---+
        #     +-----------------------+    +----------------------------+    |
        #                                                                    v
        #                            +----------------------+    +---------------------------+
        #                            | InputNode(type:item) | -->| GraphNode(type:item_view) |
        #                            +----------------------+    +---------------------------+
        drop_column_names = drop_column_names or []
        event_drop_column_names = event_drop_column_names or []
        event_column_cleaning_operations = event_column_cleaning_operations or []
        event_join_column_names = event_join_column_names or [event_view.timestamp_column]
        if view_mode == ViewMode.AUTO:
            if item_data.record_creation_date_column:
                drop_column_names.append(item_data.record_creation_date_column)

            event_view_param_metadata = event_view.node.parameters.metadata  # type: ignore
            event_join_column_names = [event_view.timestamp_column] + event_view.entity_columns
            event_drop_column_names = event_view_param_metadata.drop_column_names
            event_column_cleaning_operations = event_view_param_metadata.column_cleaning_operations

        data_node = item_data.frame.node
        assert isinstance(data_node, InputNode)
        item_table_data = cast(ItemTableData, item_data.table_data)
        column_cleaning_operations = column_cleaning_operations or []
        (
            item_table_data,
            column_cleaning_operations,
        ) = cls._prepare_table_data_and_column_cleaning_operations(
            table_data=item_table_data,
            column_cleaning_operations=column_cleaning_operations,
            view_mode=view_mode,
        )

        (
            view_graph_node,
            columns_info,
            timestamp_column,
        ) = item_table_data.construct_item_view_graph_node(
            item_data_node=data_node,
            columns_to_join=event_join_column_names,
            event_view_node=event_view.node,
            event_view_columns_info=event_view.columns_info,
            event_view_event_id_column=event_view.event_id_column,
            event_suffix=event_suffix,
            drop_column_names=drop_column_names,
            metadata=ItemViewMetadata(
                event_suffix=event_suffix,
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                data_id=data_node.parameters.id,
                event_drop_column_names=event_drop_column_names,
                event_column_cleaning_operations=event_column_cleaning_operations,
                event_join_column_names=event_join_column_names,
                event_data_id=event_data.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(
            view_graph_node, input_nodes=[data_node, event_view.node]
        )
        return ItemView(
            feature_store=item_data.feature_store,
            tabular_source=item_data.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=join_tabular_data_ids([item_data.id], event_view.tabular_data_ids),
            event_id_column=item_data.event_id_column,
            item_id_column=item_data.item_id_column,
            event_data_id=item_data.event_data_id,
            default_feature_job_setting=item_data.default_feature_job_setting,
            event_view=event_view,
            timestamp_column_name=timestamp_column,
        )

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
                "event_data_id": self.event_data_id,
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

        self._assert_not_all_columns_are_from_event_data(keys, value_column)

    def validate_simple_aggregate_parameters(
        self, keys: list[str], value_column: Optional[str]
    ) -> None:
        """
        Check whether aggregation parameters are valid for ItemView

        Columns imported from the EventData or their derivatives can not be aggregated per an entity
        inherited from the EventData. Those features should be engineered directly from the
        EventData.

        Parameters
        ----------
        keys: list[str]
            keys
        value_column: Optional[str]
            Column to be aggregated

        Raises
        ------
        ValueError
            If aggregation is using an EventData derived column and the groupby key is an Entity
            from EventData
        """
        if self.event_id_column not in keys:
            raise ValueError(
                f"GroupBy keys must contain the event ID column ({self.event_id_column}) to prevent time leakage "
                "when performing simple aggregates."
            )

        self._assert_not_all_columns_are_from_event_data(keys, value_column)

    def _assert_not_all_columns_are_from_event_data(
        self, keys: list[str], value_column: Optional[str]
    ) -> None:
        """
        Helper method to validate whether columns are from event data.

        Parameters
        ----------
        keys: list[str]
            keys
        value_column: Optional[str]
            Column to be aggregated

        Raises
        ------
        ValueError
            raised when all columns are found in event data
        """
        if value_column is None:
            return

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

    def _get_as_feature_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        _ = offset
        return {
            "event_parameters": {
                "event_timestamp_column": self.timestamp_column,
            }
        }
