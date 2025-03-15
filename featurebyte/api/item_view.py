"""
ItemView class
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Optional

from pydantic import Field

from featurebyte.api.event_view import EventView
from featurebyte.api.view import GroupByMixin, RawMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSettingUnion,
)
from featurebyte.query_graph.model.table import ItemTableData
from featurebyte.query_graph.node.metadata.operation import DerivedDataColumn


class ItemViewColumn(ViewColumn):
    """
    ItemViewColumn class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()


class ItemView(View, GroupByMixin, RawMixin):
    """
    An ItemView object is a modified version of the ItemTable object that provides additional capabilities for
    transforming data. With an ItemView, you can create and transform columns, extract lags and filter records
    prior to feature declaration.

    When you create an ItemView, the object automatically adds the event_timestamp and entity columns from the
    associated event table. You can also include additional columns from the event table if desired. However:

    - lag (and inter-event time) computation is only possible for entities that are not inherited from the event table.
    - imported columns from the event table or their derivatives cannot be aggregated per an entity inherited from the
    event table. Those features must be engineered directly from the event table.

    Item views are typically used to create Lookup features for the item entity, to create Simple Aggregate features
    for the event entity or to create Aggregate Over a Window features for other entities."

    See Also
    --------
    - [item_table#get_view](/reference/featurebyte.api.item_table.ItemTable.get_view/): get item view from an `ItemTable`
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.ItemView",
        skip_params_and_signature_in_class_docs=True,
    )
    _series_class: ClassVar[Any] = ItemViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.ITEM_VIEW

    # pydantic instance variables
    event_id_column: str = Field(frozen=True)
    item_id_column: Optional[str] = Field(frozen=True)
    event_table_id: PydanticObjectId = Field(
        frozen=True,
        description="Returns the unique identifier (ID) "
        "of the Event Table related to the Item "
        "view.",
    )
    default_feature_job_setting: Optional[FeatureJobSettingUnion] = Field(
        frozen=True,
        description="Returns the default feature job setting for the view.\n\n"
        "The Default Feature Job Setting establishes the default setting used "
        "by features that aggregate data in the view, ensuring consistency of "
        "the Feature Job Setting across features created by different team members. "
        "While it's possible to override the setting during feature declaration, "
        "using the Default Feature Job Setting simplifies the process of setting "
        "up the Feature Job Setting for each feature.",
        default=None,
    )
    event_view: EventView = Field(frozen=True)
    timestamp_column_name: str = Field(frozen=True)
    timestamp_timezone_offset_column_name: Optional[str] = Field(frozen=True, default=None)

    def join_event_table_attributes(
        self,
        columns: list[str],
        event_suffix: Optional[str] = None,
    ) -> ItemView:
        """
        Joins additional attributes from the related EventTable. This operation returns a new ItemView object.

        Note that the event timestamp and event attributes representing entities in the related Event table are
        already automatically added to the ItemView.

        Parameters
        ----------
        columns : list[str]
            List of column names to include from the EventTable.
        event_suffix : Optional[str]
            A suffix to append on to the columns from the EventTable.

        Returns
        -------
        ItemView
            The ItemView object with the joined columns from the EventTable.

        Examples
        --------
        Join columns into an ItemView.

        >>> item_view = catalog.get_view("INVOICEITEMS")
        >>> item_view = item_view.join_event_table_attributes(
        ...     columns=["Timestamp"], event_suffix="_event"
        ... )

        """
        assert self.event_view.event_id_column, "event_id_column is not set"
        (
            node,
            joined_columns_info,
            join_parameters,
        ) = ItemTableData.join_event_view_columns(
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
        for right_in_col, right_out_col in zip(
            join_parameters.right_input_columns, join_parameters.right_output_columns
        ):
            if right_in_col == self.event_view.timestamp_column:
                metadata_kwargs["timestamp_column_name"] = right_out_col

        # create a new view and return it
        return self._create_joined_view(node.name, joined_columns_info, **metadata_kwargs)

    @property
    def timestamp_column(self) -> str:
        """
        Event timestamp column

        Returns
        -------
        str
        """
        return self.timestamp_column_name

    @property
    def timestamp_timezone_offset_column(self) -> Optional[str]:
        """
        Event timezone offset column

        Returns
        -------
        str
        """
        return self.timestamp_timezone_offset_column_name

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        out = super().protected_attributes + [
            "event_id_column",
            "item_id_column",
            "timestamp_column",
        ]
        if self.timestamp_timezone_offset_column is not None:
            out.append("timestamp_timezone_offset_column")
        return out

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        params = super()._getitem_frame_params
        params.update({
            "event_id_column": self.event_id_column,
            "item_id_column": self.item_id_column,
            "event_table_id": self.event_table_id,
            "default_feature_job_setting": self.default_feature_job_setting,
            "event_view": self.event_view,
            "timestamp_column_name": self.timestamp_column_name,
        })
        return params

    def _get_create_joined_view_parameters(self) -> dict[str, Any]:
        return {"event_view": self.event_view}

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
        # FIXME: This method is may not work correctly when the column is from SCD table joined with EventTable
        operation_structure = self.graph.extract_operation_structure(
            self.node, keep_all_source_columns=False
        )
        for column_name in column_names:
            column_structure = next(
                column for column in operation_structure.columns if column.name == column_name
            )
            if isinstance(column_structure, DerivedDataColumn):
                if not all(
                    input_column.table_type == TableDataType.EVENT_TABLE
                    for input_column in column_structure.columns
                ):
                    return False
                continue
            # column_structure is a SourceDataColumn
            if column_structure.table_type != TableDataType.EVENT_TABLE:
                return False
        return True

    def get_join_column(self) -> str:
        join_column = self._get_join_column()
        assert join_column is not None, "Item ID column is not available."
        return join_column

    def _get_join_column(self) -> Optional[str]:
        return self.item_id_column

    def get_additional_lookup_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        _ = offset
        return {
            "event_parameters": {
                "event_timestamp_column": self.timestamp_column,
                "event_timestamp_metadata": self.operation_structure.get_dtype_metadata(
                    column_name=self.timestamp_column
                ),
            }
        }
