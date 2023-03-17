"""
ItemTable class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Literal, Optional, Type, cast

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.api.base_table import TableApiObject
from featurebyte.api.event_table import EventTable
from featurebyte.api.source_table import SourceTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.join_utils import join_tabular_data_ids
from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.item_table import ItemTableModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import AllTableDataT, ItemTableData
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ItemViewMetadata
from featurebyte.schema.item_table import ItemTableCreate, ItemTableUpdate

if TYPE_CHECKING:
    from featurebyte.api.item_view import ItemView


class ItemTable(TableApiObject):
    """
    ItemTable is an object connected with an item table that has a ‘one to many’ relationship with an event table.
    Example:\n
    - Order item table -> Order table\n
    - Drug prescriptions -> Doctor visits.

    The table does not explicitly contain any timestamp, but is implicitly related to an event timestamp via its
    relationship with the event table.

    To register a new ItemTable, users are asked to provide:\n
    - the name of the column of the item id\n
    - the name of the column of the event id\n
    - the name of the event table it is related to

    The ItemTable inherits the default FeatureJob setting of the Event table.

    Like for Event Data, users are strongly encouraged to annotate the table by tagging entities and defining:\n
    - the semantic of the table field\n
    - critical data information on the data quality that requires cleaning before feature engineering

    To create features from an ItemTable, users create an ItemView.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Table"], proxy_class="featurebyte.ItemTable")

    # class variables
    _route = "/item_table"
    _update_schema_class = ItemTableUpdate
    _create_schema_class = ItemTableCreate
    _get_schema = ItemTableModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = ItemTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.ITEM_TABLE] = Field(TableDataType.ITEM_TABLE, const=True)
    event_table_id: PydanticObjectId = Field(allow_mutation=False)
    default_feature_job_setting: Optional[FeatureJobSetting] = Field(
        exclude=True, allow_mutation=False
    )

    # pydantic instance variable (internal use)
    internal_event_id_column: StrictStr = Field(alias="event_id_column")
    internal_item_id_column: StrictStr = Field(alias="item_id_column")

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="internal_columns_info",
            expected_column_field_name_type_pairs=[
                (
                    "internal_record_creation_timestamp_column",
                    DBVarType.supported_timestamp_types(),
                ),
                ("internal_event_id_column", DBVarType.supported_id_types()),
                ("internal_item_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    def get_view(
        self,
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
        event_suffix : Optional[str]
            A suffix to append on to the columns from the EventTable
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto), when auto, the view will be constructed with cleaning operations
            from the table, the record creation timestamp column will be dropped and the columns to join from the
            EventView will be automatically selected
        drop_column_names: Optional[List[str]]
            List of column names to drop for the ItemView (manual mode only)
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            Column cleaning operations to apply to the ItemView (manual mode only)
        event_drop_column_names: Optional[List[str]]
            List of column names to drop for the EventView (manual mode only)
        event_column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            Column cleaning operations to apply to the EventView (manual mode only)
        event_join_column_names: Optional[List[str]]
            List of column names to join from the EventView (manual mode only)

        Returns
        -------
        ItemView
            constructed ItemView object
        """
        from featurebyte.api.item_view import ItemView  # pylint: disable=import-outside-toplevel

        self._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
            event_drop_column_names=event_drop_column_names,
            event_column_cleaning_operations=event_column_cleaning_operations,
            event_join_column_names=event_join_column_names,
        )

        event_table = EventTable.get_by_id(self.event_table_id)
        event_view = event_table.get_view(
            drop_column_names=event_drop_column_names,
            column_cleaning_operations=event_column_cleaning_operations,
            view_mode=view_mode,
        )
        assert event_view.event_id_column, "event_id_column is not set"

        # construct view graph node for item table, the final graph looks like:
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
            if self.record_creation_timestamp_column:
                drop_column_names.append(self.record_creation_timestamp_column)

            event_view_param_metadata = event_view.node.parameters.metadata  # type: ignore
            event_join_column_names = [event_view.timestamp_column] + event_view.entity_columns
            event_drop_column_names = event_view_param_metadata.drop_column_names
            event_column_cleaning_operations = event_view_param_metadata.column_cleaning_operations

        data_node = self.frame.node
        assert isinstance(data_node, InputNode)
        item_table_data = cast(ItemTableData, self.table_data)
        column_cleaning_operations = column_cleaning_operations or []
        (
            item_table_data,
            column_cleaning_operations,
        ) = self._prepare_table_data_and_column_cleaning_operations(
            table_data=item_table_data,
            column_cleaning_operations=column_cleaning_operations,
            view_mode=view_mode,
        )

        (
            view_graph_node,
            columns_info,
            timestamp_column,
        ) = item_table_data.construct_item_view_graph_node(
            item_table_node=data_node,
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
                table_id=data_node.parameters.id,
                event_drop_column_names=event_drop_column_names,
                event_column_cleaning_operations=event_column_cleaning_operations,
                event_join_column_names=event_join_column_names,
                event_table_id=event_table.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(
            view_graph_node, input_nodes=[data_node, event_view.node]
        )
        return ItemView(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=join_tabular_data_ids([self.id], event_view.tabular_data_ids),
            event_id_column=self.event_id_column,
            item_id_column=self.item_id_column,
            event_table_id=self.event_table_id,
            default_feature_job_setting=self.default_feature_job_setting,
            event_view=event_view,
            timestamp_column_name=timestamp_column,
        )

    @root_validator(pre=True)
    @classmethod
    def _set_default_feature_job_setting(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "event_table_id" in values:
            event_table_id = values["event_table_id"]
            try:
                default_feature_job_setting = EventTable.get_by_id(
                    event_table_id
                ).default_feature_job_setting
            except RecordRetrievalException:
                # Typically this shouldn't happen since event_table_id should be available if the
                # ItemTable was instantiated correctly. Currently, this occurs only in tests.
                return values
            values["default_feature_job_setting"] = default_feature_job_setting
        return values

    @property
    def event_id_column(self) -> str:
        """
        Event ID column name of the EventTable associated with the ItemTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.event_id_column
        except RecordRetrievalException:
            return self.internal_event_id_column

    @property
    def item_id_column(self) -> str:
        """
        Item ID column name of the ItemTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.item_id_column
        except RecordRetrievalException:
            return self.internal_item_id_column

    @classmethod
    @typechecked
    def from_tabular_source(
        cls,
        tabular_source: SourceTable,
        name: str,
        event_id_column: str,
        item_id_column: str,
        event_table_name: str,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> ItemTable:
        """
        Create ItemTable object from tabular source

        Parameters
        ----------
        tabular_source: SourceTable
            DatabaseTable object constructed from FeatureStore
        name: str
            Item table name
        event_id_column: str
            Event ID column from the given tabular source
        item_id_column: str
            Item ID column from the given tabular source
        event_table_name: str
            Name of the EventTable associated with this ItemTable
        record_creation_timestamp_column: Optional[str]
            Record creation timestamp column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object

        Returns
        -------
        EventTable

        Raises
        ------
        ValueError
            If the associated EventTable does not have event_id_column defined

        Examples
        --------
        Create ItemTable from a table in the feature store

        >>> order_items = ItemTable.from_tabular_source(  # doctest: +SKIP
        ...    name="Order Items",
        ...    tabular_source=feature_store.get_table(
        ...      database_name="DEMO",
        ...      schema_name="ORDERS",
        ...      table_name="ORDER_ITEMS"
        ...    ),
        ...    event_id_column="ORDER_ID",
        ...    item_id_column="ITEM_ID",
        ...    event_table_name="Order List",
        ...    record_creation_timestamp_column="RECORD_AVAILABLE_AT",
        ... )

        Get information about the ItemTable

        >>> order_items.info(verbose=True)  # doctest: +SKIP
        """
        event_table = EventTable.get(event_table_name)
        if event_table.event_id_column is None:
            raise ValueError("EventTable without event_id_column is not supported")
        event_table_id = event_table.id
        return super().create(
            tabular_source=tabular_source,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            _id=_id,
            event_id_column=event_id_column,
            item_id_column=item_id_column,
            event_table_id=event_table_id,
        )
