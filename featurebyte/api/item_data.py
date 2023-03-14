"""
ItemData class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Literal, Optional, Type, cast

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.api.base_data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.event_data import EventData
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.join_utils import join_tabular_data_ids
from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.item_data import ItemDataModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import AllTableDataT, ItemTableData
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ItemViewMetadata
from featurebyte.schema.item_data import ItemDataCreate, ItemDataUpdate

if TYPE_CHECKING:
    from featurebyte.api.item_view import ItemView


class ItemData(DataApiObject):
    """
    ItemData is an object connected with an item table that has a ‘one to many’ relationship with an event table.
    Example:\n
    - Order item table -> Order table\n
    - Drug prescriptions -> Doctor visits.

    The table does not explicitly contain any timestamp, but is implicitly related to an event timestamp via its
    relationship with the event table.

    To register a new ItemData, users are asked to provide:\n
    - the name of the column of the item id\n
    - the name of the column of the event id\n
    - the name of the event data it is related to

    The ItemData inherits the default FeatureJob setting of the Event data.

    Like for Event Data, users are strongly encouraged to annotate the data by tagging entities and defining:\n
    - the semantic of the data field\n
    - critical data information on the data quality that requires cleaning before feature engineering

    To create features from an ItemData, users create an ItemView.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Data"], proxy_class="featurebyte.ItemData")

    # class variables
    _route = "/item_data"
    _update_schema_class = ItemDataUpdate
    _create_schema_class = ItemDataCreate
    _get_schema = ItemDataModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = ItemTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.ITEM_DATA] = Field(TableDataType.ITEM_DATA, const=True)
    event_data_id: PydanticObjectId = Field(allow_mutation=False)
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
                ("internal_record_creation_date_column", DBVarType.supported_timestamp_types()),
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
            A suffix to append on to the columns from the EventData
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto), when auto, the view will be constructed with cleaning operations
            from the data, the record creation date column will be dropped and the columns to join from the
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

        event_data = EventData.get_by_id(self.event_data_id)
        event_view = event_data.get_view(
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
            if self.record_creation_date_column:
                drop_column_names.append(self.record_creation_date_column)

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
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=join_tabular_data_ids([self.id], event_view.tabular_data_ids),
            event_id_column=self.event_id_column,
            item_id_column=self.item_id_column,
            event_data_id=self.event_data_id,
            default_feature_job_setting=self.default_feature_job_setting,
            event_view=event_view,
            timestamp_column_name=timestamp_column,
        )

    @root_validator(pre=True)
    @classmethod
    def _set_default_feature_job_setting(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "event_data_id" in values:
            event_data_id = values["event_data_id"]
            try:
                default_feature_job_setting = EventData.get_by_id(
                    event_data_id
                ).default_feature_job_setting
            except RecordRetrievalException:
                # Typically this shouldn't happen since event_data_id should be available if the
                # ItemData was instantiated correctly. Currently, this occurs only in tests.
                return values
            values["default_feature_job_setting"] = default_feature_job_setting
        return values

    @property
    def event_id_column(self) -> str:
        """
        Event ID column name of the EventData associated with the ItemData

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
        Item ID column name of the ItemData

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
        tabular_source: DatabaseTable,
        name: str,
        event_id_column: str,
        item_id_column: str,
        event_data_name: str,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> ItemData:
        """
        Create ItemData object from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
            DatabaseTable object constructed from FeatureStore
        name: str
            Item data name
        event_id_column: str
            Event ID column from the given tabular source
        item_id_column: str
            Item ID column from the given tabular source
        event_data_name: str
            Name of the EventData associated with this ItemData
        record_creation_date_column: Optional[str]
            Record creation datetime column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object

        Returns
        -------
        EventData

        Raises
        ------
        ValueError
            If the associated EventData does not have event_id_column defined

        Examples
        --------
        Create ItemData from a table in the feature store

        >>> order_items = ItemData.from_tabular_source(  # doctest: +SKIP
        ...    name="Order Items",
        ...    tabular_source=feature_store.get_table(
        ...      database_name="DEMO",
        ...      schema_name="ORDERS",
        ...      table_name="ORDER_ITEMS"
        ...    ),
        ...    event_id_column="ORDER_ID",
        ...    item_id_column="ITEM_ID",
        ...    event_data_name="Order List",
        ...    record_creation_date_column="RECORD_AVAILABLE_AT",
        ... )

        Get information about the ItemData

        >>> order_items.info(verbose=True)  # doctest: +SKIP
        """
        event_data = EventData.get(event_data_name)
        if event_data.event_id_column is None:
            raise ValueError("EventData without event_id_column is not supported")
        event_data_id = event_data.id
        return super().create(
            tabular_source=tabular_source,
            name=name,
            record_creation_date_column=record_creation_date_column,
            _id=_id,
            event_id_column=event_id_column,
            item_id_column=item_id_column,
            event_data_id=event_data_id,
        )
