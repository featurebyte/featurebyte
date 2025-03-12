"""
ItemTable class
"""

from __future__ import annotations

import operator
from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Type, Union, cast

from bson import ObjectId
from cachetools import cachedmethod
from pydantic import Field, StrictStr, model_validator
from typing_extensions import Literal

from featurebyte.api.api_object import ApiObjectT, get_api_object_cache_key
from featurebyte.api.base_table import TableApiObject
from featurebyte.api.event_table import EventTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.join_utils import apply_column_name_modifiers
from featurebyte.common.validator import construct_data_model_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import FeatureByteBaseDocumentModel, PydanticObjectId
from featurebyte.models.item_table import ItemTableModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSettingUnion,
)
from featurebyte.query_graph.model.table import AllTableDataT, ItemTableData
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ItemViewMetadata
from featurebyte.schema.item_table import ItemTableCreate, ItemTableUpdate

if TYPE_CHECKING:
    from featurebyte.api.item_view import ItemView


def get_default_job_setting_cache_key(
    obj: Union[ApiObjectT, FeatureByteBaseDocumentModel], *args: Any, **kwargs: Any
) -> Any:
    """
    Construct cache key for a given document model object

    Parameters
    ----------
    obj: Union[ApiObjectT, FeatureByteBaseDocumentModel]
        Api object or document model object
    args: Any
        Additional positional arguments
    kwargs: Any
        Additional keywords arguments

    Returns
    -------
    Any
    """
    return get_api_object_cache_key(obj, *args, attribute="default_job_setting", **kwargs)


class ItemTable(TableApiObject):
    """
    An ItemTable object represents a source table in the data warehouse containing in-depth details about a
    business event.

    Typically, an Item table has a 'one-to-many' relationship with an Event table. Despite not explicitly including
    a timestamp, it is inherently linked to an event timestamp through its association with the Event table.

    For instance, an Item table can contain information about Product Items purchased in Customer Orders or Drug
    Prescriptions issued during Doctor Visits by Patients.

    ItemTable objects are created from a SourceTable object via the create_item_table method, and by identifying
    the columns representing the columns representing the item key and the event key and determine which EventTable
    object is associated with the Item table.

    After creation, the table can optionally incorporate additional metadata at the column level to further aid
    feature engineering. This can include identifying columns that identify or reference entities, providing
    information about the semantics of the table columns, specifying default cleaning operations, or furnishing
    descriptions of its columns.

    See Also
    --------
    - [create_item_table](/reference/featurebyte.api.source_table.SourceTable.create_item_table/): create item table from source table
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.ItemTable",
        skip_params_and_signature_in_class_docs=True,
    )
    _route: ClassVar[str] = "/item_table"
    _update_schema_class: ClassVar[Any] = ItemTableUpdate
    _create_schema_class: ClassVar[Any] = ItemTableCreate
    _get_schema: ClassVar[Any] = ItemTableModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = ItemTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.ITEM_TABLE] = TableDataType.ITEM_TABLE
    event_table_id: PydanticObjectId = Field(
        frozen=True,
        description="Returns the ID of the event table that " "is associated with the item table.",
    )

    # pydantic instance variable (internal use)
    internal_event_id_column: StrictStr = Field(alias="event_id_column")
    internal_item_id_column: Optional[StrictStr] = Field(alias="item_id_column")

    # pydantic validators
    _model_validator = model_validator(mode="after")(
        construct_data_model_validator(
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
        Gets an ItemView object from an ItemTable object.

        Item views are typically used to create Lookup features for the item entity, to create Simple Aggregate
        features for the event entity or to create Aggregate Over a Window features for other entities.

        You have the option to choose between two view construction modes: auto and manual, with auto being
        the default mode.

        When using the auto mode, the data accessed through the view is cleaned based on the default cleaning
        operations specified in the catalog table and special columns such as the record creation timestamp that
        are not intended for feature engineering are not included in the view columns.

        The event timestamp and event attributes representing entities in the related Event table are also
        automatically added to the ItemView.

        If there are column name conflicts between the EventTable and the ItemTable, and all the parameters are
        default, the column names from the EventTable that conflict with the ItemTable columns will be automatically
        removed from the ItemView.

        In manual mode, the default cleaning operations are not applied, and you have the flexibility to define your
        own cleaning operations.

        Parameters
        ----------
        event_suffix : Optional[str]
            A suffix to append on to the columns from the EventTable. This is useful to prevent column name
            collisions.
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use. When auto, the view will be constructed with cleaning operations from the table, the
            record creation timestamp column will be dropped and the event timestamp, timezone offset, and columns
            representing entities from the EventView will be automatically added.
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only).
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            List of cleaning operations to apply per column in manual mode only. Each element in the list indicates
            the cleaning operations for a specific column. The association between this column and the cleaning
            operations is established via the ColumnCleaningOperation constructor.
        event_drop_column_names: Optional[List[str]]
            List of column names to drop for the EventView (manual mode only).
        event_column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            Column cleaning operations to apply to the EventView (manual mode only).
        event_join_column_names: Optional[List[str]]
            List of column names to join from the EventView (manual mode only).

        Returns
        -------
        ItemView
            ItemView object constructed from the source table.

        Examples
        --------
        Get an ItemView in automated mode.

        >>> item_table = catalog.get_table("INVOICEITEMS")
        >>> item_view = item_table.get_view()


        Get an ItemView in manual mode.

        >>> item_table = catalog.get_table("INVOICEITEMS")
        >>> item_view = item_table.get_view(
        ...     event_suffix=None,
        ...     view_mode="manual",
        ...     drop_column_names=[],
        ...     column_cleaning_operations=[
        ...         fb.ColumnCleaningOperation(
        ...             column_name="Discount",
        ...             cleaning_operations=[
        ...                 fb.MissingValueImputation(imputed_value=0),
        ...                 fb.ValueBeyondEndpointImputation(
        ...                     type="less_than", end_point=0, imputed_value=None
        ...                 ),
        ...             ],
        ...         )
        ...     ],
        ...     event_drop_column_names=["record_available_at"],
        ...     event_column_cleaning_operations=[],
        ...     event_join_column_names=[
        ...         "Timestamp",
        ...         "GroceryInvoiceGuid",
        ...         "GroceryCustomerGuid",
        ...     ],
        ... )
        """
        from featurebyte.api.item_view import ItemView

        self._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
            event_drop_column_names=event_drop_column_names,
            event_column_cleaning_operations=event_column_cleaning_operations,
            event_join_column_names=event_join_column_names,
        )

        # auto resolve conflict columns only if all the parameters are default
        to_auto_resolve_column_conflict = (
            event_suffix is None
            and view_mode == ViewMode.AUTO
            and drop_column_names is None
            and column_cleaning_operations is None
            and event_drop_column_names is None
            and event_column_cleaning_operations is None
            and event_join_column_names is None
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
            if event_view.timestamp_timezone_offset_column is not None:
                event_join_column_names.append(event_view.timestamp_timezone_offset_column)
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
            to_auto_resolve_column_conflict=to_auto_resolve_column_conflict,
        )
        timestamp_column = apply_column_name_modifiers(
            [event_view.timestamp_column], rsuffix=event_suffix, rprefix=None
        )[0]
        if event_view.timestamp_timezone_offset_column is None:
            timestamp_timezone_offset_column = None
        else:
            timestamp_timezone_offset_column = apply_column_name_modifiers(
                [event_view.timestamp_timezone_offset_column], rsuffix=event_suffix, rprefix=None
            )[0]
        inserted_graph_node = GlobalQueryGraph().add_node(
            view_graph_node, input_nodes=[data_node, event_view.node]
        )
        return ItemView(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            event_id_column=self.event_id_column,
            item_id_column=self.item_id_column,
            event_table_id=self.event_table_id,
            default_feature_job_setting=self.default_feature_job_setting,
            event_view=event_view,
            timestamp_column_name=timestamp_column,
            timestamp_timezone_offset_column_name=timestamp_timezone_offset_column,
        )

    @property
    @cachedmethod(cache=operator.attrgetter("_cache"), key=get_default_job_setting_cache_key)
    def default_feature_job_setting(self) -> Optional[FeatureJobSettingUnion]:
        """
        Returns the default feature job setting for the table.

        The Default Feature Job Setting establishes the default setting used by features that
        aggregate data in the table, ensuring consistency of the Feature Job Setting across
        features created by different team members. While it's possible to override the setting
        during feature declaration, using the Default Feature Job Setting simplifies the process
        of setting up the Feature Job Setting for each feature.

        Returns
        -------
        Optional[FeatureJobSettingUnion]
        """
        try:
            return EventTable.get_by_id(self.event_table_id).default_feature_job_setting
        except RecordRetrievalException:
            # Typically this shouldn't happen since event_table_id should be available if the
            # ItemTable was instantiated correctly. Currently, this occurs only in tests.
            return None

    @property
    def event_id_column(self) -> str:
        """
        Returns the name of the column representing the event key of the Item view.

        Returns
        -------
        str
        """
        try:
            return self.cached_model.event_id_column
        except RecordRetrievalException:
            return self.internal_event_id_column

    @property
    def item_id_column(self) -> Optional[str]:
        """
        Returns the name of the column representing the item key of the Item view.

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.item_id_column
        except RecordRetrievalException:
            return self.internal_item_id_column

    @property
    def timestamp_column(self) -> Optional[str]:
        return None

    @classmethod
    def get_by_id(cls, id: ObjectId) -> ItemTable:
        """
        Returns an ItemTable object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            ItemTable unique identifier ID.

        Returns
        -------
        ItemTable
            ItemTable object.

        Examples
        --------
        Get an ItemTable object that is already saved.

        >>> fb.ItemTable.get_by_id(<item_table_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)
