"""
CalendarTable class
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Type, cast

from bson import ObjectId
from pydantic import Field, StrictStr, model_validator
from typing_extensions import Literal

from featurebyte.api.base_table import TableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.calendar_table import CalendarTableModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.table import (
    AllTableDataT,
    CalendarTableData,
)
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata
from featurebyte.schema.calendar_table import CalendarTableCreate, CalendarTableUpdate

if TYPE_CHECKING:
    from featurebyte.api.calendar_view import CalendarView


class CalendarTable(TableApiObject):
    """
    A CalendarTable object represents a calendar/date dimension table.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.CalendarTable",
        skip_params_and_signature_in_class_docs=True,
    )
    _route: ClassVar[str] = "/calendar_table"
    _update_schema_class: ClassVar[Any] = CalendarTableUpdate
    _create_schema_class: ClassVar[Any] = CalendarTableCreate
    _get_schema: ClassVar[Any] = CalendarTableModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = CalendarTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.CALENDAR_TABLE] = TableDataType.CALENDAR_TABLE

    # pydantic instance variable (internal use)
    internal_calendar_datetime_column: StrictStr = Field(alias="calendar_datetime_column")
    internal_calendar_datetime_schema: TimestampSchema = Field(alias="calendar_datetime_schema")
    internal_series_id_column: Optional[StrictStr] = Field(alias="series_id_column", default=None)

    # pydantic validators
    _model_validator = model_validator(mode="after")(
        construct_data_model_validator(
            columns_info_key="internal_columns_info",
            expected_column_field_name_type_pairs=[
                (
                    "internal_record_creation_timestamp_column",
                    DBVarType.supported_timestamp_types(),
                ),
                (
                    "internal_calendar_datetime_column",
                    DBVarType.supported_ts_datetime_types(),
                ),
                ("internal_series_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    def get_view(
        self,
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL] = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
    ) -> CalendarView:
        """
        Gets a CalendarView object from a CalendarTable object.

        Parameters
        ----------
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use. When auto, the view will be constructed with cleaning operations from
            the table, and the record creation timestamp column will be dropped.
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only).
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            List of cleaning operations to apply per column in manual mode only. Each element in
            the list indicates the cleaning operations for a specific column.

        Returns
        -------
        CalendarView
            CalendarView object constructed from the source table.

        Examples
        --------
        Get a CalendarView in automated mode.

        >>> calendar_table = catalog.get_table("CALENDAR")  # doctest: +SKIP
        >>> calendar_view = calendar_table.get_view()  # doctest: +SKIP
        """
        from featurebyte.api.calendar_view import CalendarView

        self._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        drop_column_names = drop_column_names or []
        if view_mode == ViewMode.AUTO and self.record_creation_timestamp_column:
            drop_column_names.append(self.record_creation_timestamp_column)

        data_node = self.frame.node
        assert isinstance(data_node, InputNode)
        calendar_table_data = cast(CalendarTableData, self.table_data)
        (
            calendar_table_data,
            column_cleaning_operations,
        ) = self._prepare_table_data_and_column_cleaning_operations(
            table_data=calendar_table_data,
            column_cleaning_operations=column_cleaning_operations,
            view_mode=view_mode,
        )

        view_graph_node, columns_info = calendar_table_data.construct_calendar_view_graph_node(
            calendar_table_node=data_node,
            drop_column_names=drop_column_names,
            metadata=ViewMetadata(
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                table_id=data_node.parameters.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        columns_info = self._prepare_columns_info_for_view(
            view_node=inserted_graph_node, columns_info=columns_info
        )
        return CalendarView(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            series_id_column=self.series_id_column,
        )

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Timestamp column name of the CalendarTable

        Returns
        -------
        Optional[str]
        """
        return self.calendar_datetime_column

    @property
    def series_id_column(self) -> Optional[str]:
        """
        Series ID column name of the CalendarTable

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.series_id_column
        except RecordRetrievalException:
            return self.internal_series_id_column

    @property
    def calendar_datetime_column(self) -> str:
        """
        Calendar datetime column name of the CalendarTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.calendar_datetime_column
        except RecordRetrievalException:
            return self.internal_calendar_datetime_column

    @property
    def calendar_datetime_schema(self) -> TimestampSchema:
        """
        Schema of the calendar datetime column

        Returns
        -------
        TimestampSchema
        """
        try:
            return self.cached_model.calendar_datetime_schema
        except RecordRetrievalException:
            return self.internal_calendar_datetime_schema

    @classmethod
    def get_by_id(
        cls,
        id: ObjectId,
    ) -> CalendarTable:
        """
        Returns a CalendarTable object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            CalendarTable unique identifier ID.

        Returns
        -------
        CalendarTable
            CalendarTable object.

        Examples
        --------
        Get a CalendarTable object that is already saved.

        >>> fb.CalendarTable.get_by_id(<calendar_table_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)
