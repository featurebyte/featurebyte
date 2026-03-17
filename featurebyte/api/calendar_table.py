"""
CalendarTable class
"""

from __future__ import annotations

from typing import Any, ClassVar, Optional, Type

from bson import ObjectId
from pydantic import Field, StrictStr, model_validator
from typing_extensions import Literal

from featurebyte.api.base_table import TableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_validator
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.calendar_table import CalendarTableModel
from featurebyte.query_graph.model.table import (
    AllTableDataT,
    CalendarTableData,
)
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.schema.calendar_table import CalendarTableCreate, CalendarTableUpdate


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

    def get_view(self, **kwargs: Any) -> None:
        """
        CalendarView is not yet implemented.

        Parameters
        ----------
        **kwargs: Any
            Unused keyword arguments.

        Raises
        ------
        NotImplementedError
            Always raised as CalendarView is not yet implemented.
        """
        raise NotImplementedError("CalendarView not yet implemented")

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
