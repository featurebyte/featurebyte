"""
CalendarTable API payload schema
"""

from __future__ import annotations

from typing import Literal, Optional, Sequence

from pydantic import Field, StrictStr, field_validator

from featurebyte.enum import TableDataType
from featurebyte.models.calendar_table import CalendarTableModel
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class CalendarTableCreate(TableCreate):
    """
    CalendarTable Creation Schema
    """

    type: Literal[TableDataType.CALENDAR_TABLE] = TableDataType.CALENDAR_TABLE
    calendar_datetime_column: StrictStr
    calendar_datetime_schema: TimestampSchema
    series_id_column: Optional[StrictStr] = Field(default=None)

    # pydantic validators
    _special_columns_validator = field_validator(
        "record_creation_timestamp_column",
        "series_id_column",
        "calendar_datetime_column",
        "datetime_partition_column",
        mode="after",
    )(TableCreate._special_column_validator)


class CalendarTableList(PaginationMixin):
    """
    Paginated list of CalendarTable
    """

    data: Sequence[CalendarTableModel]


class CalendarTableUpdate(TableUpdate):
    """
    CalendarTable update payload schema
    """


class CalendarTableServiceUpdate(TableServiceUpdate):
    """
    CalendarTable service update schema
    """
