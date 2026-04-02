"""
ForecastTable API payload schema
"""

from __future__ import annotations

from typing import Literal, Optional, Sequence

from pydantic import Field, StrictStr, field_validator

from featurebyte.enum import TableDataType
from featurebyte.models.forecast_table import ForecastTableModel
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class ForecastTableCreate(TableCreate):
    """
    ForecastTable Creation Schema
    """

    type: Literal[TableDataType.FORECAST_TABLE] = TableDataType.FORECAST_TABLE
    natural_key_column: Optional[StrictStr] = Field(default=None)
    effective_timestamp_column: StrictStr
    effective_timestamp_schema: Optional[TimestampSchema] = Field(default=None)
    forecast_timestamp_column: StrictStr
    forecast_timestamp_schema: Optional[TimestampSchema] = Field(default=None)

    # pydantic validators
    _special_columns_validator = field_validator(
        "record_creation_timestamp_column",
        "natural_key_column",
        "effective_timestamp_column",
        "forecast_timestamp_column",
        "datetime_partition_column",
        mode="after",
    )(TableCreate._special_column_validator)


class ForecastTableList(PaginationMixin):
    """
    Paginated list of ForecastTable
    """

    data: Sequence[ForecastTableModel]


class ForecastTableUpdate(TableUpdate):
    """
    ForecastTable update payload schema
    """


class ForecastTableServiceUpdate(TableServiceUpdate):
    """
    ForecastTable service update schema
    """
