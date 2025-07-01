"""
DimensionTable API payload schema
"""

from __future__ import annotations

from typing import List, Literal

from pydantic import StrictStr, field_validator

from featurebyte.enum import TableDataType
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class DimensionTableCreate(TableCreate):
    """
    DimensionTable Creation Schema
    """

    type: Literal[TableDataType.DIMENSION_TABLE] = TableDataType.DIMENSION_TABLE
    dimension_id_column: StrictStr

    # pydantic validators
    _special_columns_validator = field_validator(
        "record_creation_timestamp_column",
        "dimension_id_column",
        "datetime_partition_column",
    )(TableCreate._special_column_validator)


class DimensionTableList(PaginationMixin):
    """
    Paginated list of DimensionTable
    """

    data: List[DimensionTableModel]


class DimensionTableUpdate(TableUpdate):
    """
    DimensionTable update payload schema
    """


class DimensionTableServiceUpdate(TableServiceUpdate):
    """
    DimensionTable service update schema
    """
