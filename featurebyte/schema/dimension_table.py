"""
DimensionTable API payload schema
"""

from __future__ import annotations

from typing import List, Literal

from pydantic import Field, StrictStr, validator

from featurebyte.enum import TableDataType
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class DimensionTableCreate(TableCreate):
    """
    DimensionTable Creation Schema
    """

    type: Literal[TableDataType.DIMENSION_TABLE] = Field(TableDataType.DIMENSION_TABLE, const=True)
    dimension_id_column: StrictStr

    # pydantic validators
    _special_columns_validator = validator(
        "record_creation_timestamp_column", "dimension_id_column", allow_reuse=True
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
