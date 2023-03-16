"""
SCDData API payload schema
"""
from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class SCDTableCreate(TableCreate):
    """
    SCDData Creation Schema
    """

    type: Literal[TableDataType.SCD_DATA] = Field(TableDataType.SCD_DATA, const=True)
    natural_key_column: StrictStr
    surrogate_key_column: Optional[StrictStr]
    effective_timestamp_column: StrictStr
    end_timestamp_column: Optional[StrictStr]
    current_flag_column: Optional[StrictStr]


class SCDTableList(PaginationMixin):
    """
    Paginated list of SCDTable
    """

    data: List[SCDTableModel]


class SCDDataUpdateMixin(FeatureByteBaseModel):
    """
    SCDData specific update schema
    """

    end_timestamp_column: Optional[StrictStr]
    current_flag_column: Optional[StrictStr]


class SCDTableUpdate(SCDDataUpdateMixin, TableUpdate):
    """
    SCDData update payload schema
    """


class SCDTableServiceUpdate(SCDDataUpdateMixin, TableServiceUpdate):
    """
    SCDData service update schema
    """
