"""
SCDData API payload schema
"""
from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.scd_data import SCDDataModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.tabular_data import DataCreate, DataUpdate


class SCDDataCreate(DataCreate):
    """
    SCDData Creation Schema
    """

    type: Literal[TableDataType.SCD_DATA] = Field(TableDataType.SCD_DATA, const=True)
    natural_key_column: StrictStr
    surrogate_key_column: Optional[StrictStr]
    effective_timestamp_column: StrictStr
    end_timestamp_column: Optional[StrictStr]
    current_flag_column: Optional[StrictStr]


class SCDDataList(PaginationMixin):
    """
    Paginated list of SCD Data
    """

    data: List[SCDDataModel]


class SCDDataUpdate(DataUpdate):
    """
    SCDData Update Schema
    """

    end_timestamp_column: Optional[StrictStr]
    current_flag_column: Optional[StrictStr]
