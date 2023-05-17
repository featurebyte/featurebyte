"""
SCDTable API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import Field, StrictStr, root_validator

from featurebyte.enum import TableDataType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class SCDTableCreate(TableCreate):
    """
    SCDTable Creation Schema
    """

    type: Literal[TableDataType.SCD_TABLE] = Field(TableDataType.SCD_TABLE, const=True)
    natural_key_column: StrictStr
    surrogate_key_column: Optional[StrictStr]
    effective_timestamp_column: StrictStr
    end_timestamp_column: Optional[StrictStr]
    current_flag_column: Optional[StrictStr]

    @root_validator
    @classmethod
    def _validate_scd_table_settings(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        effective_timestamp_column = values.get("effective_timestamp_column")
        end_timestamp_column = values.get("end_timestamp_column")
        if effective_timestamp_column and end_timestamp_column:
            if effective_timestamp_column == end_timestamp_column:
                raise ValueError(
                    "effective_timestamp_column and end_timestamp_column cannot be same"
                )
        return values


class SCDTableList(PaginationMixin):
    """
    Paginated list of SCDTable
    """

    data: List[SCDTableModel]


class SCDDataUpdateMixin(FeatureByteBaseModel):
    """
    SCDTable specific update schema
    """

    end_timestamp_column: Optional[StrictStr]
    current_flag_column: Optional[StrictStr]


class SCDTableUpdate(SCDDataUpdateMixin, TableUpdate):
    """
    SCDTable update payload schema
    """


class SCDTableServiceUpdate(SCDDataUpdateMixin, TableServiceUpdate):
    """
    SCDTable service update schema
    """
