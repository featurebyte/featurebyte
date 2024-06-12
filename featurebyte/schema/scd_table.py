"""
SCDTable API payload schema
"""

from __future__ import annotations

from typing import Literal, Optional, Sequence

from pydantic import Field, StrictStr, validator

from featurebyte.enum import TableDataType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
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
    default_feature_job_setting: Optional[FeatureJobSetting] = Field(
        default=FeatureJobSetting(blind_spot="0h", offset="0h", period="24h")
    )

    # pydantic validators
    _special_columns_validator = validator(
        "record_creation_timestamp_column",
        "natural_key_column",
        "surrogate_key_column",
        "effective_timestamp_column",
        "end_timestamp_column",
        "current_flag_column",
        allow_reuse=True,
    )(TableCreate._special_column_validator)


class SCDTableList(PaginationMixin):
    """
    Paginated list of SCDTable
    """

    data: Sequence[SCDTableModel]


class SCDDataUpdateMixin(FeatureByteBaseModel):
    """
    SCDTable specific update schema
    """

    end_timestamp_column: Optional[StrictStr]
    current_flag_column: Optional[StrictStr]
    default_feature_job_setting: Optional[FeatureJobSetting]


class SCDTableUpdate(SCDDataUpdateMixin, TableUpdate):
    """
    SCDTable update payload schema
    """


class SCDTableServiceUpdate(SCDDataUpdateMixin, TableServiceUpdate):
    """
    SCDTable service update schema
    """
