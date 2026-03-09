"""
TimeSeriesTable API payload schema
"""

from __future__ import annotations

from typing import List, Literal, Optional, Sequence

from pydantic import Field, StrictStr, ValidationInfo, field_validator

from featurebyte.enum import TableDataType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.time_series_table import TimeSeriesTableModel
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class TimeSeriesTableCreate(TableCreate):
    """
    TimeSeriesTable Creation Schema
    """

    type: Literal[TableDataType.TIME_SERIES_TABLE] = TableDataType.TIME_SERIES_TABLE
    series_id_column: Optional[StrictStr]
    series_id_columns: Optional[List[StrictStr]] = None
    reference_datetime_column: StrictStr
    reference_datetime_schema: TimestampSchema
    time_interval: TimeInterval
    default_feature_job_setting: Optional[CronFeatureJobSetting] = Field(default=None)

    # pydantic validators
    _special_columns_validator = field_validator(
        "record_creation_timestamp_column",
        "series_id_column",
        "reference_datetime_column",
        "datetime_partition_column",
        mode="after",
    )(TableCreate._special_column_validator)

    @field_validator("series_id_columns", mode="after")
    @classmethod
    def _series_id_columns_validator(
        cls, columns: Optional[List[StrictStr]], info: ValidationInfo
    ) -> Optional[List[StrictStr]]:
        if columns is not None:
            columns_info = info.data.get("columns_info")
            if columns_info is not None:
                known = {col.name for col in columns_info}
                for col in columns:
                    if col not in known:
                        raise ValueError(f"Column not found in table: {col}")
        return columns


class TimeSeriesTableList(PaginationMixin):
    """
    Paginated list of TimeSeriesTable
    """

    data: Sequence[TimeSeriesTableModel]


class TimeSeriesTableUpdateMixin(FeatureByteBaseModel):
    """
    TimeSeriesTable specific update schema
    """

    default_feature_job_setting: Optional[CronFeatureJobSetting] = Field(default=None)


class TimeSeriesTableUpdate(TimeSeriesTableUpdateMixin, TableUpdate):
    """
    TimeSeriesTable update payload schema
    """


class TimeSeriesTableServiceUpdate(TimeSeriesTableUpdateMixin, TableServiceUpdate):
    """
    TimeSeriesTable service update schema
    """

    is_global_series: Optional[bool] = Field(default=None)
