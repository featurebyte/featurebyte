"""
TimeSeriesTable API payload schema
"""

from __future__ import annotations

from typing import Literal, Optional, Sequence

from pydantic import Field, StrictStr, field_validator

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
