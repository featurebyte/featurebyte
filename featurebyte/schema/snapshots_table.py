"""
SnapshotsTable API payload schema
"""

from __future__ import annotations

from typing import Literal, Optional, Sequence

from pydantic import Field, StrictStr, field_validator

from featurebyte.enum import TableDataType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.snapshots_table import SnapshotsTableModel
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class SnapshotsTableCreate(TableCreate):
    """
    Snapshots Creation Schema
    """

    type: Literal[TableDataType.SNAPSHOTS_TABLE] = TableDataType.SNAPSHOTS_TABLE
    series_id_column: StrictStr
    snapshot_datetime_column: StrictStr
    snapshot_datetime_schema: TimestampSchema
    time_interval: TimeInterval
    default_feature_job_setting: Optional[CronFeatureJobSetting] = Field(default=None)

    # pydantic validators
    _special_columns_validator = field_validator(
        "record_creation_timestamp_column",
        "series_id_column",
        "snapshot_datetime_column",
        "datetime_partition_column",
        mode="after",
    )(TableCreate._special_column_validator)


class SnapshotsTableList(PaginationMixin):
    """
    Paginated list of SnapshotsTable
    """

    data: Sequence[SnapshotsTableModel]


class SnapshotsTableUpdateMixin(FeatureByteBaseModel):
    """
    SnapshotsTable specific update schema
    """

    default_feature_job_setting: Optional[CronFeatureJobSetting] = Field(default=None)


class SnapshotsTableUpdate(SnapshotsTableUpdateMixin, TableUpdate):
    """
    SnapshotsTable update payload schema
    """


class SnapshotsTableServiceUpdate(SnapshotsTableUpdateMixin, TableServiceUpdate):
    """
    SnapshotsTable service update schema
    """
