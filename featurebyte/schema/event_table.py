"""
EventTable API payload schema
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

from pydantic import Field, StrictStr, field_validator, model_validator

from featurebyte.common.model_util import validate_timezone_offset_string
from featurebyte.enum import TableDataType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.event_table import EventTableModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class EventTableCreate(TableCreate):
    """
    EventTable Creation Schema
    """

    type: Literal[TableDataType.EVENT_TABLE] = TableDataType.EVENT_TABLE
    event_id_column: StrictStr
    event_timestamp_column: StrictStr
    event_timestamp_timezone_offset: StrictStr | None = Field(default=None)
    event_timestamp_timezone_offset_column: StrictStr | None = Field(default=None)
    default_feature_job_setting: FeatureJobSetting | None = Field(default=None)

    # pydantic validators
    _special_columns_validator = field_validator(
        "record_creation_timestamp_column",
        "event_id_column",
        "event_timestamp_column",
        "event_timestamp_timezone_offset_column",
        mode="after",
    )(TableCreate._special_column_validator)

    @field_validator("event_timestamp_timezone_offset")
    @staticmethod
    def _validate_event_timestamp_timezone_offset(value: str | None) -> str | None:
        if value is not None:
            validate_timezone_offset_string(value)
        return value

    @model_validator(mode="after")
    def _validate_event_timestamp_timezone_offset_parameters(self) -> EventTableCreate:
        if (
            self.event_timestamp_timezone_offset is not None
            and self.event_timestamp_timezone_offset_column is not None
        ):
            raise ValueError(
                "Cannot specify both event_timestamp_timezone_offset and event_timestamp_timezone_offset_column"
            )
        return self


class EventTableList(PaginationMixin):
    """
    Paginated list of EventTable
    """

    data: Sequence[EventTableModel]


class EventTableUpdateMixin(FeatureByteBaseModel):
    """
    EventTable specific update schema
    """

    default_feature_job_setting: FeatureJobSetting | None = Field(default=None)


class EventTableUpdate(EventTableUpdateMixin, TableUpdate):
    """
    EventTable update payload schema
    """


class EventTableServiceUpdate(EventTableUpdateMixin, TableServiceUpdate):
    """
    EventTable service update schema
    """
