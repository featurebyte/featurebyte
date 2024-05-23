"""
EventTable API payload schema
"""

from __future__ import annotations

from typing import Any, Literal, Optional, Sequence

from pydantic import Field, StrictStr, root_validator, validator

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

    type: Literal[TableDataType.EVENT_TABLE] = Field(TableDataType.EVENT_TABLE, const=True)
    event_id_column: StrictStr
    event_timestamp_column: StrictStr
    event_timestamp_timezone_offset: Optional[StrictStr]
    event_timestamp_timezone_offset_column: Optional[StrictStr]
    default_feature_job_setting: Optional[FeatureJobSetting]

    # pydantic validators
    _special_columns_validator = validator(
        "record_creation_timestamp_column",
        "event_id_column",
        "event_timestamp_column",
        "event_timestamp_timezone_offset_column",
        allow_reuse=True,
    )(TableCreate._special_column_validator)

    @validator("event_timestamp_timezone_offset")
    @classmethod
    def _validate_event_timestamp_timezone_offset(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            validate_timezone_offset_string(value)
        return value

    @root_validator()
    @classmethod
    def _validate_event_timestamp_timezone_offset_parameters(
        cls, values: dict[str, Any]
    ) -> dict[str, Any]:
        if (
            values.get("event_timestamp_timezone_offset") is not None
            and values.get("event_timestamp_timezone_offset_column") is not None
        ):
            raise ValueError(
                "Cannot specify both event_timestamp_timezone_offset and event_timestamp_timezone_offset_column"
            )
        return values


class EventTableList(PaginationMixin):
    """
    Paginated list of EventTable
    """

    data: Sequence[EventTableModel]


class EventTableUpdateMixin(FeatureByteBaseModel):
    """
    EventTable specific update schema
    """

    default_feature_job_setting: Optional[FeatureJobSetting]


class EventTableUpdate(EventTableUpdateMixin, TableUpdate):
    """
    EventTable update payload schema
    """


class EventTableServiceUpdate(EventTableUpdateMixin, TableServiceUpdate):
    """
    EventTable service update schema
    """
