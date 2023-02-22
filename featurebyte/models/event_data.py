"""
This module contains EventData related models
"""
from __future__ import annotations

from typing import ClassVar, List, Optional, Type

from datetime import datetime

from pydantic import root_validator

from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import DataModel
from featurebyte.query_graph.model.common_table import BaseTableData
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import EventTableData


class FeatureJobSettingHistoryEntry(FeatureByteBaseModel):
    """
    Model for an entry in setting history

    created_at: datetime
        Datetime when the history entry is created
    setting: FeatureJobSetting
        Feature job setting that just becomes history (no longer used) at the time of the history entry creation
    """

    created_at: datetime
    setting: Optional[FeatureJobSetting]


class EventDataModel(EventTableData, DataModel):
    """
    Model for EventData entity

    id: PydanticObjectId
        EventData id of the object
    name : str
        Name of the EventData
    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of event data columns
    event_id_column: str
        Event ID column name
    event_timestamp_column: str
        Event timestamp column name
    default_feature_job_setting : Optional[FeatureJobSetting]
        Default feature job setting
    status : DataStatus
        Status of the EventData
    created_at : Optional[datetime]
        Datetime when the EventData was first saved or published
    updated_at: Optional[datetime]
        Datetime when the EventData object was last updated
    """

    default_feature_job_setting: Optional[FeatureJobSetting]
    _table_data_class: ClassVar[Type[BaseTableData]] = EventTableData

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="columns_info",
            expected_column_field_name_type_pairs=[
                ("event_timestamp_column", DBVarType.supported_timestamp_types()),
                ("record_creation_date_column", DBVarType.supported_timestamp_types()),
                ("event_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    @property
    def primary_key_columns(self) -> List[str]:
        if self.event_id_column:
            return [self.event_id_column]
        return []  # DEV-556: event_id_column should not be empty
