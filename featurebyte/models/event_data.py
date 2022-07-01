"""
This module contains EventData related models
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, root_validator

from featurebyte.common.feature_job_setting_validation import validate_job_setting_parameters
from featurebyte.models.feature_store import DatabaseTableModel


class FeatureJobSetting(BaseModel):
    """Model for Feature Job Setting"""

    blind_spot: str
    frequency: str
    time_modulo_frequency: str

    @root_validator(pre=True)
    @classmethod
    def validate_setting_parameters(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate feature job setting parameters

        Parameters
        ----------
        values : dict
            Parameter values

        Returns
        -------
        dict
        """
        _ = cls
        validate_job_setting_parameters(
            frequency=values["frequency"],
            time_modulo_frequency=values["time_modulo_frequency"],
            blind_spot=values["blind_spot"],
        )
        return values


class FeatureJobSettingHistoryEntry(BaseModel):
    """Model for an entry in setting history"""

    creation_date: datetime
    setting: FeatureJobSetting


class EventDataStatus(str, Enum):
    """EventData status"""

    PUBLISHED = "PUBLISHED"
    DRAFT = "DRAFT"
    DEPRECATED = "DEPRECATED"


class EventDataModel(DatabaseTableModel):
    """
    Model for EventData entity

    name : str
        Name of the EventData
    tabular_source : Tuple[FeatureStoreModel, TableDetails]
        Data warehouse connection information & table name tuple
    event_timestamp_column: str
        Event timestamp column name
    record_creation_date_column: Optional[str]
        Record creation date column name
    column_entity_map: Dict[str, str]
        Column name to entity name mapping
    default_feature_job_setting : Optional[FeatureJobSetting]
        Default feature job setting
    created_at : Optional[datetime]
        Datetime when the EventData was first saved or published
    history : list[FeatureJobSettingHistoryEntry]
        History of feature job settings
    status : Optional[EventDataStatus]
        Status of the EventData
    """

    name: str
    event_timestamp_column: str
    record_creation_date_column: Optional[str]
    column_entity_map: Dict[str, str] = Field(default_factory=dict)
    default_feature_job_setting: Optional[FeatureJobSetting]
    created_at: Optional[datetime] = Field(default=None)
    history: List[FeatureJobSettingHistoryEntry] = Field(default_factory=list)
    status: Optional[EventDataStatus] = Field(default=None)
