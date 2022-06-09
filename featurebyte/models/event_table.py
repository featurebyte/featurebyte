"""
This module contains EventTable related models
"""
from typing import List, Optional

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class SnowflakeSource(BaseModel):
    """Model for Snowflake data source information"""

    account: str
    warehouse: str
    database: str
    sf_schema: str  # schema shadows a BaseModel attribute


class FeatureJobSetting(BaseModel):
    """Model for Feature Job Setting"""

    blind_spot: str
    frequency: str
    time_modulo_frequency: str


class FeatureJobSettingHistoryEntry(BaseModel):
    """Model for an entry in setting history"""

    creation_date: datetime
    setting: FeatureJobSetting


class EventTableStatus(str, Enum):
    """EventTable status"""

    PUBLISHED = "PUBLISHED"
    DRAFT = "DRAFT"
    DEPRECATED = "DEPRECATED"


class EventTableModel(BaseModel):
    """
    Model for EventTable entity

    Parameters
    ----------
    name : str
        Name of the EventTable
    table_name : str
        Database table name
    source : SnowflakeSource
        Data warehouse connection information
    default_feature_job_setting : FeatureJobSetting
        Default feature job setting
    created_at : datetime
        Date when the EventTable was first saved or published
    history : list[FeatureJobSettingHistoryEntry]
        History of feature job settings
    status : EventTableStatus
        Status of the EventTable
    """

    name: str
    table_name: str
    source: SnowflakeSource
    event_timestamp_column: str
    record_creation_date_column: Optional[str]
    default_feature_job_setting: Optional[FeatureJobSetting]
    created_at: datetime
    history: List[FeatureJobSettingHistoryEntry]
    status: EventTableStatus
