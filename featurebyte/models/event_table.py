"""
This module contains EventTable related models
"""
from typing import List, Optional, Union

from datetime import datetime
from enum import Enum

import pandas as pd
from pydantic import BaseModel, validator

from featurebyte.enum import SourceType


def validate_duration_string(duration_string: str) -> None:
    """Check whether the string is a valid duration

    Parameters
    ----------
    duration_string : str
        String to validate
    """
    pd.Timedelta(duration_string)


class SnowflakeDetails(BaseModel):
    """Model for Snowflake data source information"""

    account: str
    warehouse: str
    database: str
    sf_schema: str  # schema shadows a BaseModel attribute


class SQLiteDetails(BaseModel):
    """Model for SQLite data source information"""

    filename: str


class DatabaseSource(BaseModel):
    """Model for a database source"""

    type: SourceType
    details: Union[SnowflakeDetails, SQLiteDetails]


class FeatureJobSetting(BaseModel):
    """Model for Feature Job Setting"""

    blind_spot: str
    frequency: str
    time_modulo_frequency: str

    @validator("blind_spot", "frequency", "time_modulo_frequency")
    def valid_duration(cls, value: str) -> str:  # pylint: disable=no-self-argument
        """Validate that job setting values are valid

        Parameters
        ----------
        value : str
            Duration string for feature job setting

        Returns
        -------
        str
        """
        _ = cls
        validate_duration_string(value)
        return value


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
    source : DatabaseSource
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
    source: DatabaseSource
    event_timestamp_column: str
    record_creation_date_column: Optional[str]
    default_feature_job_setting: Optional[FeatureJobSetting]
    created_at: datetime
    history: List[FeatureJobSettingHistoryEntry]
    status: EventTableStatus
