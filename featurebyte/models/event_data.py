"""
This module contains EventData related models
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, root_validator

from featurebyte.common.feature_job_setting_validation import validate_job_setting_parameters
from featurebyte.enum import SourceType


class SnowflakeDetails(BaseModel):
    """Model for Snowflake data source information"""

    account: str
    warehouse: str
    database: str
    sf_schema: str  # schema shadows a BaseModel attribute


class SQLiteDetails(BaseModel):
    """Model for SQLite data source information"""

    filename: str


DB_DETAILS_CLASS = {
    SourceType.SNOWFLAKE: SnowflakeDetails,
    SourceType.SQLITE: SQLiteDetails,
}


class DatabaseSourceModel(BaseModel):
    """Model for a database source"""

    type: SourceType
    details: Union[SnowflakeDetails, SQLiteDetails]

    def __hash__(self) -> int:
        """
        Hash function to support use as a dict key

        Returns
        -------
        int
            hash_value
        """
        return hash(str(self.type) + str(self.details))


class FeatureJobSetting(BaseModel):
    """Model for Feature Job Setting"""

    blind_spot: str
    frequency: str
    time_modulo_frequency: str

    # pylint: disable=no-self-argument
    @root_validator(pre=True)
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


class DatabaseTableModel(BaseModel):
    """Model for a table of database source"""

    tabular_source: Tuple[DatabaseSourceModel, str]


class EventDataModel(DatabaseTableModel):
    """
    Model for EventData entity

    Parameters
    ----------
    name : str
        Name of the EventData
    tabular_source : Tuple[DatabaseSourceModel, str]
        Data warehouse connection information & table name tuple
    event_timestamp_column: str
        Event timestamp column name
    record_creation_date_column: Optional[str]
        Record creation date column name
    default_feature_job_setting : Optional[FeatureJobSetting]
        Default feature job setting
    created_at : Optional[datetime]
        Date when the EventData was first saved or published
    history : list[FeatureJobSettingHistoryEntry]
        History of feature job settings
    status : Optional[EventDataStatus]
        Status of the EventData
    """

    name: str
    event_timestamp_column: str
    record_creation_date_column: Optional[str]
    default_feature_job_setting: Optional[FeatureJobSetting]
    created_at: Optional[datetime] = Field(default=None)
    history: List[FeatureJobSettingHistoryEntry] = Field(default_factory=list)
    status: Optional[EventDataStatus] = Field(default=None)
