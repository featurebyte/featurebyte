"""
This module contains EventData related models
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, root_validator

from featurebyte.common.feature_job_setting_validation import validate_job_setting_parameters
from featurebyte.models.feature_store import DatabaseTableModel, FeatureStoreModel


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


###### Temporary Definition of Feature and TileSpec ######
class TileSpec(BaseModel):
    """
    Temporary Model for TileSpec
    Parameters
    ----------
    tile_id: str
        hash value of tile id and name
    time_modulo_frequency_seconds: int
        time modulo seconds for the tile
    blind_spot_seconds: int
        blind spot seconds for the tile
    frequency_minute: int
        frequency minute for the tile
    tile_sql: str
        sql for tile generation
    column_names: str
        comma separated string of column names for the tile table
    """

    tile_id: str
    tile_sql: str
    column_names: str

    time_modulo_frequency_second: int
    blind_spot_second: int
    frequency_minute: int


class Feature(BaseModel):
    """
    Temporary Model for Feature
    Parameters
    ----------
    name : str
        Name of the Feature
    readiness : EventDataStatus
        Status of the Feature
    version : str
        feature version
    is_default : bool
        whether it is the default feature
    online_enabled: bool
        whether feature is online enabled or not
    datasource: FeatureStoreModel
        datasource instance
    """

    name: str
    readiness: str
    version: str
    is_default: bool

    tile_specs: List[TileSpec] = Field(default=[])

    online_enabled: bool
    datasource: FeatureStoreModel


class TileType(str, Enum):
    """Tile Type"""

    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"
