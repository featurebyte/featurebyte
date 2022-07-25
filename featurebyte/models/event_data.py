"""
This module contains EventData related models
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime
from enum import Enum

from beanie import PydanticObjectId
from pydantic import Field, StrictStr, root_validator

from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel
from featurebyte.models.feature_store import DatabaseTableModel


class FeatureJobSetting(FeatureByteBaseModel):
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


class FeatureJobSettingHistoryEntry(FeatureByteBaseModel):
    """Model for an entry in setting history"""

    created_at: datetime
    setting: FeatureJobSetting


class EventDataStatus(str, Enum):
    """EventData status"""

    PUBLISHED = "PUBLISHED"
    DRAFT = "DRAFT"
    DEPRECATED = "DEPRECATED"


class EventDataModel(DatabaseTableModel, FeatureByteBaseDocumentModel):
    """
    Model for EventData entity

    id: PydanticObjectId
        EventData id of the object
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

    event_timestamp_column: StrictStr
    record_creation_date_column: Optional[StrictStr]
    column_entity_map: Optional[Dict[StrictStr, PydanticObjectId]] = Field(default=None)
    default_feature_job_setting: Optional[FeatureJobSetting]
    history: List[FeatureJobSettingHistoryEntry] = Field(default_factory=list)
    status: Optional[EventDataStatus] = Field(default=None)
