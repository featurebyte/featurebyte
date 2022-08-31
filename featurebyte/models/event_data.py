"""
This module contains EventData related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime

from pydantic import Field, StrictStr, root_validator

from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.enum import OrderedStrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.feature_store import ColumnInfo, DatabaseTableModel


class FeatureJobSetting(FeatureByteBaseModel):
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


class EventDataStatus(OrderedStrEnum):
    """EventData status"""

    DEPRECATED = "DEPRECATED"
    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"


class EventDataModel(DatabaseTableModel, FeatureByteBaseDocumentModel):
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
    event_timestamp_column: str
        Event timestamp column name
    record_creation_date_column: Optional[str]
        Record creation date column name
    default_feature_job_setting : Optional[FeatureJobSetting]
        Default feature job setting
    created_at : Optional[datetime]
        Datetime when the EventData was first saved or published
    status : Optional[EventDataStatus]
        Status of the EventData
    """

    columns_info: List[ColumnInfo]
    event_timestamp_column: StrictStr
    record_creation_date_column: Optional[StrictStr]
    default_feature_job_setting: Optional[FeatureJobSetting]
    status: EventDataStatus = Field(default=EventDataStatus.DRAFT, allow_mutation=False)

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "event_data"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("tabular_source",),
                conflict_fields_signature={"tabular_source": ["tabular_source"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
