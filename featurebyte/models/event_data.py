"""
This module contains EventData related models
"""
from __future__ import annotations

from typing import Any, ClassVar, Dict, Optional, Tuple, Type

from datetime import datetime

from pydantic import root_validator, validator

from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import DataModel
from featurebyte.query_graph.model.common_table import BaseTableData
from featurebyte.query_graph.model.table import EventTableData


class FeatureJobSetting(FeatureByteBaseModel):
    """
    Model for Feature Job Setting

    The setting is defined by 3 main duration parameters.
    - Frequency: how often we want the job to run
    - Blind spot: the length of time that we deliberately want to from feature derivation. For example, if we
      calculate features at 10am, a blind spot of 2h means we only use data up to 8am.
      This is useful to account for data delay in the warehouse, as without this, the features can be noisy.
    - Time modulo frequency: an offset to specify when feature jobs are run.
    Note that these duration parameters are the same duration type strings that pandas accepts in pd.Timedelta().

    Some examples:
    - job run at 12am
        frequency 24h
        time modulo frequency 0

    - job run at 8am
        frequency 24h
        time modulo frequency 8h
    """

    __fbautodoc_proxy_class__: Tuple[str, str] = ("featurebyte.FeatureJobSetting", "")

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

    def to_seconds(self) -> Dict[str, Any]:
        """Convert job settings format using seconds as time unit

        Returns
        -------
        Dict[str, Any]
        """
        freq, time_mod_freq, blind_spot = validate_job_setting_parameters(
            frequency=self.frequency,
            time_modulo_frequency=self.time_modulo_frequency,
            blind_spot=self.blind_spot,
        )
        return {"frequency": freq, "time_modulo_frequency": time_mod_freq, "blind_spot": blind_spot}


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

    @validator("event_timestamp_column", "record_creation_date_column")
    @classmethod
    def _check_timestamp_column_exists(
        cls, value: Optional[str], values: dict[str, Any]
    ) -> Optional[str]:
        return DataModel.validate_column_exists(
            column_name=value, values=values, expected_types={DBVarType.TIMESTAMP}
        )

    @validator("event_id_column")
    @classmethod
    def _check_id_column_exists(cls, value: Optional[str], values: dict[str, Any]) -> Optional[str]:
        return DataModel.validate_column_exists(
            column_name=value, values=values, expected_types={DBVarType.VARCHAR, DBVarType.INT}
        )
