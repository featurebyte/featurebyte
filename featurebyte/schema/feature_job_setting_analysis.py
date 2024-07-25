"""
FeatureJobSettingAnalysis API payload schema
"""

from datetime import datetime
from typing import Any, Literal, Optional, Sequence, Union

from bson import ObjectId
from pandas import Timestamp
from pydantic import AfterValidator, BaseModel, Field, StrictStr, field_validator, model_validator
from typing_extensions import Annotated

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    NameStr,
    PydanticObjectId,
)
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.common.base import PaginationMixin

PandasTimestamp = Union[
    Timestamp,
    Annotated[str, AfterValidator(Timestamp)],
    Annotated[datetime, AfterValidator(Timestamp)],
]


class EventTableCandidate(FeatureByteBaseModel):
    """
    Event Table Candidate Schema
    """

    name: NameStr
    tabular_source: TabularSource
    event_timestamp_column: StrictStr
    record_creation_timestamp_column: StrictStr


class FeatureJobSettingAnalysisCreate(FeatureByteBaseModel):
    """
    Feature Job Setting Analysis Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: Optional[NameStr] = Field(default=None)
    event_table_id: Optional[PydanticObjectId] = Field(default=None)
    event_table_candidate: Optional[EventTableCandidate] = Field(default=None)
    analysis_date: Optional[datetime] = Field(default=None)
    analysis_length: int = Field(ge=3600, le=3600 * 24 * 28 * 6, default=3600 * 24 * 28)
    min_featurejob_period: int = Field(ge=60, le=3600 * 24 * 28, default=60)
    exclude_late_job: bool = Field(default=False)
    blind_spot_buffer_setting: int = Field(ge=5, le=3600 * 24 * 28, default=5)
    job_time_buffer_setting: Union[int, Literal["auto"]] = Field(default="auto")
    late_data_allowance: float = Field(gt=0, le=0.5, default=0.005 / 100)

    @model_validator(mode="before")
    @classmethod
    def validate_event_table_parameters(cls, values: Any) -> Any:
        """
        Validate Event Table parameters are provided

        Parameters
        ----------
        values : Any
            Values to validate

        Returns
        -------
        Any
            Validated values

        Raises
        ------
        ValueError
            If neither event_table_id or event_table_candidate is provided
        """
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        event_table_id = values.get("event_table_id")
        event_table_candidate = values.get("event_table_candidate")
        if not (event_table_id or event_table_candidate):
            raise ValueError("Either event_table_id or event_table_candidate is required")
        return values


class AnalysisOptions(FeatureByteBaseModel):
    """
    Analysis options
    """

    analysis_date: PandasTimestamp
    analysis_start: PandasTimestamp
    analysis_length: int
    blind_spot_buffer_setting: int
    exclude_late_job: bool
    job_time_buffer_setting: Union[int, Literal["auto"]]
    late_data_allowance: float
    min_featurejob_period: int


class AnalysisParameters(FeatureByteBaseModel):
    """
    Analysis parameters
    """

    event_table_name: str
    creation_date_column: str
    event_timestamp_column: str
    blind_spot_buffer: int
    job_time_buffer: int
    frequency: int
    granularity: int
    reading_at: int
    job_time_modulo_frequency: int


class FeatureJobSetting(FeatureByteBaseModel):
    """
    Feature Job Setting
    """

    period: int
    offset: int
    blind_spot: int
    feature_cutoff_modulo_frequency: int

    @model_validator(mode="before")
    @classmethod
    def _handle_backward_compatibility(cls, values: Any) -> Any:
        """
        Handle backward compatibility

        Parameters
        ----------
        values : Any
            Values to validate

        Returns
        -------
        Any
        """
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        if "frequency" in values:
            values["period"] = values.pop("frequency")
        if "job_time_modulo_frequency" in values:
            values["offset"] = values.pop("job_time_modulo_frequency")
        return values


class FeatureJobSettingAnalysisWHJobFrequency(FeatureByteBaseModel):
    """
    FeatureJobSettingAnalysisWHJobFrequency Schema
    """

    best_estimate: int
    confidence: str


class FeatureJobSettingAnalysisWHJobInterval(FeatureByteBaseModel):
    """
    FeatureJobSettingAnalysisWHJobInterval Schema
    """

    avg: float
    median: float
    min: float
    max: float


class FeatureJobSettingAnalysisWHJobTimeModuloFrequency(FeatureByteBaseModel):
    """
    FeatureJobSettingAnalysisWHJobTimeModuloFrequency Schema
    """

    starts: int
    ends: int
    ends_wo_late: int
    job_at_end_of_cycle: bool

    @field_validator("starts", "ends", "ends_wo_late", mode="before")
    @classmethod
    def _coerce_float_to_int(cls, value: Any) -> int:
        return int(value)


class FeatureJobSettingAnalysisWarehouseRecord(FeatureByteBaseDocumentModel):
    """
    FeatureJobSettingAnalysis persistent record with warehouse jobs info
    """

    job_frequency: FeatureJobSettingAnalysisWHJobFrequency
    job_interval: FeatureJobSettingAnalysisWHJobInterval
    job_time_modulo_frequency: FeatureJobSettingAnalysisWHJobTimeModuloFrequency
    jobs_count: int
    missing_jobs_count: int


class FeatureJobSettingAnalysisRecord(FeatureByteBaseDocumentModel):
    """
    FeatureJobSettingAnalysis persistent record without report
    """

    event_table_id: PydanticObjectId
    analysis_options: AnalysisOptions
    recommended_feature_job_setting: FeatureJobSetting
    stats_on_wh_jobs: FeatureJobSettingAnalysisWarehouseRecord

    @model_validator(mode="before")
    @classmethod
    def _extract_recommended_feature_job_setting(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        if "recommended_feature_job_setting" not in values:
            values["recommended_feature_job_setting"] = values["analysis_result"][
                "recommended_feature_job_setting"
            ]

        # expose statistics on warehouse jobs
        if "stats_on_wh_jobs" not in values:
            values["stats_on_wh_jobs"] = values["analysis_result"]["stats_on_wh_jobs"]

        return values


class FeatureJobSettingAnalysisList(PaginationMixin):
    """
    Paginated list of Feature Job Setting Analysis
    """

    data: Sequence[FeatureJobSettingAnalysisRecord]


class FeatureJobSettingAnalysisBacktest(FeatureByteBaseModel):
    """
    Feature Job Setting Analysis Backtest Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    feature_job_setting_analysis_id: PydanticObjectId
    period: int = Field(ge=60, le=3600 * 24 * 28)
    offset: int = Field(ge=0, le=3600 * 24 * 28)
    blind_spot: int = Field(ge=0, le=3600 * 24 * 28)

    @model_validator(mode="before")
    @classmethod
    def _handle_backward_compatibility(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        if "frequency" in values:
            values["period"] = values.pop("frequency")
        if "job_time_modulo_frequency" in values:
            values["offset"] = values.pop("job_time_modulo_frequency")
        return values
