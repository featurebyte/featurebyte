"""
FeatureJobSettingAnalysis API payload schema
"""
from typing import List, Literal, Optional, Union

from datetime import datetime

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.schema.common.base import PaginationMixin


class FeatureJobSettingAnalysisCreate(FeatureByteBaseModel):
    """
    Feature Job Setting Analysis Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: Optional[StrictStr]
    event_data_id: PydanticObjectId
    analysis_date: Optional[datetime] = Field(default=None)
    analysis_length: int = Field(ge=3600, le=3600 * 24 * 28 * 6, default=3600 * 24 * 28)
    min_featurejob_period: int = Field(ge=60, le=3600 * 24 * 28, default=60)
    exclude_late_job: bool = Field(default=False)
    blind_spot_buffer_setting: int = Field(ge=5, le=3600 * 24 * 28, default=5)
    job_time_buffer_setting: Union[int, Literal["auto"]] = Field(default="auto")
    late_data_allowance: float = Field(gt=0, le=0.5, default=0.005 / 100)


class FeatureJobSettingAnalysisList(PaginationMixin):
    """
    Paginated list of Feature Job Setting Analysis
    """

    data: List[FeatureJobSettingAnalysisModel]


class FeatureJobSettingAnalysisBacktest(FeatureByteBaseModel):
    """
    Feature Job Setting Analysis Backtest Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    feature_job_setting_analysis_id: PydanticObjectId
    frequency: int = Field(ge=60, le=3600 * 24 * 28)
    job_time_modulo_frequency: int = Field(ge=0, le=3600 * 24 * 28)
    blind_spot: int = Field(ge=0, le=3600 * 24 * 28)
