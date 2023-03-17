"""
This module contains FeatureJobSettingAnalysis related models
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import json

import numpy as np
import numpy.typing as npt
import pandas as pd
from featurebyte_freeware.feature_job_analysis.schema import AnalysisData as BaseAnalysisData
from featurebyte_freeware.feature_job_analysis.schema import (
    AnalysisOptions,
    AnalysisParameters,
    AnalysisPlots,
)
from featurebyte_freeware.feature_job_analysis.schema import BacktestResult as BaseBacktestResult
from featurebyte_freeware.feature_job_analysis.schema import (
    BlindSpotSearchResult as BaseBlindSpotSearchResult,
)
from featurebyte_freeware.feature_job_analysis.schema import (
    EventLandingTimeResult as BaseEventLandingTimeResult,
)
from featurebyte_freeware.feature_job_analysis.schema import FeatureJobSetting
from featurebyte_freeware.feature_job_analysis.schema import MissingJobsInfo as BaseMissingJobsInfo
from pydantic import BaseModel, validator

from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteWorkspaceBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class AnalysisResult(FeatureByteBaseModel):
    """
    Analysis results
    """

    stats_on_wh_jobs: Dict[str, Any]
    recommended_feature_job_setting: FeatureJobSetting


class FeatureJobSettingAnalysisModel(FeatureByteWorkspaceBaseDocumentModel):
    """
    FeatureJobSettingAnalysis persistent model
    """

    event_data_id: PydanticObjectId
    analysis_options: AnalysisOptions
    analysis_parameters: AnalysisParameters
    analysis_result: AnalysisResult
    analysis_report: str

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "feature_job_setting_analysis"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]


class NumpyEncoder(json.JSONEncoder):
    """Special json encoder for numpy types"""

    def default(self, obj: Any) -> Any:  # pylint: disable=arguments-renamed
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


class DataFrameResultsMixin(BaseModel):
    """
    Handle conversion of json to DataFrame in results field
    """

    results: pd.DataFrame

    class Config:
        """
        Config for pydantic model
        """

        arbitrary_types_allowed: bool = True

    @validator("results", pre=True)
    @classmethod
    def convert_to_dataframe(cls, value: Any) -> pd.DataFrame:
        """
        Convert json to DataFrame

        Parameters
        ----------
        value: Any
            value to be converted

        Returns
        -------
        DataFrame
            DataFrame object
        """
        if isinstance(value, str):
            return pd.read_json(value)
        return value


class BlindSpotSearchResult(DataFrameResultsMixin, BaseBlindSpotSearchResult):
    """
    BlindSpotSearchResult with support for json deserialization
    """


class EventLandingTimeResult(DataFrameResultsMixin, BaseEventLandingTimeResult):
    """
    EventLandingTimeResult with support for json deserialization
    """


class BacktestResult(DataFrameResultsMixin, BaseBacktestResult):
    """
    BacktestResult with support for json deserialization
    """


class AnalysisResultsData(BaseModel):
    """
    Data heavy part of AnalysisResults
    """

    blind_spot_search_result: BlindSpotSearchResult
    blind_spot_search_exc_missing_jobs_result: Optional[BlindSpotSearchResult]
    event_landing_time_result: EventLandingTimeResult
    backtest_result: BacktestResult


class MissingJobsInfo(BaseMissingJobsInfo):
    """
    MissingJobsInfo with support for json deserialization
    """

    late_job_index: Optional[npt.NDArray[Any]]
    late_event_index: Optional[pd.Series]
    jobs_after_missing_jobs_index: npt.NDArray[Any]
    affected_jobs_index: npt.NDArray[Any]
    affected_event_index: Optional[pd.Series]

    @validator("late_job_index", "jobs_after_missing_jobs_index", "affected_jobs_index", pre=True)
    @classmethod
    def convert_to_ndarray(
        cls, value: Optional[Union[npt.NDArray[Any], str]]
    ) -> Optional[npt.NDArray[Any]]:
        """
        Convert json to ndarray

        Parameters
        ----------
        value: Any
            value to be converted

        Returns
        -------
        Optional[npt.NDArray[Any]]
            NDArray object
        """
        if isinstance(value, str):
            return np.array(json.loads(value))
        return value

    @validator("late_event_index", "affected_event_index", pre=True)
    @classmethod
    def convert_to_series(cls, value: Optional[Union[pd.Series, str]]) -> Optional[pd.Series]:
        """
        Convert json to Series

        Parameters
        ----------
        value: Any
            value to be converted

        Returns
        -------
        Optional[pd.Series]
            Series object
        """
        if isinstance(value, str):
            return pd.Series(json.loads(value))
        return value


class AnalysisData(BaseAnalysisData):
    """
    Analysis Data with support for json serialization
    """

    count_data: pd.DataFrame
    count_per_creation_date: pd.DataFrame
    missing_jobs_info: MissingJobsInfo

    @validator("count_data", "count_per_creation_date", pre=True)
    @classmethod
    def convert_to_dataframe(cls, value: Any) -> pd.DataFrame:
        """
        Convert json to DataFrame

        Parameters
        ----------
        value: Any
            value to be converted

        Returns
        -------
        DataFrame
            DataFrame object
        """
        if isinstance(value, str):
            return pd.read_json(value)
        return value


class FeatureJobSettingAnalysisData(BaseModel):
    """
    Store large objects from the analysis
    """

    analysis_plots: Optional[AnalysisPlots]
    analysis_data: Optional[AnalysisData]
    analysis_result: AnalysisResultsData

    class Config:
        """
        Config for pydantic model
        """

        arbitrary_types_allowed: bool = True
        # With this mapping, `ObjectId` type attribute is converted to string during json serialization.
        json_encoders = {
            pd.DataFrame: lambda df: df.to_json(),
            pd.Series: lambda series: series.to_json(),
            np.ndarray: lambda data: json.dumps(data, cls=NumpyEncoder),
        }
