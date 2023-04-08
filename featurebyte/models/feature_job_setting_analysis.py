"""
This module contains FeatureJobSettingAnalysis related models
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pymongo
from pydantic import BaseModel

from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.schema.feature_job_setting_analysis import (
    AnalysisOptions,
    AnalysisParameters,
    FeatureJobSetting,
)


class BlindSpotSearchResult(BaseModel):
    """
    BlindSpotSearchResult with support for json deserialization
    """

    pct_late_data: float
    optimal_blind_spot: int
    results: str
    plot: str
    thresholds: List[Tuple[float, int]]
    warnings: List[str]


class AnalysisResult(FeatureByteBaseModel):
    """
    Analysis results
    """

    stats_on_wh_jobs: Dict[str, Any]
    recommended_feature_job_setting: FeatureJobSetting


class AnalysisPlots(BaseModel):
    """
    Analysis plots
    """

    wh_job_time_modulo_frequency_hist: Dict[str, Any]
    wh_intervals_hist: Optional[Dict[str, Any]]
    wh_intervals_exclude_missing_jobs_hist: Optional[Dict[str, Any]]
    affected_jobs_record_age: Optional[Dict[str, Any]]


class FeatureJobSettingAnalysisModel(FeatureByteCatalogBaseDocumentModel):
    """
    FeatureJobSettingAnalysis persistent model
    """

    event_table_id: PydanticObjectId
    analysis_options: AnalysisOptions
    analysis_parameters: AnalysisParameters
    analysis_result: AnalysisResult
    analysis_report: str

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
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

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("event_table_id"),
            [
                ("name", pymongo.TEXT),
            ],
        ]


class BacktestResult(BaseModel):
    """
    Backtest result
    """

    results: str
    job_with_issues_count: int
    warnings: List[str]
    plot: Optional[str]


class EventLandingTimeResult(BaseModel):
    """
    EventLandingTimeResult with support for json deserialization
    """

    results: str
    plot: str
    thresholds: List[Tuple[int, float, int]]
    warnings: List[str]


class AnalysisResultsData(BaseModel):
    """
    Data heavy part of AnalysisResults
    """

    blind_spot_search_result: BlindSpotSearchResult
    blind_spot_search_exc_missing_jobs_result: Optional[BlindSpotSearchResult]
    event_landing_time_result: EventLandingTimeResult
    backtest_result: BacktestResult


class MissingJobsInfo(BaseModel):
    """
    MissingJobsInfo with support for json deserialization
    """

    normal_age_max: float
    late_job_index: Optional[str]
    late_event_index: Optional[str]
    jobs_after_missing_jobs_index: str
    affected_jobs_index: str
    affected_event_index: Optional[str]


class AnalysisData(BaseModel):
    """
    Analysis Data with support for json serialization
    """

    count_data: str
    count_per_creation_date: str
    missing_jobs_info: MissingJobsInfo


class FeatureJobSettingAnalysisData(BaseModel):
    """
    Store large objects from the analysis
    """

    analysis_plots: Optional[AnalysisPlots]
    analysis_data: Optional[AnalysisData]
    analysis_result: AnalysisResultsData
