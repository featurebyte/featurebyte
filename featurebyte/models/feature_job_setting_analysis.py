"""
This module contains FeatureJobSettingAnalysis related models
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pymongo
from pydantic import Field

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
    EventTableCandidate,
    FeatureJobSetting,
)


class BlindSpotSearchResult(FeatureByteBaseModel):
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


class AnalysisPlots(FeatureByteBaseModel):
    """
    Analysis plots
    """

    wh_job_time_modulo_frequency_hist: Dict[str, Any]
    wh_intervals_hist: Optional[Dict[str, Any]] = Field(default=None)
    wh_intervals_exclude_missing_jobs_hist: Optional[Dict[str, Any]] = Field(default=None)
    affected_jobs_record_age: Optional[Dict[str, Any]] = Field(default=None)


class BackTestSummary(FeatureByteBaseModel):
    """
    BackTestSummary model
    """

    output_document_id: PydanticObjectId
    user_id: Optional[PydanticObjectId] = Field(default=None)
    created_at: datetime
    feature_job_setting: FeatureJobSetting
    total_pct_late_data: float
    pct_incomplete_jobs: float


class FeatureJobSettingAnalysisModel(FeatureByteCatalogBaseDocumentModel):
    """
    FeatureJobSettingAnalysis persistent model
    """

    event_table_id: Optional[PydanticObjectId] = Field(default=None)
    event_table_candidate: Optional[EventTableCandidate] = Field(default=None)
    analysis_options: AnalysisOptions
    analysis_parameters: AnalysisParameters
    analysis_result: AnalysisResult
    analysis_report: str
    backtest_summaries: Optional[List[BackTestSummary]] = Field(default_factory=list)

    @classmethod
    def _get_remote_attribute_paths(cls, document_dict: Dict[str, Any]) -> List[Path]:
        return [Path(f"feature_job_setting_analysis/{document_dict['_id']}/data.json")]

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
                ("description", pymongo.TEXT),
            ],
        ]


class BacktestResult(FeatureByteBaseModel):
    """
    Backtest result
    """

    results: str
    job_with_issues_count: int
    warnings: List[str]
    plot: Optional[str] = Field(default=None)


class EventLandingTimeResult(FeatureByteBaseModel):
    """
    EventLandingTimeResult with support for json deserialization
    """

    results: str
    plot: str
    thresholds: List[Tuple[int, float, int]]
    warnings: List[str]


class AnalysisResultsData(FeatureByteBaseModel):
    """
    Data heavy part of AnalysisResults
    """

    blind_spot_search_result: BlindSpotSearchResult
    blind_spot_search_exc_missing_jobs_result: Optional[BlindSpotSearchResult] = Field(default=None)
    event_landing_time_result: EventLandingTimeResult
    backtest_result: BacktestResult


class MissingJobsInfo(FeatureByteBaseModel):
    """
    MissingJobsInfo with support for json deserialization
    """

    normal_age_max: float
    late_job_index: Optional[str] = Field(default=None)
    late_event_index: Optional[str] = Field(default=None)
    jobs_after_missing_jobs_index: str
    affected_jobs_index: str
    affected_event_index: Optional[str] = Field(default=None)


class AnalysisData(FeatureByteBaseModel):
    """
    Analysis Data with support for json serialization
    """

    count_data: str
    count_per_creation_date: str
    missing_jobs_info: MissingJobsInfo


class FeatureJobSettingAnalysisData(FeatureByteBaseModel):
    """
    Store large objects from the analysis
    """

    analysis_plots: Optional[AnalysisPlots] = Field(default=None)
    analysis_data: Optional[AnalysisData] = Field(default=None)
    analysis_result: AnalysisResultsData
