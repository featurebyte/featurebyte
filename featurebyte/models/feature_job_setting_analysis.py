"""
This module contains FeatureJobSettingAnalysis related models
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

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
    thresholds: list[tuple[float, int]]
    warnings: list[str]


class AnalysisResult(FeatureByteBaseModel):
    """
    Analysis results
    """

    stats_on_wh_jobs: dict[str, Any]
    recommended_feature_job_setting: FeatureJobSetting


class AnalysisPlots(FeatureByteBaseModel):
    """
    Analysis plots
    """

    wh_job_time_modulo_frequency_hist: dict[str, Any]
    wh_intervals_hist: dict[str, Any] | None = Field(default=None)
    wh_intervals_exclude_missing_jobs_hist: dict[str, Any] | None = Field(default=None)
    affected_jobs_record_age: dict[str, Any] | None = Field(default=None)


class BackTestSummary(FeatureByteBaseModel):
    """
    BackTestSummary model
    """

    output_document_id: PydanticObjectId
    user_id: PydanticObjectId | None = Field(default=None)
    created_at: datetime
    feature_job_setting: FeatureJobSetting
    total_pct_late_data: float
    pct_incomplete_jobs: float


class FeatureJobSettingAnalysisModel(FeatureByteCatalogBaseDocumentModel):
    """
    FeatureJobSettingAnalysis persistent model
    """

    event_table_id: PydanticObjectId | None = Field(default=None)
    event_table_candidate: EventTableCandidate | None = Field(default=None)
    analysis_options: AnalysisOptions
    analysis_parameters: AnalysisParameters
    analysis_result: AnalysisResult
    analysis_report: str
    backtest_summaries: list[BackTestSummary] | None = Field(default_factory=list)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature_job_setting_analysis"
        unique_constraints: list[UniqueValuesConstraint] = [
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
    warnings: list[str]
    plot: str | None = Field(default=None)


class EventLandingTimeResult(FeatureByteBaseModel):
    """
    EventLandingTimeResult with support for json deserialization
    """

    results: str
    plot: str
    thresholds: list[tuple[int, float, int]]
    warnings: list[str]


class AnalysisResultsData(FeatureByteBaseModel):
    """
    Data heavy part of AnalysisResults
    """

    blind_spot_search_result: BlindSpotSearchResult
    blind_spot_search_exc_missing_jobs_result: BlindSpotSearchResult | None = Field(default=None)
    event_landing_time_result: EventLandingTimeResult
    backtest_result: BacktestResult


class MissingJobsInfo(FeatureByteBaseModel):
    """
    MissingJobsInfo with support for json deserialization
    """

    normal_age_max: float
    late_job_index: str | None = Field(default=None)
    late_event_index: str | None = Field(default=None)
    jobs_after_missing_jobs_index: str
    affected_jobs_index: str
    affected_event_index: str | None = Field(default=None)


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

    analysis_plots: AnalysisPlots | None = Field(default=None)
    analysis_data: AnalysisData | None = Field(default=None)
    analysis_result: AnalysisResultsData
