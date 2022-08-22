"""
This module contains FeatureJobSettingAnalysis related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Any, Dict, List

from beanie import PydanticObjectId
from featurebyte_freeware.feature_job_analysis.schema import (
    AnalysisOptions,
    AnalysisParameters,
    FeatureJobSetting,
)

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class AnalysisResult(FeatureByteBaseModel):
    """
    Analysis results
    """

    stats_on_wh_jobs: Dict[str, Any]
    recommended_feature_job_setting: FeatureJobSetting


class FeatureJobSettingAnalysisModel(FeatureByteBaseDocumentModel):
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
