"""
This model contains the schema for backward compatible request responses.
"""

from typing import Any, Dict, Optional, Sequence

from pydantic import Field, root_validator

from featurebyte.models.feature_job_setting_analysis import (
    AnalysisResult,
    BackTestSummary,
    FeatureJobSettingAnalysisModel,
)
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSetting,
    FeatureJobSettingAnalysisList,
    FeatureJobSettingAnalysisRecord,
)


class FeatureJobSettingResponse(FeatureJobSetting):
    """
    Feature Job Setting Response
    """

    frequency: int
    job_time_modulo_frequency: int

    @root_validator(pre=True)
    @classmethod
    def _handle_backward_compatibility_by_populating_older_fields(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle backward compatibility by populating older fields

        Parameters
        ----------
        values : Dict[str, Any]
            Values to validate

        Returns
        -------
        Dict[str, Any]
            Validated values
        """
        if "period" in values:
            values["frequency"] = values["period"]
        if "offset" in values:
            values["job_time_modulo_frequency"] = values["offset"]
        return values


class FeatureJobSettingAnalysisRecordResponse(FeatureJobSettingAnalysisRecord):
    """
    Feature Job Setting Analysis Record Response
    """

    recommended_feature_job_setting: FeatureJobSettingResponse


class AnalysisResultResponse(AnalysisResult):
    """
    Analysis Result Response
    """

    recommended_feature_job_setting: FeatureJobSettingResponse


class BackTestSummaryResponse(BackTestSummary):
    """
    Back Test Summary Response
    """

    feature_job_setting: FeatureJobSettingResponse


class FeatureJobSettingAnalysisModelResponse(FeatureJobSettingAnalysisModel):
    """
    Feature Job Setting Analysis Model Response
    """

    analysis_result: AnalysisResultResponse
    backtest_summaries: Optional[Sequence[BackTestSummary]] = Field(default_factory=list)  # type: ignore


class FeatureJobSettingAnalysisListResponse(FeatureJobSettingAnalysisList):
    """
    Paginated list of Feature Job Setting Analysis
    """

    data: Sequence[FeatureJobSettingAnalysisRecordResponse]
