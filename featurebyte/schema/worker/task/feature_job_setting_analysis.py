"""
FeatureJobSettingAnalysisTaskPayload schema
"""
from typing import Optional

from featurebyte.enum import WorkerCommand
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisCreate,
)
from featurebyte.schema.worker.task.base import BaseTaskPayload


class FeatureJobSettingAnalysisTaskPayload(BaseTaskPayload, FeatureJobSettingAnalysisCreate):
    """
    Feature Job Setting Analysis Task Payload
    """

    output_collection_name = FeatureJobSettingAnalysisModel.collection_name()
    command = WorkerCommand.FEATURE_JOB_SETTING_ANALYSIS_CREATE


class FeatureJobSettingAnalysisBackTestTaskPayload(
    BaseTaskPayload, FeatureJobSettingAnalysisBacktest
):
    """
    Feature Job Setting Analysis Backtest Task Payload
    """

    output_collection_name = "temp_data"
    command = WorkerCommand.FEATURE_JOB_SETTING_ANALYSIS_BACKTEST

    @property
    def task_output_path(self) -> Optional[str]:
        """
        Redirect route used to retrieve the task result

        Returns
        -------
        Optional[str]
        """
        return f"/{self.output_collection_name}?path=feature_job_setting_analysis/backtest/{self.output_document_id}"
