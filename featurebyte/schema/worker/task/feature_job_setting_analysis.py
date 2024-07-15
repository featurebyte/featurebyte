"""
FeatureJobSettingAnalysisTaskPayload schema
"""

from typing import ClassVar, Optional

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisCreate,
)
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class FeatureJobSettingAnalysisTaskPayload(BaseTaskPayload, FeatureJobSettingAnalysisCreate):
    """
    Feature Job Setting Analysis Task Payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.FEATURE_JOB_SETTING_ANALYSIS_CREATE
    output_collection_name: ClassVar[Optional[str]] = (
        FeatureJobSettingAnalysisModel.collection_name()
    )

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)


class FeatureJobSettingAnalysisBackTestTaskPayload(
    BaseTaskPayload, FeatureJobSettingAnalysisBacktest
):
    """
    Feature Job Setting Analysis Backtest Task Payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.FEATURE_JOB_SETTING_ANALYSIS_BACKTEST
    output_collection_name: ClassVar[str] = "temp_data"

    @property
    def task_output_path(self) -> Optional[str]:
        """
        Redirect route used to retrieve the task result

        Returns
        -------
        Optional[str]
        """
        return f"/{self.output_collection_name}?path=feature_job_setting_analysis/backtest/{self.output_document_id}"
