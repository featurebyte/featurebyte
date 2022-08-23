"""
FeatureJobSettingAnalysisTaskPayload schema
"""
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
