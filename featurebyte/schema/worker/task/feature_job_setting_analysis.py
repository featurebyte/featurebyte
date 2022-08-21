"""
FeatureJobSettingAnalysisTaskPayload schema
"""
from enum import Enum

from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.schema.feature_job_setting_analysis import FeatureJobSettingAnalysisCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload


class FeatureJobSettingAnalysisCommand(str, Enum):
    """
    Command enum for FeatureJobSettingAnalysis
    """

    CREATE = "FeatureJobSettingAnalysis.Create"


class FeatureJobSettingAnalysisTaskPayload(BaseTaskPayload, FeatureJobSettingAnalysisCreate):
    """
    Feature Job Setting Analysis Task Payload
    """

    output_collection_name = FeatureJobSettingAnalysisModel.collection_name()
    command = FeatureJobSettingAnalysisCommand.CREATE
