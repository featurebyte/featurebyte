"""
This module contains default job setting initialization task
"""
from __future__ import annotations

from typing import ClassVar

from beanie import PydanticObjectId

from featurebyte.worker.enum import Command
from featurebyte.worker.task.base import BaseTask, BaseTaskPayload


class DefaultJobSettingAnalysisPayload(BaseTaskPayload):
    """
    DefaultJobSettingAnalysisPayload model
    """

    # pylint: disable=too-few-public-methods

    event_data_id: PydanticObjectId
    collection_name: ClassVar[str] = "default_job_setting_initial"
    command: ClassVar[Command] = Command.DEFAULT_JOB_SETTINGS_ANALYSIS


class DefaultJobSettingsAnalysis(BaseTask):
    """
    DefaultJobSettingAnalysis task
    """

    # pylint: disable=too-few-public-methods

    payload_class = DefaultJobSettingAnalysisPayload
    command = payload_class.command

    def execute(self) -> None:
        """
        Perform default job settings initialization related task
        """
