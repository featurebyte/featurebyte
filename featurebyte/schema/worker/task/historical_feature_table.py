"""
HistoricalFeaturesTaskPayload schema
"""

from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class HistoricalFeatureTableTaskPayload(BaseTaskPayload, HistoricalFeatureTableCreate):
    """
    HistoricalFeatureTable creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.HISTORICAL_FEATURE_TABLE_CREATE
    output_collection_name: ClassVar[str] = HistoricalFeatureTableModel.collection_name()
    is_revocable: ClassVar[bool] = True
    is_rerunnable: ClassVar[bool] = True

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    observation_set_storage_path: Optional[str] = Field(default=None)
