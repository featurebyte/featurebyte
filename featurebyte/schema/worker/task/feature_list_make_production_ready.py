"""
Feature list make production ready task payload
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class FeatureListMakeProductionReadyTaskPayload(BaseTaskPayload):
    """
    Feature list make production ready task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.FEATURE_LIST_MAKE_PRODUCTION_READY
    output_collection_name: ClassVar[str] = FeatureListModel.collection_name()

    # instance variables
    task_type: TaskType = Field(default=TaskType.IO_TASK)
    ignore_guardrails: bool = Field(default=False)
    feature_list_id: PydanticObjectId
