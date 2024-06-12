"""
Feature list creation creation schema
"""

from __future__ import annotations

from typing import ClassVar, List, Union

from pydantic import Field

from featurebyte.enum import ConflictResolution, WorkerCommand
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class FeatureParameters(FeatureByteBaseModel):
    """Feature parameters"""

    id: PydanticObjectId
    name: NameStr


class FeaturesParameters(FeatureByteBaseModel):
    """Feature list feature parameters"""

    features: Union[List[FeatureParameters], List[PydanticObjectId]]


class FeatureListCreateTaskPayload(BaseTaskPayload):
    """
    Feature list creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.FEATURE_LIST_CREATE
    output_collection_name: ClassVar[str] = FeatureListModel.collection_name()

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    feature_list_id: PydanticObjectId
    feature_list_name: NameStr
    features_parameters_path: str
    features_conflict_resolution: ConflictResolution
