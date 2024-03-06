"""
Feature list creation creation schema
"""
from __future__ import annotations

from typing import List, Union

from pydantic import BaseModel, Field, StrictStr

from featurebyte.enum import ConflictResolution, WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class FeatureParameters(BaseModel):
    """Feature parameters"""

    id: PydanticObjectId
    name: StrictStr


class FeaturesParameters(BaseModel):
    """Feature list feature parameters"""

    features: Union[List[FeatureParameters], List[PydanticObjectId]]


class FeatureListCreateTaskPayload(BaseTaskPayload):
    """
    Feature list creation task payload
    """

    command = WorkerCommand.FEATURE_LIST_CREATE
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    output_collection_name = FeatureListModel.collection_name()
    feature_list_id: PydanticObjectId
    feature_list_name: str
    features_parameters_path: str
    features_conflict_resolution: ConflictResolution
