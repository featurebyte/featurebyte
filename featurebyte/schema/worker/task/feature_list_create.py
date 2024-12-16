"""
Feature list creation creation schema
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Literal, Union

from pydantic import Discriminator, Field, Tag
from typing_extensions import Annotated

from featurebyte.enum import ConflictResolution, WorkerCommand
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class FeatureParameters(FeatureByteBaseModel):
    """Feature parameters"""

    id: PydanticObjectId
    name: NameStr


def feature_params_discriminator(value: Any) -> Literal["feature_ids", "feature_params"]:
    """
    Discriminator for feature parameters

    Parameters
    ----------
    value: Any
        Input value

    Returns
    -------
    Literal["feature_ids", "feature_params"]
    """
    if isinstance(value, list):
        if value:
            return feature_params_discriminator(value[0])
        return "feature_ids"
    if isinstance(value, (dict, FeatureParameters)):
        return "feature_params"
    return "feature_ids"


class FeaturesParameters(FeatureByteBaseModel):
    """Feature list feature parameters"""

    features: Annotated[
        Union[
            Annotated[List[FeatureParameters], Tag("feature_params"), Field(min_length=1)],
            Annotated[List[PydanticObjectId], Tag("feature_ids"), Field(min_length=1)],
        ],
        Discriminator(feature_params_discriminator),
    ]


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
