"""
Feature create task payload
"""
from __future__ import annotations

from featurebyte.enum import WorkerCommand
from featurebyte.models.feature import FeatureModel
from featurebyte.schema.feature import FeatureCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload


class FeatureCreateTaskPayload(BaseTaskPayload, FeatureCreate):
    """
    Feature create task payload
    """

    output_collection_name = FeatureModel.collection_name()
    command = WorkerCommand.FEATURE_CREATE
