"""
FeatureList Deploy Task Payload schema
"""
from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload


class FeatureListDeployTaskPayload(BaseTaskPayload):
    """
    FeatureList Deploy Task Payload
    """

    command = WorkerCommand.FEATURE_LIST_DEPLOY

    feature_list_id: PydanticObjectId
    deployed: bool
