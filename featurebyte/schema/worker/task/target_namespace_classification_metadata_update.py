"""
TargetNamespaceClassificationMetadataUpdateTask schema
"""

from typing import ClassVar

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload


class TargetNamespaceClassificationMetadataUpdateTaskPayload(BaseTaskPayload):
    """
    TargetNamespaceClassificationMetadataUpdateTask class
    """

    command: ClassVar[WorkerCommand] = WorkerCommand.TARGET_NAMESPACE_CLASSIFICATION_METADATA_UPDATE
    target_namespace_id: PydanticObjectId
    observation_table_id: PydanticObjectId
