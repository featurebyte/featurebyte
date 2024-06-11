"""
FeatureList Deploy Task Payload schema
"""

from typing import ClassVar, Optional, Union
from typing_extensions import Annotated, Literal

from pydantic import Field

from featurebyte.enum import StrEnum, WorkerCommand
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.deployment import DeploymentModel
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class DeploymentPayloadType(StrEnum):
    """Deployment payload type"""

    CREATE = "create"
    UPDATE = "update"


class CreateDeploymentPayload(FeatureByteBaseModel):
    """Create deployment"""

    type: Literal[DeploymentPayloadType.CREATE] = Field(DeploymentPayloadType.CREATE, const=True)
    name: Optional[NameStr] = Field(default=None)
    feature_list_id: PydanticObjectId
    enabled: bool
    use_case_id: Optional[PydanticObjectId] = Field(default=None)
    context_id: Optional[PydanticObjectId] = Field(default=None)


class UpdateDeploymentPayload(FeatureByteBaseModel):
    """Update deployment"""

    type: Literal[DeploymentPayloadType.UPDATE] = Field(DeploymentPayloadType.UPDATE, const=True)
    enabled: bool


DeploymentPayload = Annotated[
    Union[CreateDeploymentPayload, UpdateDeploymentPayload], Field(discriminator="type")
]


class DeploymentCreateUpdateTaskPayload(BaseTaskPayload):
    """
    Deployment Create & Update Task Payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.DEPLOYMENT_CREATE_UPDATE
    output_collection_name: ClassVar[str] = DeploymentModel.collection_name()

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    deployment_payload: DeploymentPayload
