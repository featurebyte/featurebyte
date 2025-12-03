"""
DeploymentSqlTaskPayload schema
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.deployment_sql import DeploymentSqlModel
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class DeploymentSqlCreateTaskPayload(BaseTaskPayload):
    """
    DeploymentSql creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.DEPLOYMENT_SQL_CREATE
    output_collection_name: ClassVar[str] = DeploymentSqlModel.collection_name()
    is_revocable: ClassVar[bool] = True

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    deployment_id: PydanticObjectId
