"""
DeploymentSql API route controller
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.models.deployment_sql import DeploymentSqlModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.deployment_sql import (
    DeploymentSqlCreate,
    DeploymentSqlList,
    DeploymentSqlUpdate,
)
from featurebyte.schema.task import Task
from featurebyte.service.deployment_sql import DeploymentSqlService
from featurebyte.service.task_manager import TaskManager


class DeploymentSqlController(
    BaseDocumentController[DeploymentSqlModel, DeploymentSqlService, DeploymentSqlList]
):
    """
    DeploymentSql controller
    """

    paginated_document_class = DeploymentSqlList
    document_update_schema_class = DeploymentSqlUpdate

    def __init__(
        self,
        deployment_sql_service: DeploymentSqlService,
        task_controller: TaskController,
        task_manager: TaskManager,
    ):
        super().__init__(service=deployment_sql_service)
        self.task_controller = task_controller
        self.task_manager = task_manager

    async def generate_deployment_sql(self, deployment_id: str) -> Task:
        """
        Generate deployment SQL asynchronously

        Parameters
        ----------
        deployment_id: str
            ID of the deployment to generate SQL for

        Returns
        -------
        Task
            Task object that tracks the generation progress
        """
        # Create the deployment SQL creation data
        data = DeploymentSqlCreate(deployment_id=ObjectId(deployment_id))

        # Get the task payload
        payload = await self.service.get_deployment_sql_create_task_payload(data=data)

        # Submit the task
        task_id = await self.task_manager.submit(payload=payload)

        # Return the task object
        return await self.task_controller.get_task(task_id=str(task_id))
