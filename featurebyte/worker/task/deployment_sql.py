"""
DeploymentSql creation task
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.deployment_sql import DeploymentSqlCreateTaskPayload
from featurebyte.service.deployment_sql import DeploymentSqlService
from featurebyte.service.deployment_sql_generation import DeploymentSqlGenerationService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class DeploymentSqlCreateTask(DataWarehouseMixin, BaseTask[DeploymentSqlCreateTaskPayload]):
    """
    DeploymentSqlCreate Task
    """

    payload_class = DeploymentSqlCreateTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        deployment_sql_service: DeploymentSqlService,
        deployment_sql_generation_service: DeploymentSqlGenerationService,
    ):
        super().__init__(task_manager=task_manager)
        self.deployment_sql_service = deployment_sql_service
        self.deployment_sql_generation_service = deployment_sql_generation_service

    async def get_task_description(self, payload: DeploymentSqlCreateTaskPayload) -> str:
        return f'Generate deployment SQL for deployment ID "{payload.deployment_id}"'

    async def execute(self, payload: DeploymentSqlCreateTaskPayload) -> Any:
        # Generate and save the SQL using the generation service
        deployment_sql_model = await self.deployment_sql_generation_service.generate_deployment_sql(
            deployment_id=payload.deployment_id,
            output_document_id=payload.output_document_id,
        )
        await self.deployment_sql_service.create_document(deployment_sql_model)
