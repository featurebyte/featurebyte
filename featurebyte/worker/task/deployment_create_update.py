"""
Deployment Create & Update Task
"""

from __future__ import annotations

from typing import Any, Optional, cast

from redis import Redis

from featurebyte.exception import DocumentCreationError, DocumentUpdateError
from featurebyte.schema.worker.task.deployment_create_update import (
    CreateDeploymentPayload,
    DeploymentCreateUpdateTaskPayload,
    DeploymentPayloadType,
    UpdateDeploymentPayload,
)
from featurebyte.service.deploy import DeployService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseLockTask
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater


class DeploymentCreateUpdateTask(BaseLockTask[DeploymentCreateUpdateTaskPayload]):
    """
    FeatureList Deploy Task
    """

    payload_class = DeploymentCreateUpdateTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        redis: Redis[Any],
        deployment_service: DeploymentService,
        deploy_service: DeployService,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(task_manager=task_manager, redis=redis)
        self.deployment_service = deployment_service
        self.deploy_service = deploy_service
        self.task_progress_updater = task_progress_updater

    async def get_task_description(self, payload: DeploymentCreateUpdateTaskPayload) -> str:
        if payload.deployment_payload.type == DeploymentPayloadType.CREATE:
            action = "Create"
            creation_payload = cast(CreateDeploymentPayload, payload.deployment_payload)
            deployment_name = creation_payload.name
        else:
            deployment = await self.deployment_service.get_document(
                document_id=payload.output_document_id
            )
            deployment_name = deployment.name
            if payload.deployment_payload.enabled:
                action = "Enable"
            else:
                action = "Disable"
        return f'{action} deployment "{deployment_name}"'

    def lock_key(self, payload: DeploymentCreateUpdateTaskPayload) -> str:
        # each deployment can only be created or updated once at a time
        return f"deployment:{payload.output_document_id}:create_update"

    @property
    def lock_timeout(self) -> Optional[int]:
        # set lock timeout to 24 hours
        return 24 * 60 * 60

    @property
    def lock_blocking(self) -> bool:
        # if the deployment is being created or updated by another task,
        # fail the current task immediately without waiting
        return False

    def handle_lock_not_acquired(self, payload: DeploymentCreateUpdateTaskPayload) -> None:
        error_msg = f"Deployment {payload.output_document_id} is currently being created or updated"
        if payload.deployment_payload.type == DeploymentPayloadType.CREATE:
            raise DocumentCreationError(error_msg)
        raise DocumentUpdateError(error_msg)

    async def _execute(self, payload: DeploymentCreateUpdateTaskPayload) -> Any:
        if payload.deployment_payload.type == DeploymentPayloadType.CREATE:
            create_deployment_payload = cast(CreateDeploymentPayload, payload.deployment_payload)
            await self.deploy_service.create_deployment(
                feature_list_id=create_deployment_payload.feature_list_id,
                deployment_id=payload.output_document_id,
                deployment_name=create_deployment_payload.name,
                to_enable_deployment=create_deployment_payload.enabled,
                use_case_id=create_deployment_payload.use_case_id,
                context_id=create_deployment_payload.context_id,
            )

        if payload.deployment_payload.type == DeploymentPayloadType.UPDATE:
            update_deployment_payload = cast(UpdateDeploymentPayload, payload.deployment_payload)
            await self.deploy_service.update_deployment(
                deployment_id=payload.output_document_id,
                to_enable_deployment=update_deployment_payload.enabled,
            )
