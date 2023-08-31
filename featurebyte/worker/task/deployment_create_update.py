"""
Deployment Create & Update Task
"""
from __future__ import annotations

from typing import Any, Optional, cast

from featurebyte.exception import DocumentCreationError, DocumentUpdateError
from featurebyte.schema.worker.task.deployment_create_update import (
    CreateDeploymentPayload,
    DeploymentCreateUpdateTaskPayload,
    DeploymentPayloadType,
    UpdateDeploymentPayload,
)
from featurebyte.worker.task.base import BaseLockTask


class DeploymentCreateUpdateTask(BaseLockTask):
    """
    FeatureList Deploy Task
    """

    payload_class = DeploymentCreateUpdateTaskPayload

    async def get_task_description(self) -> str:
        payload = cast(DeploymentCreateUpdateTaskPayload, self.payload)
        if payload.deployment_payload.type == DeploymentPayloadType.CREATE:
            action = "Create"
            creation_payload = cast(CreateDeploymentPayload, payload.deployment_payload)
            deployment_name = creation_payload.name
        else:
            deployment = await self.app_container.deployment_service.get_document(
                document_id=payload.output_document_id
            )
            deployment_name = deployment.name
            if payload.deployment_payload.enabled:
                action = "Enable"
            else:
                action = "Disable"
        return f'{action} deployment "{deployment_name}"'

    @property
    def lock_key(self) -> str:
        # each deployment can only be created or updated once at a time
        payload = cast(DeploymentCreateUpdateTaskPayload, self.payload)
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

    def handle_lock_not_acquired(self) -> None:
        payload = cast(DeploymentCreateUpdateTaskPayload, self.payload)
        error_msg = f"Deployment {payload.output_document_id} is currently being created or updated"
        if payload.deployment_payload.type == DeploymentPayloadType.CREATE:
            raise DocumentCreationError(error_msg)
        raise DocumentUpdateError(error_msg)

    async def _execute(self) -> Any:
        """
        Execute Deployment Create & Update Task
        """
        payload = cast(DeploymentCreateUpdateTaskPayload, self.payload)
        if payload.deployment_payload.type == DeploymentPayloadType.CREATE:
            create_deployment_payload = cast(CreateDeploymentPayload, payload.deployment_payload)
            await self.app_container.deploy_service.create_deployment(
                feature_list_id=create_deployment_payload.feature_list_id,
                deployment_id=payload.output_document_id,
                deployment_name=create_deployment_payload.name,
                to_enable_deployment=create_deployment_payload.enabled,
                get_credential=self.get_credential,
                update_progress=self.update_progress,
                use_case_id=create_deployment_payload.use_case_id,
                context_id=create_deployment_payload.context_id,
            )

        if payload.deployment_payload.type == DeploymentPayloadType.UPDATE:
            update_deployment_payload = cast(UpdateDeploymentPayload, payload.deployment_payload)
            await self.app_container.deploy_service.update_deployment(
                deployment_id=payload.output_document_id,
                enabled=update_deployment_payload.enabled,
                get_credential=self.get_credential,
                update_progress=self.update_progress,
            )
