"""
Deployment Create & Update Task
"""
from __future__ import annotations

from typing import Any, cast

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

    @property
    def lock_key(self) -> str:
        # each deployment can only be created or updated once at a time
        payload = cast(DeploymentCreateUpdateTaskPayload, self.payload)
        return f"deployment:{payload.output_document_id}:create_update"

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
            )

        if payload.deployment_payload.type == DeploymentPayloadType.UPDATE:
            update_deployment_payload = cast(UpdateDeploymentPayload, payload.deployment_payload)
            await self.app_container.deploy_service.update_deployment(
                deployment_id=payload.output_document_id,
                enabled=update_deployment_payload.enabled,
                get_credential=self.get_credential,
                update_progress=self.update_progress,
            )
