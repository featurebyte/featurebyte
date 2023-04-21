"""
FeatureList Deploy Task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.schema.worker.task.feature_list_deploy import (
    CreateDeploymentPayload,
    DeploymentPayloadType,
    FeatureListDeployTaskPayload,
    UpdateDeploymentPayload,
)
from featurebyte.worker.task.base import BaseTask


class FeatureListDeployTask(BaseTask):
    """
    FeatureList Deploy Task
    """

    payload_class = FeatureListDeployTaskPayload

    async def execute(self) -> Any:
        """
        Execute FeatureList Deploy task
        """
        payload = cast(FeatureListDeployTaskPayload, self.payload)
        if payload.deployment_payload.type == DeploymentPayloadType.CREATE:
            create_deployment_payload = cast(CreateDeploymentPayload, payload.deployment_payload)
            await self.app_container.deploy_service.create_deployment(
                feature_list_id=create_deployment_payload.feature_list_id,
                deployment_id=payload.output_document_id,
                deployment_name=create_deployment_payload.name,
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
