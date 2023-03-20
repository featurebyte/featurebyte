"""
FeatureList Deploy Task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.routes.app_container import AppContainer
from featurebyte.schema.worker.task.feature_list_deploy import FeatureListDeployTaskPayload
from featurebyte.service.deploy import DeployService
from featurebyte.service.task_manager import TaskManager
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

        app_container = AppContainer.get_instance(
            user=self.user,
            persistent=self.get_persistent(),
            temp_storage=self.get_temp_storage(),
            task_manager=TaskManager(
                user=self.user,
                persistent=self.get_persistent(),
                catalog_id=payload.catalog_id,
            ),
            storage=self.get_storage(),
            container_id=payload.catalog_id,
        )

        deploy_service: DeployService = app_container.deploy_service

        await deploy_service.update_feature_list(
            feature_list_id=payload.feature_list_id,
            deployed=payload.deployed,
            get_credential=self.get_credential,
            return_document=False,
        )
