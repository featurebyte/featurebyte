"""
Deployment API route controller
"""
from __future__ import annotations

from typing import Optional

from bson import ObjectId

from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.deployment import (
    DeploymentCreate,
    DeploymentList,
    DeploymentSummary,
    DeploymentUpdate,
)
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.feature_list_deploy import (
    CreateDeploymentPayload,
    FeatureListDeployTaskPayload,
    UpdateDeploymentPayload,
)
from featurebyte.service.context import ContextService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.feature_list import FeatureListService


class DeploymentController(
    BaseDocumentController[DeploymentModel, DeploymentService, DeploymentList]
):
    """
    Deployment Controller
    """

    paginated_document_class = DeploymentList

    def __init__(
        self,
        service: DeploymentService,
        context_service: ContextService,
        feature_list_service: FeatureListService,
        task_controller: TaskController,
    ):
        super().__init__(service)
        self.context_service = context_service
        self.feature_list_service = feature_list_service
        self.task_controller = task_controller

    async def create_deployment(self, data: DeploymentCreate) -> Task:
        """
        Create deployment.

        Parameters
        ----------
        data : DeploymentCreate
            Deployment data to create.

        Returns
        -------
        Task
            Task to create deployment.
        """
        # check if feature list exists
        _ = await self.feature_list_service.get_document(document_id=data.feature_list_id)

        payload = FeatureListDeployTaskPayload(
            deployment_payload=CreateDeploymentPayload(
                name=data.name,
                feature_list_id=data.feature_list_id,
                enabled=True,
            ),
            user_id=self.service.user.id,
            catalog_id=self.service.catalog_id,
            output_document_id=data.id or ObjectId(),
        )
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def update_deployment(
        self, document_id: ObjectId, data: DeploymentUpdate
    ) -> Optional[Task]:
        """
        Update deployment.

        Parameters
        ----------
        document_id: ObjectId
            Deployment ID to update.
        data: DeploymentUpdate
            Deployment data to update.

        Returns
        -------
        Optional[Task]
            Task to create deployment.
        """
        # check if deployment exists
        deployment = await self.service.get_document(document_id=document_id)
        if data.enabled is not None and data.enabled != deployment.enabled:
            payload = FeatureListDeployTaskPayload(
                deployment_payload=UpdateDeploymentPayload(enabled=data.enabled),
                user_id=self.service.user.id,
                catalog_id=self.service.catalog_id,
                output_document_id=document_id,
            )
            task_id = await self.task_controller.task_manager.submit(payload=payload)
            return await self.task_controller.get_task(task_id=str(task_id))
        return None

    async def get_deployment_summary(self) -> DeploymentSummary:
        """
        Get summary of all deployments.

        Returns
        -------
        DeploymentSummary
            Summary of all deployments.
        """
        feature_list_ids = set()
        feature_ids = set()
        deployment_data = await self.service.list_documents(
            page=1,
            page_size=0,
            query_filter={"enabled": True},
        )

        for doc in deployment_data["data"]:
            deployment_model = DeploymentModel(**doc)
            feature_list_ids.add(deployment_model.feature_list_id)

        async for doc in self.feature_list_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(feature_list_ids)}},
        ):
            feature_list_model = FeatureListModel(**doc)
            feature_ids.update(set(feature_list_model.feature_ids))
        return DeploymentSummary(
            num_feature_list=len(feature_list_ids),
            num_feature=len(feature_ids),
        )
