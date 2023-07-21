"""
Deployment API route controller
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from http import HTTPStatus

from bson import ObjectId
from fastapi import HTTPException

from featurebyte.exception import FeatureListNotOnlineEnabledError
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.deployment import (
    AllDeploymentList,
    AllDeploymentListRecord,
    DeploymentCreate,
    DeploymentList,
    DeploymentSummary,
    DeploymentUpdate,
    OnlineFeaturesResponseModel,
)
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from featurebyte.schema.info import DeploymentInfo
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.deployment_create_update import (
    CreateDeploymentPayload,
    DeploymentCreateUpdateTaskPayload,
    UpdateDeploymentPayload,
)
from featurebyte.service.catalog import AllCatalogService, CatalogService
from featurebyte.service.context import ContextService
from featurebyte.service.deployment import AllDeploymentService, DeploymentService
from featurebyte.service.feature_list import AllFeatureListService, FeatureListService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.online_serving import OnlineServingService


class DeploymentController(
    BaseDocumentController[DeploymentModel, DeploymentService, DeploymentList]
):
    """
    Deployment Controller
    """

    paginated_document_class = DeploymentList

    def __init__(
        self,
        deployment_service: DeploymentService,
        catalog_service: CatalogService,
        context_service: ContextService,
        feature_list_service: FeatureListService,
        online_serving_service: OnlineServingService,
        task_controller: TaskController,
    ):
        super().__init__(deployment_service)
        self.catalog_service = catalog_service
        self.context_service = context_service
        self.feature_list_service = feature_list_service
        self.online_serving_service = online_serving_service
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

        payload = DeploymentCreateUpdateTaskPayload(
            deployment_payload=CreateDeploymentPayload(
                name=data.name,
                feature_list_id=data.feature_list_id,
                enabled=False,
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
            payload = DeploymentCreateUpdateTaskPayload(
                deployment_payload=UpdateDeploymentPayload(enabled=data.enabled),
                user_id=self.service.user.id,
                catalog_id=self.service.catalog_id,
                output_document_id=document_id,
            )
            task_id = await self.task_controller.task_manager.submit(payload=payload)
            return await self.task_controller.get_task(task_id=str(task_id))
        return None

    async def get_info(self, document_id: ObjectId, verbose: bool) -> DeploymentInfo:
        """
        Get deployment info.

        Parameters
        ----------
        document_id: ObjectId
            Deployment ID to get info
        verbose: bool
            Whether to return verbose info

        Returns
        -------
        DeploymentInfo
        """
        _ = verbose
        deployment = await self.service.get_document(document_id=document_id)
        feature_list = await self.feature_list_service.get_document(
            document_id=deployment.feature_list_id
        )
        return DeploymentInfo(
            name=deployment.name,
            feature_list_name=feature_list.name,
            feature_list_version=feature_list.version.to_str(),
            num_feature=len(feature_list.feature_ids),
            enabled=deployment.enabled,
            serving_endpoint=(
                f"/deployment/{deployment.id}/online_features" if deployment.enabled else None
            ),
            created_at=deployment.created_at,
            updated_at=deployment.updated_at,
            description=deployment.description,
        )

    async def compute_online_features(
        self,
        deployment_id: ObjectId,
        data: OnlineFeaturesRequestPayload,
        get_credential: Any,
    ) -> OnlineFeaturesResponseModel:
        """
        Compute online features for a given deployment ID.

        Parameters
        ----------
        deployment_id: ObjectId
            ID of deployment to compute online features
        data: OnlineFeaturesRequestPayload
            Online features request payload
        get_credential: Any
            Get credential handler function

        Returns
        -------
        OnlineFeaturesResponseModel

        Raises
        ------
        HTTPException
            Invalid request payload
        """
        document = await self.service.get_document(deployment_id)
        feature_list = await self.feature_list_service.get_document(document.feature_list_id)
        try:
            result = await self.online_serving_service.get_online_features_from_feature_list(
                feature_list=feature_list,
                request_data=data.entity_serving_names,
                get_credential=get_credential,
            )
        except (FeatureListNotOnlineEnabledError, RuntimeError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc
        assert result is not None, result
        return result


class AllDeploymentController(
    BaseDocumentController[DeploymentModel, AllDeploymentService, DeploymentList]
):
    """
    All Deployment Controller
    """

    paginated_document_class = DeploymentList

    def __init__(
        self,
        all_deployment_service: AllDeploymentService,
        all_catalog_service: AllCatalogService,
        all_feature_list_service: AllFeatureListService,
        task_controller: TaskController,
    ):
        super().__init__(all_deployment_service)
        self.catalog_service = all_catalog_service
        self.feature_list_service = all_feature_list_service
        self.task_controller = task_controller

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
        with self.service.allow_use_raw_query_filter():
            deployment_data = await self.service.list_documents_as_dict(
                page=1,
                page_size=0,
                query_filter={"enabled": True},
                use_raw_query_filter=True,
            )

        for doc in deployment_data["data"]:
            deployment_model = DeploymentModel(**doc)
            feature_list_ids.add(deployment_model.feature_list_id)

        with self.feature_list_service.allow_use_raw_query_filter():
            async for feature_list in self.feature_list_service.list_documents_iterator(
                query_filter={"_id": {"$in": list(feature_list_ids)}},
                use_raw_query_filter=True,
            ):
                feature_ids.update(set(feature_list.feature_ids))

        return DeploymentSummary(
            num_feature_list=len(feature_list_ids),
            num_feature=len(feature_ids),
        )

    async def list_all_deployments(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        enabled: bool | None = None,
    ) -> AllDeploymentList:
        """
        List all deployments across all catalogs.

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: str | None
            Key used to sort the returning documents
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order
        enabled: bool | None
            Whether to return only enabled deployments

        Returns
        -------
        AllDeploymentList
        """
        with self.service.allow_use_raw_query_filter():
            deployment_data = await self.service.list_documents_as_dict(
                page=page,
                page_size=page_size,
                sort_by=sort_by,
                sort_dir=sort_dir,
                query_filter={"enabled": enabled} if enabled is not None else {},
                use_raw_query_filter=True,
            )

        feature_list_ids = {doc["feature_list_id"] for doc in deployment_data["data"]}
        with self.feature_list_service.allow_use_raw_query_filter():
            feature_list_documents = await self.feature_list_service.list_documents_as_dict(
                page_size=0,
                query_filter={"_id": {"$in": list(feature_list_ids)}},
                use_raw_query_filter=True,
            )
            deployment_id_to_feature_list = {
                doc["_id"]: FeatureListModel(**doc) for doc in feature_list_documents["data"]
            }

        catalog_ids = {doc["catalog_id"] for doc in deployment_data["data"]}
        catalog_documents = await self.catalog_service.list_documents_as_dict(
            page_size=0, query_filter={"_id": {"$in": list(catalog_ids)}}
        )
        deployment_id_to_catalog_name = {
            doc["_id"]: doc["name"] for doc in catalog_documents["data"]
        }

        output = []
        for doc in deployment_data["data"]:
            feature_list = deployment_id_to_feature_list[doc["feature_list_id"]]
            output.append(
                AllDeploymentListRecord(
                    **doc,
                    catalog_name=deployment_id_to_catalog_name[doc["catalog_id"]],
                    feature_list_name=feature_list.name,
                    feature_list_version=feature_list.version.to_str(),
                    num_feature=len(feature_list.feature_ids),
                )
            )

        deployment_data["data"] = output
        return AllDeploymentList(**deployment_data)
