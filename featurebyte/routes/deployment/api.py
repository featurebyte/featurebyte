"""
Deployment API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Literal, Optional

from bson import ObjectId
from fastapi import Query, Request, Response
from fastapi.responses import ORJSONResponse

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseApiRouter
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.routes.deployment.controller import (
    AllDeploymentController,
    DeploymentController,
)
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.deployment import (
    AllDeploymentList,
    DeploymentCreate,
    DeploymentJobHistory,
    DeploymentList,
    DeploymentSummary,
    DeploymentUpdate,
    OnlineFeaturesResponseModel,
)
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload, SampleEntityServingNames
from featurebyte.schema.info import DeploymentInfo, DeploymentRequestCodeTemplate
from featurebyte.schema.task import Task


class DeploymentRouter(
    BaseApiRouter[DeploymentModel, DeploymentList, DeploymentCreate, DeploymentController]
):
    """
    Deployment router
    """

    object_model = DeploymentModel
    list_object_model = DeploymentList
    create_object_schema = DeploymentCreate
    controller = DeploymentController

    def __init__(self) -> None:
        super().__init__("/deployment")

        self.remove_routes({
            "/deployment": ["POST"],  # custom create returns Task instead of Deployment
            "/deployment/{deployment_id}": [
                "GET"
            ],  # avoid dynamic /{deployment_id} shadowing /deployment/all & /deployment/summary
        })

        self.router.add_api_route(
            "",
            self.create_deployment,
            methods=["POST"],
            response_model=Task,
            status_code=HTTPStatus.CREATED,
        )
        self.router.add_api_route(
            "/all",
            self.list_all_deployments,
            methods=["GET"],
            response_model=AllDeploymentList,
        )
        self.router.add_api_route(
            "/summary",
            self.get_deployment_summary,
            methods=["GET"],
            response_model=DeploymentSummary,
        )
        self.router.add_api_route(
            "/{deployment_id}",
            self.get_object,
            methods=["GET"],
            response_model=DeploymentModel,
        )
        self.router.add_api_route(
            "/{deployment_id}",
            self.update_deployment,
            methods=["PATCH"],
        )
        self.router.add_api_route(
            "/{deployment_id}/info",
            self.get_deployment_info,
            methods=["GET"],
            response_model=DeploymentInfo,
        )
        self.router.add_api_route(
            "/{deployment_id}/online_features",
            self.compute_online_features,
            methods=["POST"],
            response_model=OnlineFeaturesResponseModel,
            response_class=ORJSONResponse,
        )
        self.router.add_api_route(
            "/{deployment_id}/job_history",
            self.get_deployment_job_history,
            methods=["GET"],
            response_model=DeploymentJobHistory,
        )
        self.router.add_api_route(
            "/{deployment_id}/request_code_template",
            self.get_deployment_request_code_template,
            methods=["GET"],
            response_model=DeploymentRequestCodeTemplate,
        )
        self.router.add_api_route(
            "/{deployment_id}/sample_entity_serving_names",
            self.get_deployment_sample_entity_serving_names,
            methods=["GET"],
            response_model=SampleEntityServingNames,
        )

    async def create_deployment(self, request: Request, data: DeploymentCreate) -> Task:
        """
        Create Deployment
        """
        controller = self.get_controller_for_request(request)
        task = await controller.create_deployment(data=data)
        return task

    async def list_objects(
        self,
        request: Request,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = SortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
        name: Optional[str] = NameQuery,
        feature_list_id: Optional[PydanticObjectId] = None,
    ) -> DeploymentList:
        """
        List Deployments
        """
        controller = self.get_controller_for_request(request)
        query_filter = None
        if feature_list_id:
            query_filter = {"feature_list_id": feature_list_id}
        deployment_list = await controller.list(
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            search=search,
            name=name,
            query_filter=query_filter,
        )
        return deployment_list

    async def get_object(
        self, request: Request, deployment_id: PydanticObjectId
    ) -> DeploymentModel:
        return await super().get_object(request, deployment_id)

    async def delete_object(
        self, request: Request, deployment_id: PydanticObjectId
    ) -> DeleteResponse:
        return await super().delete_object(request, deployment_id)

    async def list_all_deployments(
        self,
        request: Request,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = SortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        enabled: Optional[bool] = Query(default=None),
    ) -> AllDeploymentList:
        """
        List All Deployments (Regardless of Catalog)
        """
        controller: AllDeploymentController = request.state.app_container.all_deployment_controller
        deployment_list = await controller.list_all_deployments(
            page=page,
            page_size=page_size,
            sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
            enabled=enabled,
        )
        return deployment_list

    async def get_deployment_summary(self, request: Request) -> DeploymentSummary:
        """
        Get Deployment Summary
        """
        controller: AllDeploymentController = request.state.app_container.all_deployment_controller
        deployment_summary = await controller.get_deployment_summary()
        return deployment_summary

    async def update_deployment(
        self,
        request: Request,
        deployment_id: PydanticObjectId,
        data: DeploymentUpdate,
        response: Response,
    ) -> Optional[Task]:
        """
        Update Deployment
        """
        controller = self.get_controller_for_request(request)
        task = await controller.update_deployment(document_id=ObjectId(deployment_id), data=data)
        if isinstance(task, Task):
            # if task is returned, it means the deployment is being updated asynchronously
            response.status_code = HTTPStatus.ACCEPTED
        return task

    async def list_audit_logs(
        self,
        request: Request,
        deployment_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request, deployment_id, page, page_size, sort_by, sort_dir, search
        )

    async def update_description(
        self, request: Request, deployment_id: PydanticObjectId, data: DescriptionUpdate
    ) -> DeploymentModel:
        return await super().update_description(request, deployment_id, data)

    async def get_deployment_info(
        self,
        request: Request,
        deployment_id: PydanticObjectId,
        verbose: bool = False,
    ) -> DeploymentInfo:
        """
        Get Deployment Info
        """
        controller = self.get_controller_for_request(request)
        deployment_info = await controller.get_info(
            document_id=ObjectId(deployment_id), verbose=verbose
        )
        return deployment_info

    async def compute_online_features(
        self,
        request: Request,
        deployment_id: PydanticObjectId,
        data: OnlineFeaturesRequestPayload,
    ) -> OnlineFeaturesResponseModel:
        """
        Compute online features
        """
        controller = self.get_controller_for_request(request)
        result = await controller.compute_online_features(
            deployment_id=ObjectId(deployment_id),
            data=data,
        )
        return result

    async def get_deployment_job_history(
        self,
        request: Request,
        deployment_id: PydanticObjectId,
        num_runs: int = Query(default=5),
    ) -> DeploymentJobHistory:
        """
        Get deployment job history
        """
        controller = self.get_controller_for_request(request)
        return await controller.get_deployment_job_history(ObjectId(deployment_id), num_runs)

    async def get_deployment_request_code_template(
        self,
        request: Request,
        deployment_id: PydanticObjectId,
        language: Literal["python", "sh"] = Query(default="python"),
    ) -> DeploymentRequestCodeTemplate:
        """
        Get Deployment Request Code Template
        """
        controller = self.get_controller_for_request(request)
        request_code_template = await controller.get_request_code_template(
            deployment_id=ObjectId(deployment_id),
            language=language,
        )
        return request_code_template

    async def get_deployment_sample_entity_serving_names(
        self,
        request: Request,
        deployment_id: PydanticObjectId,
        count: int = Query(default=1, gt=0, le=10),
    ) -> SampleEntityServingNames:
        """
        Get Deployment Sample Entity Serving Names
        """
        controller = self.get_controller_for_request(request)
        sample_entity_serving_names = await controller.get_sample_entity_serving_names(
            deployment_id=ObjectId(deployment_id), count=count
        )
        return sample_entity_serving_names
