"""
Deployment API routes
"""

from typing import Literal, Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import ORJSONResponse

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseRouter
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.deployment import (
    AllDeploymentList,
    DeploymentCreate,
    DeploymentList,
    DeploymentSummary,
    DeploymentUpdate,
    OnlineFeaturesResponseModel,
)
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload, SampleEntityServingNames
from featurebyte.schema.info import DeploymentInfo, DeploymentRequestCodeTemplate
from featurebyte.schema.task import Task

router = APIRouter(prefix="/deployment")


class DeploymentRouter(BaseRouter):
    """
    Deployment router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.post("", response_model=Task, status_code=HTTPStatus.CREATED)
async def create_deployment(request: Request, data: DeploymentCreate) -> Task:
    """
    Create Deployment
    """
    controller = request.state.app_container.deployment_controller
    task: Task = await controller.create_deployment(data=data)
    return task


@router.get("/{deployment_id}", response_model=DeploymentModel)
async def get_deployment(request: Request, deployment_id: PydanticObjectId) -> DeploymentModel:
    """
    Get Deployment
    """
    controller = request.state.app_container.deployment_controller
    deployment: DeploymentModel = await controller.get(document_id=deployment_id)
    return deployment


@router.patch("/{deployment_id}")
async def update_deployment(
    request: Request, deployment_id: PydanticObjectId, data: DeploymentUpdate, response: Response
) -> Optional[Task]:
    """
    Update Deployment
    """
    controller = request.state.app_container.deployment_controller
    task: Optional[Task] = await controller.update_deployment(document_id=deployment_id, data=data)
    if isinstance(task, Task):
        # if task is returned, it means the deployment is being updated asynchronously
        response.status_code = HTTPStatus.ACCEPTED
    return task


@router.get("", response_model=DeploymentList)
async def list_deployments(
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
    controller = request.state.app_container.deployment_controller
    query_filter = None
    if feature_list_id:
        query_filter = {"feature_list_id": feature_list_id}
    deployment_list: DeploymentList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
        name=name,
        query_filter=query_filter,
    )
    return deployment_list


@router.delete("/{deployment_id}")
async def delete_deployment(request: Request, deployment_id: PydanticObjectId) -> None:
    """
    Delete Deployment
    """
    controller = request.state.app_container.deployment_controller
    await controller.delete(document_id=deployment_id)


@router.get("/audit/{deployment_id}", response_model=AuditDocumentList)
async def list_deployment_audit_logs(
    request: Request,
    deployment_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Deployment audit logs
    """
    controller = request.state.app_container.deployment_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=deployment_id,
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
    )
    return audit_doc_list


@router.get("/{deployment_id}/info", response_model=DeploymentInfo)
async def get_deployment_info(
    request: Request,
    deployment_id: PydanticObjectId,
    verbose: bool = False,
) -> DeploymentInfo:
    """
    Get Deployment Info
    """
    controller = request.state.app_container.deployment_controller
    deployment_info: DeploymentInfo = await controller.get_info(
        document_id=deployment_id, verbose=verbose
    )
    return deployment_info


@router.post(
    "/{deployment_id}/online_features",
    response_model=OnlineFeaturesResponseModel,
    response_class=ORJSONResponse,
)
async def compute_online_features(
    request: Request,
    deployment_id: PydanticObjectId,
    data: OnlineFeaturesRequestPayload,
) -> OnlineFeaturesResponseModel:
    """
    Compute online features
    """
    controller = request.state.app_container.deployment_controller
    result = await controller.compute_online_features(
        deployment_id=deployment_id,
        data=data,
    )
    return cast(OnlineFeaturesResponseModel, result)


@router.get("/all/")
async def list_all_deployments(
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
    controller = request.state.app_container.all_deployment_controller
    deployment_list: AllDeploymentList = await controller.list_all_deployments(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        enabled=enabled,
    )
    return deployment_list


@router.get("/summary/", response_model=DeploymentSummary)
async def get_deployment_summary(
    request: Request,
) -> DeploymentSummary:
    """
    Get Deployment Summary
    """
    controller = request.state.app_container.all_deployment_controller
    deployment_summary: DeploymentSummary = await controller.get_deployment_summary()
    return deployment_summary


@router.patch("/{deployment_id}/description", response_model=DeploymentModel)
async def update_deployment_description(
    request: Request,
    deployment_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> DeploymentModel:
    """
    Update deployment description
    """
    controller = request.state.app_container.deployment_controller
    deployment: DeploymentModel = await controller.update_description(
        document_id=deployment_id,
        description=data.description,
    )
    return deployment


@router.get("/{deployment_id}/request_code_template", response_model=DeploymentRequestCodeTemplate)
async def get_deployment_request_code_template(
    request: Request,
    deployment_id: PydanticObjectId,
    language: Literal["python", "sh"] = Query(default="python"),
) -> DeploymentRequestCodeTemplate:
    """
    Get Deployment Request Code Template
    """
    controller = request.state.app_container.deployment_controller
    request_code_template: DeploymentRequestCodeTemplate = (
        await controller.get_request_code_template(
            deployment_id=deployment_id,
            language=language,
        )
    )
    return request_code_template


@router.get(
    "/{deployment_id}/sample_entity_serving_names",
    response_model=SampleEntityServingNames,
)
async def get_deployment_sample_entity_serving_names(
    request: Request,
    deployment_id: PydanticObjectId,
    count: int = Query(default=1, gt=0, le=10),
) -> SampleEntityServingNames:
    """
    Get Deployment Sample Entity Serving Names
    """
    controller = request.state.app_container.deployment_controller
    sample_entity_serving_names: SampleEntityServingNames = (
        await controller.get_sample_entity_serving_names(deployment_id=deployment_id, count=count)
    )
    return sample_entity_serving_names
