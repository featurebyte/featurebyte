"""
Deployment API routes
"""
from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Query, Request
from fastapi.responses import ORJSONResponse

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.persistent import AuditDocumentList
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
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from featurebyte.schema.info import DeploymentInfo
from featurebyte.schema.task import Task

router = APIRouter(prefix="/deployment")


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
    request: Request, deployment_id: PydanticObjectId, data: DeploymentUpdate
) -> Optional[Task]:
    """
    Update Deployment
    """
    controller = request.state.app_container.deployment_controller
    task: Task = await controller.update_deployment(document_id=deployment_id, data=data)
    return task


@router.get("", response_model=DeploymentList)
async def list_deployments(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> DeploymentList:
    """
    List Deployments
    """
    controller = request.state.app_container.deployment_controller
    deployment_list: DeploymentList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return deployment_list


@router.get("/audit/{deployment_id}", response_model=AuditDocumentList)
async def list_deployment_audit_logs(
    request: Request,
    deployment_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
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
        sort_by=sort_by,
        sort_dir=sort_dir,
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
        get_credential=request.state.get_credential,
    )
    return cast(OnlineFeaturesResponseModel, result)


@router.get("/all/")
async def list_all_deployments(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    enabled: Optional[bool] = Query(default=None),
) -> AllDeploymentList:
    """
    List All Deployments (Regardless of Catalog)
    """
    controller = request.state.app_container.all_deployment_controller
    deployment_list: AllDeploymentList = await controller.list_all_deployments(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
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
