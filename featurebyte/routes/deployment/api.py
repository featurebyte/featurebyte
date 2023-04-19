"""
Deployment API routes
"""
from typing import Optional

from fastapi import APIRouter, Request

from featurebyte.routes.common.schema import (
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.deployment import DeploymentList, DeploymentSummary

router = APIRouter(prefix="/deployment")


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


@router.get("/summary", response_model=DeploymentSummary)
async def get_deployment_summary(
    request: Request,
) -> DeploymentSummary:
    """
    Get Deployment Summary
    """
    controller = request.state.app_container.deployment_controller
    deployment_summary: DeploymentSummary = await controller.get_deployment_summary()
    return deployment_summary
