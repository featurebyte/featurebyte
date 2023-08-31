"""
UseCase API routes
"""
from typing import Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.use_case import UseCaseModel
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
from featurebyte.schema.deployment import DeploymentList
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableList
from featurebyte.schema.observation_table import ObservationTableList
from featurebyte.schema.use_case import UseCaseCreate, UseCaseList, UseCaseUpdate

router = APIRouter(prefix="/use_case")


@router.post("", response_model=UseCaseModel, status_code=HTTPStatus.CREATED)
async def create_use_case(
    request: Request,
    data: UseCaseCreate,
) -> UseCaseModel:
    """
    Create a UseCase
    """
    controller = request.state.app_container.use_case_controller
    use_case: UseCaseModel = await controller.create_use_case(
        data=data,
    )
    return use_case


@router.get("/{use_case_id}", response_model=UseCaseModel)
async def get_use_case(request: Request, use_case_id: PydanticObjectId) -> UseCaseModel:
    """
    Get Use Case
    """
    controller = request.state.app_container.use_case_controller
    use_case: UseCaseModel = await controller.get(document_id=use_case_id)
    return use_case


@router.get("/{use_case_id}/observation_tables", response_model=ObservationTableList)
async def list_use_case_observation_tables(
    request: Request,
    use_case_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
) -> ObservationTableList:
    """
    List Observation Tables associated with the Use Case
    """
    use_case_controller = request.state.app_container.use_case_controller
    use_case: UseCaseModel = await use_case_controller.get(document_id=use_case_id)

    observation_table_controller = request.state.app_container.observation_table_controller
    observation_table_list: ObservationTableList = await observation_table_controller.list(
        query_filter={"_id": {"$in": use_case.observation_table_ids}},
        page=page,
        page_size=page_size,
    )
    return observation_table_list


@router.get("/{use_case_id}/feature_tables", response_model=HistoricalFeatureTableList)
async def list_use_case_feature_tables(
    request: Request,
    use_case_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
) -> HistoricalFeatureTableList:
    """
    List Feature Tables associated with the Use Case
    """
    use_case_controller = request.state.app_container.use_case_controller
    use_case: UseCaseModel = await use_case_controller.get(document_id=use_case_id)

    historical_feature_table_controller = (
        request.state.app_container.historical_feature_table_controller
    )
    historical_feature_table_list: HistoricalFeatureTableList = (
        await historical_feature_table_controller.list(
            query_filter={"observation_table_id": {"$in": use_case.observation_table_ids}},
            page=page,
            page_size=page_size,
        )
    )
    return historical_feature_table_list


@router.get("/{use_case_id}/deployments", response_model=DeploymentList)
async def list_use_case_deployments(
    request: Request,
    use_case_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
) -> DeploymentList:
    """
    List Feature Tables associated with the Use Case
    """
    use_case_controller = request.state.app_container.use_case_controller
    use_case: UseCaseModel = await use_case_controller.get(document_id=use_case_id)

    deployment_controller = request.state.app_container.deployment_controller
    deployment_list: DeploymentList = await deployment_controller.list(
        query_filter={"use_case_id": use_case.id},
        page=page,
        page_size=page_size,
    )
    return deployment_list


@router.patch("/{use_case_id}", response_model=UseCaseModel)
async def update_use_case(
    request: Request, use_case_id: PydanticObjectId, data: UseCaseUpdate
) -> UseCaseModel:
    """
    Update Use Case
    """
    controller = request.state.app_container.use_case_controller
    use_case: UseCaseModel = await controller.update_use_case(use_case_id=use_case_id, data=data)
    return use_case


@router.delete("/{use_case_id}", status_code=HTTPStatus.OK)
async def delete_use_case(request: Request, use_case_id: PydanticObjectId) -> None:
    """
    Update Use Case
    """
    controller = request.state.app_container.use_case_controller
    await controller.delete_use_case(document_id=use_case_id)


@router.get("", response_model=UseCaseList)
async def list_use_cases(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> UseCaseList:
    """
    List Use Case
    """
    controller = request.state.app_container.use_case_controller
    doc_list: UseCaseList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return doc_list


@router.get("/audit/{use_case_id}", response_model=AuditDocumentList)
async def list_use_case_audit_logs(
    request: Request,
    use_case_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Use Case audit logs
    """
    controller = request.state.app_container.use_case_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=use_case_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.patch("/{use_case_id}/description", response_model=UseCaseModel)
async def update_use_case_description(
    request: Request,
    use_case_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> UseCaseModel:
    """
    Update Use Case description
    """
    controller = request.state.app_container.use_case_controller
    doc: UseCaseModel = await controller.update_description(
        document_id=use_case_id,
        description=data.description,
    )
    return doc
