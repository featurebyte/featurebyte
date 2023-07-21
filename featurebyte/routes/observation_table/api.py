"""
ObservationTable API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request
from starlette.responses import StreamingResponse

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.info import ObservationTableInfo
from featurebyte.schema.observation_table import ObservationTableCreate, ObservationTableList
from featurebyte.schema.task import Task

router = APIRouter(prefix="/observation_table")


@router.post("", response_model=Task, status_code=HTTPStatus.CREATED)
async def create_observation_table(
    request: Request,
    data: ObservationTableCreate,
) -> Task:
    """
    Create ObservationTable by submitting a materialization task
    """
    controller = request.state.app_container.observation_table_controller
    task_submit: Task = await controller.create_observation_table(
        data=data,
    )
    return task_submit


@router.get("/{observation_table_id}", response_model=ObservationTableModel)
async def get_observation_table(
    request: Request, observation_table_id: PydanticObjectId
) -> ObservationTableModel:
    """
    Get ObservationTable
    """
    controller = request.state.app_container.observation_table_controller
    observation_table: ObservationTableModel = await controller.get(
        document_id=observation_table_id
    )
    return observation_table


@router.delete("/{observation_table_id}", response_model=Task, status_code=HTTPStatus.ACCEPTED)
async def delete_observation_table(
    request: Request, observation_table_id: PydanticObjectId
) -> Task:
    """
    Delete ObservationTable by submitting a deletion task
    """
    controller = request.state.app_container.observation_table_controller
    task: Task = await controller.delete_materialized_table(document_id=observation_table_id)
    return task


@router.get("", response_model=ObservationTableList)
async def list_observation_tables(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> ObservationTableList:
    """
    List ObservationTables
    """
    controller = request.state.app_container.observation_table_controller
    observation_table_list: ObservationTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return observation_table_list


@router.get("/audit/{observation_table_id}", response_model=AuditDocumentList)
async def list_observation_table_audit_logs(
    request: Request,
    observation_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List ObservationTable audit logs
    """
    controller = request.state.app_container.observation_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=observation_table_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{observation_table_id}/info", response_model=ObservationTableInfo)
async def get_observation_table_info(
    request: Request, observation_table_id: PydanticObjectId, verbose: bool = VerboseQuery
) -> ObservationTableInfo:
    """
    Get ObservationTable info
    """
    controller = request.state.app_container.observation_table_controller
    info = await controller.get_info(document_id=observation_table_id, verbose=verbose)
    return cast(ObservationTableInfo, info)


@router.get("/pyarrow_table/{observation_table_id}")
async def download_table_as_pyarrow_table(
    request: Request, observation_table_id: PydanticObjectId
) -> StreamingResponse:
    """
    Download ObservationTable as pyarrow table
    """
    controller = request.state.app_container.observation_table_controller
    result: StreamingResponse = await controller.download_materialized_table(
        document_id=observation_table_id,
        get_credential=request.state.get_credential,
    )
    return result


@router.patch("/{observation_table_id}/description", response_model=ObservationTableModel)
async def update_observation_table_description(
    request: Request,
    observation_table_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> ObservationTableModel:
    """
    Update observation_table description
    """
    controller = request.state.app_container.observation_table_controller
    observation_table: ObservationTableModel = await controller.update_description(
        document_id=observation_table_id,
        description=data.description,
    )
    return observation_table
