"""
FeatureJobSettingAnalysis API routes response
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional, cast

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
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
    VerboseQuery,
)
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisCreate,
    FeatureJobSettingAnalysisList,
)
from featurebyte.schema.info import FeatureJobSettingAnalysisInfo
from featurebyte.schema.task import Task

router = APIRouter(prefix="/feature_job_setting_analysis")


class FeatureJobSettingAnalysisRouter(BaseRouter):
    """
    Feature job setting analysis router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.post("", response_model=Task, status_code=HTTPStatus.CREATED)
async def create_feature_job_setting_analysis(
    request: Request,
    data: FeatureJobSettingAnalysisCreate,
) -> Task:
    """
    Create Feature Job Setting Analysis
    """
    controller = request.state.app_container.feature_job_setting_analysis_controller
    task_submit: Task = await controller.create_feature_job_setting_analysis(
        data=data,
    )
    return task_submit


@router.get("", response_model=FeatureJobSettingAnalysisList)
async def list_feature_job_setting_analysis(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
    event_table_id: Optional[PydanticObjectId] = None,
) -> FeatureJobSettingAnalysisList:
    """
    List Feature Job Setting Analysis
    """
    params = {}
    if event_table_id:
        params["query_filter"] = {"event_table_id": event_table_id}

    controller = request.state.app_container.feature_job_setting_analysis_controller
    analysis_list: FeatureJobSettingAnalysisList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
        name=name,
        **params,
    )
    return analysis_list


@router.get("/{feature_job_setting_analysis_id}", response_model=FeatureJobSettingAnalysisModel)
async def get_feature_job_setting_analysis(
    request: Request,
    feature_job_setting_analysis_id: PydanticObjectId,
) -> FeatureJobSettingAnalysisModel:
    """
    Retrieve Feature Job Setting Analysis
    """
    controller = request.state.app_container.feature_job_setting_analysis_controller
    analysis: FeatureJobSettingAnalysisModel = await controller.get(
        document_id=feature_job_setting_analysis_id,
    )
    return analysis


@router.get("/{feature_job_setting_analysis_id}/info", response_model=FeatureJobSettingAnalysisInfo)
async def get_feature_job_setting_analysis_info(
    request: Request,
    feature_job_setting_analysis_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> FeatureJobSettingAnalysisInfo:
    """
    Retrieve FeatureJobSettingAnalysis info
    """
    controller = request.state.app_container.feature_job_setting_analysis_controller
    info = await controller.get_info(
        document_id=feature_job_setting_analysis_id,
        verbose=verbose,
    )
    return cast(FeatureJobSettingAnalysisInfo, info)


@router.get("/{feature_job_setting_analysis_id}/report")
async def get_feature_job_setting_analysis_report(
    request: Request, feature_job_setting_analysis_id: PydanticObjectId
) -> StreamingResponse:
    """
    Retrieve FeatureJobSettingAnalysis pdf report
    """
    controller = request.state.app_container.feature_job_setting_analysis_controller
    return cast(
        StreamingResponse,
        await controller.get_feature_job_setting_analysis_report(
            feature_job_setting_analysis_id=feature_job_setting_analysis_id,
        ),
    )


@router.get("/audit/{feature_job_setting_analysis_id}", response_model=AuditDocumentList)
async def list_feature_job_setting_analysis_audit_logs(
    request: Request,
    feature_job_setting_analysis_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Feature Job Setting Analysis audit logs
    """
    controller = request.state.app_container.feature_job_setting_analysis_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=feature_job_setting_analysis_id,
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
    )
    return audit_doc_list


@router.post(
    "/backtest",
    response_model=Task,
    status_code=HTTPStatus.CREATED,
)
async def run_backtest(
    request: Request,
    data: FeatureJobSettingAnalysisBacktest,
) -> Task:
    """
    Run Backtest on Feature Job Setting Analysis
    """
    controller = request.state.app_container.feature_job_setting_analysis_controller
    task_submit: Task = await controller.backtest(
        data=data,
    )
    return task_submit


@router.patch(
    "/{feature_job_setting_analysis_id}/description",
    response_model=FeatureJobSettingAnalysisModel,
)
async def update_feature_job_setting_analysis_description(
    request: Request,
    feature_job_setting_analysis_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> FeatureJobSettingAnalysisModel:
    """
    Update feature_job_setting_analysis description
    """
    controller = request.state.app_container.feature_job_setting_analysis_controller
    feature_job_setting_analysis: FeatureJobSettingAnalysisModel = (
        await controller.update_description(
            document_id=feature_job_setting_analysis_id,
            description=data.description,
        )
    )
    return feature_job_setting_analysis


@router.delete("/{feature_job_setting_analysis_id}")
async def delete_feature_job_setting_analysis(
    request: Request, feature_job_setting_analysis_id: PydanticObjectId
) -> None:
    """
    Delete Feature Job Setting Analysis
    """
    controller = request.state.app_container.feature_job_setting_analysis_controller
    await controller.delete(
        document_id=feature_job_setting_analysis_id,
    )
