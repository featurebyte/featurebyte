"""
FeatureJobSettingAnalysis API routes
"""
from __future__ import annotations

from typing import Any, Dict, Optional, cast

from http import HTTPStatus

from beanie import PydanticObjectId
from fastapi import APIRouter, Request

from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
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
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisCreate,
    FeatureJobSettingAnalysisList,
)
from featurebyte.schema.task import Task

router = APIRouter(prefix="/feature_job_setting_analysis")


@router.post("", response_model=Task, status_code=HTTPStatus.CREATED)
async def create_feature_job_setting_analysis(
    request: Request,
    data: FeatureJobSettingAnalysisCreate,
) -> Task:
    """
    Create Feature Job Setting Analysis
    """
    task_submit: Task = await request.state.controller.create_feature_job_setting_analysis(
        user=request.state.user,
        persistent=request.state.persistent,
        task_manager=request.state.task_manager,
        data=data,
    )
    return task_submit


@router.get("", response_model=FeatureJobSettingAnalysisList)
async def list_feature_job_setting_analysis(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> FeatureJobSettingAnalysisList:
    """
    List Feature Job Setting Analysis
    """
    analysis_list: FeatureJobSettingAnalysisList = await request.state.controller.list(
        user=request.state.user,
        persistent=request.state.persistent,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
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
    analysis: FeatureJobSettingAnalysisModel = await request.state.controller.get(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_job_setting_analysis_id,
    )
    return analysis


@router.get("/audit/{feature_job_setting_analysis_id}", response_model=AuditDocumentList)
async def list_feature_job_setting_analysis_audit_logs(
    request: Request,
    feature_job_setting_analysis_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Feature Job Setting Analysis audit logs
    """
    audit_doc_list: AuditDocumentList = await request.state.controller.list_audit(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_job_setting_analysis_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.post(
    "/{feature_job_setting_analysis_id}/backtest",
    response_model=Task,
    status_code=HTTPStatus.ACCEPTED,
)
async def run_backtest(
    request: Request,
    data: FeatureJobSettingAnalysisBacktest,
) -> Task:
    """
    Run Backtest on Feature Job Setting Analysis
    """
    task_submit: Task = await request.state.controller.backtest(
        user=request.state.user,
        persistent=request.state.persistent,
        task_manager=request.state.task_manager,
        data=data,
    )
    return task_submit
