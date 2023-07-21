"""
FeatureStore API routes
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Query, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.query_graph.node.schema import ColumnSpec
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
from featurebyte.schema.feature_store import (
    FeatureStoreCreate,
    FeatureStoreList,
    FeatureStorePreview,
    FeatureStoreSample,
    FeatureStoreShape,
)
from featurebyte.schema.info import FeatureStoreInfo

router = APIRouter(prefix="/feature_store")


@router.post("", response_model=FeatureStoreModel, status_code=HTTPStatus.CREATED)
async def create_feature_store(request: Request, data: FeatureStoreCreate) -> FeatureStoreModel:
    """
    Create Feature Store
    """
    controller = request.state.app_container.feature_store_controller
    feature_store: FeatureStoreModel = await controller.create_feature_store(data=data)
    return feature_store


@router.get("/{feature_store_id}", response_model=FeatureStoreModel)
async def get_feature_store(
    request: Request, feature_store_id: PydanticObjectId
) -> FeatureStoreModel:
    """
    Retrieve Feature Store
    """
    controller = request.state.app_container.feature_store_controller
    feature_store: FeatureStoreModel = await controller.get(
        document_id=feature_store_id,
    )
    return feature_store


@router.get("", response_model=FeatureStoreList)
async def list_feature_stores(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> FeatureStoreList:
    """
    List FeatureStore
    """
    controller = request.state.app_container.feature_store_controller
    feature_store_list: FeatureStoreList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return feature_store_list


@router.get("/audit/{feature_store_id}", response_model=AuditDocumentList)
async def list_feature_store_audit_logs(
    request: Request,
    feature_store_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Feature Store audit logs
    """
    controller = request.state.app_container.feature_store_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=feature_store_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{feature_store_id}/info", response_model=FeatureStoreInfo)
async def get_feature_store_info(
    request: Request,
    feature_store_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> FeatureStoreInfo:
    """
    Retrieve FeatureStore info
    """
    controller = request.state.app_container.feature_store_controller
    info = await controller.get_info(
        document_id=feature_store_id,
        verbose=verbose,
    )
    return cast(FeatureStoreInfo, info)


@router.post("/database", response_model=List[str])
async def list_databases_in_feature_store(
    request: Request,
    feature_store: FeatureStoreModel,
) -> List[str]:
    """
    List databases
    """
    controller = request.state.app_container.feature_store_controller
    result: List[str] = await controller.list_databases(
        feature_store=feature_store,
        get_credential=request.state.get_credential,
    )
    return result


@router.post("/schema", response_model=List[str])
async def list_schemas_in_database(
    request: Request,
    database_name: str,
    feature_store: FeatureStoreModel,
) -> List[str]:
    """
    List schemas
    """
    controller = request.state.app_container.feature_store_controller
    result: List[str] = await controller.list_schemas(
        feature_store=feature_store,
        database_name=database_name,
        get_credential=request.state.get_credential,
    )
    return result


@router.post("/table", response_model=List[str])
async def list_tables_in_database_schema(
    request: Request,
    database_name: str,
    schema_name: str,
    feature_store: FeatureStoreModel,
) -> List[str]:
    """
    List schemas
    """
    controller = request.state.app_container.feature_store_controller
    result: List[str] = await controller.list_tables(
        feature_store=feature_store,
        database_name=database_name,
        schema_name=schema_name,
        get_credential=request.state.get_credential,
    )
    return result


@router.post("/column", response_model=List[ColumnSpec])
async def list_columns_in_database_table(
    request: Request,
    database_name: str,
    schema_name: str,
    table_name: str,
    feature_store: FeatureStoreModel,
) -> List[ColumnSpec]:
    """
    List columns
    """
    controller = request.state.app_container.feature_store_controller
    result: List[ColumnSpec] = await controller.list_columns(
        feature_store=feature_store,
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        get_credential=request.state.get_credential,
    )
    return result


@router.post("/shape", response_model=FeatureStoreShape)
async def get_data_shape(
    request: Request,
    preview: FeatureStorePreview,
) -> FeatureStoreShape:
    """
    Retrieve shape for query graph node
    """
    controller = request.state.app_container.feature_store_controller
    return cast(
        FeatureStoreShape,
        await controller.shape(preview=preview, get_credential=request.state.get_credential),
    )


@router.post("/preview", response_model=Dict[str, Any])
async def get_data_preview(
    request: Request,
    preview: FeatureStorePreview,
    limit: int = Query(default=10, gt=0, le=10000),
) -> Dict[str, Any]:
    """
    Retrieve data preview for query graph node
    """
    controller = request.state.app_container.feature_store_controller
    return cast(
        Dict[str, Any],
        await controller.preview(
            preview=preview, limit=limit, get_credential=request.state.get_credential
        ),
    )


@router.post("/sample", response_model=Dict[str, Any])
async def get_data_sample(
    request: Request,
    sample: FeatureStoreSample,
    size: int = Query(default=10, gt=0, le=10000),
    seed: int = Query(default=1234),
) -> Dict[str, Any]:
    """
    Retrieve data sample for query graph node
    """
    controller = request.state.app_container.feature_store_controller
    return cast(
        Dict[str, Any],
        await controller.sample(
            sample=sample, size=size, seed=seed, get_credential=request.state.get_credential
        ),
    )


@router.post("/description", response_model=Dict[str, Any])
async def get_data_description(
    request: Request,
    sample: FeatureStoreSample,
    size: int = Query(default=0, gte=0, le=1000000),
    seed: int = Query(default=1234),
) -> Dict[str, Any]:
    """
    Retrieve data description for query graph node
    """
    controller = request.state.app_container.feature_store_controller
    return cast(
        Dict[str, Any],
        await controller.describe(
            sample=sample, size=size, seed=seed, get_credential=request.state.get_credential
        ),
    )


@router.patch("/{feature_store_id}/description", response_model=FeatureStoreModel)
async def update_feature_store_description(
    request: Request,
    feature_store_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> FeatureStoreModel:
    """
    Update feature_store description
    """
    controller = request.state.app_container.feature_store_controller
    feature_store: FeatureStoreModel = await controller.update_description(
        document_id=feature_store_id,
        description=data.description,
    )
    return feature_store
