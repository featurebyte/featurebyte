"""
FeatureStore API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, Dict, List, Optional

from bson import ObjectId
from fastapi import Query, Request

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.query_graph.model.column_info import ColumnInfo, ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.routes.base_router import BaseApiRouter
from featurebyte.routes.common.schema import (
    DESCRIPTION_SIZE_LIMIT,
    PREVIEW_DEFAULT,
    PREVIEW_LIMIT,
    PREVIEW_SEED,
    AuditLogSortByQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.routes.feature_store.controller import FeatureStoreController
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.feature_store import (
    DatabaseDetailsUpdate,
    FeatureStoreCreate,
    FeatureStoreList,
    FeatureStorePreview,
    FeatureStoreQueryPreview,
    FeatureStoreSample,
    FeatureStoreShape,
    FeatureStoreUpdate,
)
from featurebyte.schema.info import FeatureStoreInfo
from featurebyte.schema.task import Task


class FeatureStoreRouter(
    BaseApiRouter[FeatureStoreModel, FeatureStoreList, FeatureStoreCreate, FeatureStoreController]
):
    """
    Feature Store API router
    """

    object_model = FeatureStoreModel
    list_object_model = FeatureStoreList
    create_object_schema = FeatureStoreCreate
    controller = FeatureStoreController

    def __init__(self) -> None:
        super().__init__("/feature_store")
        self.router.add_api_route(
            "/{feature_store_id}/info",
            self.get_feature_store_info,
            methods=["GET"],
            response_model=FeatureStoreInfo,
        )
        self.router.add_api_route(
            "/database",
            self.list_databases_in_feature_store,
            methods=["POST"],
            response_model=List[str],
        )
        self.router.add_api_route(
            "/schema",
            self.list_schemas_in_database,
            methods=["POST"],
            response_model=List[str],
        )
        self.router.add_api_route(
            "/table",
            self.list_tables_in_database_schema,
            methods=["POST"],
            response_model=List[str],
        )
        self.router.add_api_route(
            "/column",
            self.list_columns_in_database_table,
            methods=["POST"],
            response_model=List[ColumnInfo],
        )
        self.router.add_api_route(
            "/shape",
            self.get_data_shape,
            methods=["POST"],
            response_model=FeatureStoreShape,
        )
        self.router.add_api_route(
            "/table_shape",
            self.get_table_shape,
            methods=["POST"],
            response_model=FeatureStoreShape,
        )
        self.router.add_api_route(
            "/preview",
            self.get_data_preview,
            methods=["POST"],
            response_model=Dict[str, Any],
        )
        self.router.add_api_route(
            "/table_preview",
            self.get_table_preview,
            methods=["POST"],
            response_model=Dict[str, Any],
        )
        self.router.add_api_route(
            "/sql_preview",
            self.get_sql_preview,
            methods=["POST"],
            response_model=Dict[str, Any],
        )
        self.router.add_api_route(
            "/sample",
            self.get_data_sample,
            methods=["POST"],
            response_model=Dict[str, Any],
        )
        self.router.add_api_route(
            "/description",
            self.get_data_description,
            methods=["POST"],
            response_model=Dict[str, Any],
        )
        self.router.add_api_route(
            "/data_description",
            self.submit_data_description_task,
            methods=["POST"],
            response_model=Task,
            status_code=HTTPStatus.CREATED,
        )
        self.router.add_api_route(
            "/{feature_store_id}/details",
            self.update_details,
            methods=["PATCH"],
            response_model=FeatureStoreModel,
        )
        self.router.add_api_route(
            "/{feature_store_id}",
            self.update,
            methods=["PATCH"],
            response_model=FeatureStoreModel,
        )

    async def create_object(
        self,
        request: Request,
        data: FeatureStoreCreate,
    ) -> FeatureStoreModel:
        """
        Create Feature Store
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        return await controller.create_feature_store(data=data)

    async def get_object(
        self, request: Request, feature_store_id: PydanticObjectId
    ) -> FeatureStoreModel:
        return await super().get_object(request, feature_store_id)

    async def list_audit_logs(
        self,
        request: Request,
        feature_store_id: PydanticObjectId,
        page: int = PageQuery,
        page_size: int = PageSizeQuery,
        sort_by: Optional[str] = AuditLogSortByQuery,
        sort_dir: Optional[SortDir] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request,
            feature_store_id,
            page,
            page_size,
            sort_by,
            sort_dir,
            search,
        )

    async def update_description(
        self, request: Request, feature_store_id: PydanticObjectId, data: DescriptionUpdate
    ) -> FeatureStoreModel:
        return await super().update_description(request, feature_store_id, data)

    @staticmethod
    async def get_feature_store_info(
        request: Request,
        feature_store_id: PydanticObjectId,
        verbose: bool = VerboseQuery,
    ) -> FeatureStoreInfo:
        """
        Retrieve FeatureStore info
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        return await controller.get_info(
            document_id=ObjectId(feature_store_id),
            verbose=verbose,
        )

    @staticmethod
    async def try_retrieve_feature_store(
        controller: FeatureStoreController, feature_store: FeatureStoreModel
    ) -> FeatureStoreModel:
        """
        Try to retrieve FeatureStore from database
        """
        try:
            return await controller.get(document_id=feature_store.id)
        except DocumentNotFoundError:
            return feature_store

    @staticmethod
    async def list_databases_in_feature_store(
        request: Request,
        feature_store: FeatureStoreModel,
    ) -> List[str]:
        """
        List databases
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        feature_store = await FeatureStoreRouter.try_retrieve_feature_store(
            controller, feature_store
        )
        result: List[str] = await controller.list_databases(
            feature_store=feature_store,
        )
        return result

    @staticmethod
    async def list_schemas_in_database(
        request: Request,
        database_name: str,
        feature_store: FeatureStoreModel,
    ) -> List[str]:
        """
        List schemas
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        feature_store = await FeatureStoreRouter.try_retrieve_feature_store(
            controller, feature_store
        )
        result: List[str] = await controller.list_schemas(
            feature_store=feature_store,
            database_name=database_name,
        )
        return result

    @staticmethod
    async def list_tables_in_database_schema(
        request: Request,
        database_name: str,
        schema_name: str,
        feature_store: FeatureStoreModel,
    ) -> List[str]:
        """
        List schemas
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        feature_store = await FeatureStoreRouter.try_retrieve_feature_store(
            controller, feature_store
        )
        result: List[str] = await controller.list_tables(
            feature_store=feature_store,
            database_name=database_name,
            schema_name=schema_name,
        )
        return result

    @staticmethod
    async def list_columns_in_database_table(
        request: Request,
        database_name: str,
        schema_name: str,
        table_name: str,
        feature_store: FeatureStoreModel,
    ) -> List[ColumnSpecWithDescription]:
        """
        List columns
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        feature_store = await FeatureStoreRouter.try_retrieve_feature_store(
            controller, feature_store
        )
        result: List[ColumnSpecWithDescription] = await controller.list_columns(
            feature_store=feature_store,
            database_name=database_name,
            schema_name=schema_name,
            table_name=table_name,
        )
        return result

    @staticmethod
    async def get_data_shape(
        request: Request,
        preview: FeatureStorePreview,
    ) -> FeatureStoreShape:
        """
        Retrieve shape for query graph node
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        return await controller.shape(preview=preview)

    @staticmethod
    async def get_table_shape(
        request: Request,
        location: TabularSource,
    ) -> FeatureStoreShape:
        """
        Retrieve shape for a tabular source
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        return await controller.table_shape(location=location)

    @staticmethod
    async def get_data_preview(
        request: Request,
        preview: FeatureStorePreview,
        limit: int = Query(default=PREVIEW_DEFAULT, gt=0, le=PREVIEW_LIMIT),
    ) -> Dict[str, Any]:
        """
        Retrieve data preview for query graph node
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        return await controller.preview(preview=preview, limit=limit)

    @staticmethod
    async def get_sql_preview(
        request: Request,
        preview: FeatureStoreQueryPreview,
        limit: int = Query(default=PREVIEW_DEFAULT, gt=0, le=PREVIEW_LIMIT),
    ) -> Dict[str, Any]:
        """
        Retrieve data preview for query graph node
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        return await controller.sql_preview(preview=preview, limit=limit)

    @staticmethod
    async def get_table_preview(
        request: Request,
        location: TabularSource,
        limit: int = Query(default=PREVIEW_DEFAULT, gt=0, le=PREVIEW_LIMIT),
    ) -> Dict[str, Any]:
        """
        Retrieve data preview for tabular source
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        return await controller.table_preview(location=location, limit=limit)

    @staticmethod
    async def get_data_sample(
        request: Request,
        sample: FeatureStoreSample,
        size: int = Query(default=PREVIEW_DEFAULT, gt=0, le=PREVIEW_LIMIT),
        seed: int = Query(default=PREVIEW_SEED),
    ) -> Dict[str, Any]:
        """
        Retrieve data sample for query graph node
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        return await controller.sample(sample=sample, size=size, seed=seed)

    @staticmethod
    async def get_data_description(
        request: Request,
        sample: FeatureStoreSample,
        size: int = Query(default=0, gte=0, le=DESCRIPTION_SIZE_LIMIT),
        seed: int = Query(default=PREVIEW_SEED),
    ) -> Dict[str, Any]:
        """
        Retrieve data description for query graph node
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        return await controller.describe(sample=sample, size=size, seed=seed)

    @staticmethod
    async def submit_data_description_task(
        request: Request,
        sample: FeatureStoreSample,
        size: int = Query(default=0, gte=0, le=DESCRIPTION_SIZE_LIMIT),
        seed: int = Query(default=PREVIEW_SEED),
        catalog_id: Optional[PydanticObjectId] = Query(default=None),
    ) -> Task:
        """
        Submit data description task for query graph node
        """
        controller: FeatureStoreController = request.state.app_container.feature_store_controller
        catalog_id_value = catalog_id or DEFAULT_CATALOG_ID
        task_submit: Task = await controller.create_data_description(
            sample=sample, size=size, seed=seed, catalog_id=ObjectId(catalog_id_value)
        )
        return task_submit

    async def delete_object(
        self, request: Request, feature_store_id: PydanticObjectId
    ) -> DeleteResponse:
        controller: FeatureStoreController = self.get_controller_for_request(request)
        await controller.delete(document_id=ObjectId(feature_store_id))
        return DeleteResponse()

    async def update_details(
        self, request: Request, feature_store_id: PydanticObjectId, data: DatabaseDetailsUpdate
    ) -> FeatureStoreModel:
        """Update details"""
        controller: FeatureStoreController = self.get_controller_for_request(request)
        return await controller.update_details(
            feature_store_id=ObjectId(feature_store_id), data=data
        )

    async def update(
        self,
        request: Request,
        feature_store_id: PydanticObjectId,
        data: FeatureStoreUpdate,
    ) -> FeatureStoreModel:
        """Update max_query_concurrency"""
        controller: FeatureStoreController = self.get_controller_for_request(request)
        return await controller.update(feature_store_id=ObjectId(feature_store_id), data=data)
