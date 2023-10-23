"""
FeatureStore API routes
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, cast

from fastapi import Query, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.routes.base_router import BaseApiRouter
from featurebyte.routes.common.schema import (
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
    FeatureStoreCreate,
    FeatureStoreList,
    FeatureStorePreview,
    FeatureStoreSample,
    FeatureStoreShape,
)
from featurebyte.schema.info import FeatureStoreInfo


class FeatureStoreRouter(
    BaseApiRouter[FeatureStoreModel, FeatureStoreList, FeatureStoreCreate, FeatureStoreController]
):
    """
    Feature Store API router
    """

    # pylint: disable=arguments-renamed

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
            response_model=List[ColumnSpec],
        )
        self.router.add_api_route(
            "/shape",
            self.get_data_shape,
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

    async def create_object(
        self,
        request: Request,
        data: FeatureStoreCreate,
    ) -> FeatureStoreModel:
        """
        Create Feature Store
        """
        controller = request.state.app_container.feature_store_controller
        feature_store: FeatureStoreModel = await controller.create_feature_store(data=data)
        return feature_store

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
        sort_dir: Optional[str] = SortDirQuery,
        search: Optional[str] = SearchQuery,
    ) -> AuditDocumentList:
        return await super().list_audit_logs(
            request, feature_store_id, page, page_size, sort_by, sort_dir, search
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
        controller = request.state.app_container.feature_store_controller
        info = await controller.get_info(
            document_id=feature_store_id,
            verbose=verbose,
        )
        return cast(FeatureStoreInfo, info)

    @staticmethod
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
        controller = request.state.app_container.feature_store_controller
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
        controller = request.state.app_container.feature_store_controller
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
        controller = request.state.app_container.feature_store_controller
        return cast(
            FeatureStoreShape,
            await controller.shape(preview=preview),
        )

    @staticmethod
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
            await controller.preview(preview=preview, limit=limit),
        )

    @staticmethod
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
            await controller.sample(sample=sample, size=size, seed=seed),
        )

    @staticmethod
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
            await controller.describe(sample=sample, size=size, seed=seed),
        )

    async def delete_object(
        self, request: Request, feature_store_id: PydanticObjectId
    ) -> DeleteResponse:
        controller = self.get_controller_for_request(request)
        await controller.delete(document_id=feature_store_id)
        return DeleteResponse()
