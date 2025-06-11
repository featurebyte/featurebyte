"""
ManagedViewService class
"""

from __future__ import annotations

from typing import Any, Optional, Type

from bson import ObjectId
from redis import Redis

from featurebyte.exception import (
    DocumentConflictError,
    FeatureStoreNotInCatalogError,
    InvalidViewSQL,
)
from featurebyte.models.managed_view import ManagedViewModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.persistent import Persistent
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.feature_store import validate_select_sql
from featurebyte.schema.managed_view import ManagedViewServiceCreate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import INTERACTIVE_SESSION_TIMEOUT_SECONDS
from featurebyte.storage import Storage


class ManagedViewService(
    BaseDocumentService[
        ManagedViewModel, ManagedViewServiceCreate, BaseDocumentServiceUpdateSchema
    ],
):
    """
    ManagedViewService class
    """

    document_class: Type[ManagedViewModel] = ManagedViewModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        catalog_service: CatalogService,
        feature_store_service: FeatureStoreService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        session_manager_service: SessionManagerService,
        block_modification_handler: BlockModificationHandler,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.catalog_service = catalog_service
        self.feature_store_service = feature_store_service
        self.feature_store_warehouse_service = feature_store_warehouse_service
        self.session_manager_service = session_manager_service

    async def construct_get_query_filter(
        self, document_id: ObjectId, use_raw_query_filter: bool = False, **kwargs: Any
    ) -> QueryFilter:
        output = await super().construct_get_query_filter(
            document_id=document_id, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        # managed view without catalog_id is a global function (used by all catalogs)
        output["catalog_id"] = {"$in": [None, self.catalog_id]}
        return output

    async def construct_list_query_filter(
        self,
        query_filter: Optional[QueryFilter] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> QueryFilter:
        output = await super().construct_list_query_filter(
            query_filter=query_filter, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        # managed view without catalog_id is a global function (used by all catalogs)
        output["catalog_id"] = {"$in": [None, self.catalog_id]}
        return output

    async def create_document(self, data: ManagedViewServiceCreate) -> ManagedViewModel:
        if self.catalog_id:
            # check that feature store matches the catalog
            catalog = await self.catalog_service.get_document(document_id=self.catalog_id)
            if data.tabular_source.feature_store_id not in catalog.default_feature_store_ids:
                raise FeatureStoreNotInCatalogError(
                    f'Feature store "{data.tabular_source.feature_store_id}" does not belong to catalog '
                    f'"{self.catalog_id}".'
                )

        # check if managed view with same name already exists
        document_dict = await self.persistent.find_one(
            collection_name=self.collection_name,
            query_filter={"name": data.name, "catalog_id": data.catalog_id},
        )
        if document_dict:
            if data.catalog_id:
                raise DocumentConflictError(
                    f'Managed view with name "{data.name}" already exists in '
                    f"catalog (catalog_id: {data.catalog_id})."
                )

            raise DocumentConflictError(
                f'Global managed view with name "{data.name}" already exists.'
            )

        # create the managed view
        feature_store = await self.feature_store_service.get_document(
            document_id=data.tabular_source.feature_store_id,
        )
        session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
        )

        # validate that the SQL is a single select statement
        select_expr = validate_select_sql(data.sql, session.source_type)
        data.sql = sql_to_string(select_expr, source_type=session.source_type)

        await session.create_table_as(
            table_details=data.tabular_source.table_details,
            select_expr=data.sql,
            kind="VIEW",
        )

        # populate columns info of the created view
        source_info = feature_store.get_source_info()
        table_details = data.tabular_source.table_details
        column_specs = await self.feature_store_warehouse_service.list_columns(
            feature_store=feature_store,
            database_name=table_details.database_name or source_info.database_name,
            schema_name=table_details.schema_name or source_info.schema_name,
            table_name=table_details.table_name,
        )
        columns_info = [ColumnInfo(**dict(col)) for col in column_specs]
        data.columns_info = columns_info

        return await super().create_document(data=data)

    async def delete_document(
        self,
        document_id: ObjectId,
        exception_detail: Optional[str] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> int:
        # drop the managed view from the feature store
        managed_view = await self.get_document(document_id=document_id)
        table_details = managed_view.tabular_source.table_details
        feature_store = await self.feature_store_service.get_document(
            document_id=managed_view.tabular_source.feature_store_id,
        )
        source_info = feature_store.get_source_info()
        session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
        )
        try:
            await session.drop_table(
                database_name=table_details.database_name or source_info.database_name,
                schema_name=table_details.schema_name or source_info.schema_name,
                table_name=table_details.table_name,
                timeout=INTERACTIVE_SESSION_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            raise InvalidViewSQL(f"Failed to drop managed view: {exc}") from exc

        return await super().delete_document(
            document_id=document_id,
            exception_detail=exception_detail,
            use_raw_query_filter=use_raw_query_filter,
            **kwargs,
        )
