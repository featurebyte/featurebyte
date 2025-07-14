"""
ManagedView API route controller
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple, cast

from bson import ObjectId

from featurebyte.enum import ViewNamePrefix
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.managed_view import ManagedViewModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.persistent.base import SortDir
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.managed_view import (
    ManagedViewCreate,
    ManagedViewInfo,
    ManagedViewList,
    ManagedViewServiceCreate,
    ManagedViewTableInfo,
    ManagedViewUpdate,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.managed_view import ManagedViewService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.table import TableService

logger = get_logger(__name__)


class ManagedViewController(
    BaseDocumentController[ManagedViewModel, ManagedViewService, ManagedViewList]
):
    """
    ManagedView controller
    """

    paginated_document_class = ManagedViewList

    def __init__(
        self,
        managed_view_service: ManagedViewService,
        table_service: TableService,
        feature_store_service: FeatureStoreService,
        catalog_service: CatalogService,
    ):
        super().__init__(managed_view_service)
        self.table_service = table_service
        self.feature_store_service = feature_store_service
        self.catalog_service = catalog_service

        # retrieve active catalog id and feature store id
        assert managed_view_service.catalog_id is not None
        self.active_catalog_id: ObjectId = managed_view_service.catalog_id

    async def list_managed_views(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: list[tuple[str, SortDir]] | None = None,
        search: str | None = None,
        name: str | None = None,
        feature_store_id: PydanticObjectId | None = None,
    ) -> ManagedViewList:
        """
        List ManagedView stored in persistent

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: list[tuple[str, SortDir]] | None
            Keys and directions used to sort the returning documents
        search: str | None
            Search token to be used in filtering
        name: str | None
            Feature name to be used in filtering
        feature_store_id: PydanticObjectId | None
            FeatureStore id to be used in filtering

        Returns
        -------
        ManagedViewList
            Paginated list of ManagedView
        """
        sort_by = sort_by or [("created_at", "desc")]
        params: Dict[str, Any] = {"search": search, "name": name}
        if feature_store_id:
            params["query_filter"] = {"tabular_source.feature_store_id": feature_store_id}
        return await self.list(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            **params,
        )

    async def _get_feature_store_id(self) -> ObjectId:
        """
        Get feature store id from active catalog

        Returns
        -------
        ObjectId
        """
        active_catalog = await self.catalog_service.get_document(document_id=self.active_catalog_id)
        return ObjectId(active_catalog.default_feature_store_ids[0])

    async def create_managed_view(
        self,
        data: ManagedViewCreate,
    ) -> ManagedViewModel:
        """
        Create Online Store at persistent

        Parameters
        ----------
        data: ManagedViewCreate
            ManagedView creation payload

        Returns
        -------
        ManagedViewModel
            Newly created feature store document
        """
        # validate feature store id exists
        feature_store = await self.feature_store_service.get_document(
            document_id=(await self._get_feature_store_id())
        )
        source_info = feature_store.get_source_info()

        # create managed view
        catalog_id = None if data.is_global else self.active_catalog_id
        service_data = ManagedViewServiceCreate(
            **data.model_dump(by_alias=True),
            catalog_id=catalog_id,
            tabular_source=TabularSource(
                feature_store_id=feature_store.id,
                table_details=TableDetails(
                    database_name=source_info.database_name,
                    schema_name=source_info.schema_name,
                    table_name=f"{ViewNamePrefix.MANAGED_VIEW}_{data.id}",
                ),
            ),
        )
        document = await self.service.create_document(service_data)
        return document

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        document = await self.service.get_document(document_id=document_id)
        return [
            (
                self.table_service,
                {"managed_view_id": document.id},
            )
        ]

    async def update_managed_view(
        self, managed_view_id: ObjectId, data: ManagedViewUpdate
    ) -> ManagedViewModel:
        """
        Update online store

        Parameters
        ----------
        managed_view_id: ObjectId
            ManagedView ID
        data: ManagedViewUpdate
            ManagedView update payload

        Returns
        -------
        ManagedViewModel
            Updated online store document
        """
        document = cast(ManagedViewModel, await self.service.update_document(managed_view_id, data))
        return document

    async def get_info(self, document_id: ObjectId, verbose: bool) -> ManagedViewInfo:
        """
        Get ManagedView info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose flag

        Returns
        -------
        ManagedViewInfo
        """
        _ = verbose

        document = await self.service.get_document(document_id=document_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=(await self._get_feature_store_id())
        )
        tables_info: List[ManagedViewTableInfo] = []
        tables = await self.table_service.list_documents_as_dict(
            query_filter={"tabular_source": document.tabular_source.model_dump(by_alias=True)},
            projection={"_id": 1, "name": 1},
        )
        if tables["total"]:
            for doc in tables["data"]:
                tables_info.append(ManagedViewTableInfo(id=doc["_id"], name=doc["name"]))

        return ManagedViewInfo(
            name=document.name,
            feature_store_name=feature_store.name,
            table_details=document.tabular_source.table_details,
            sql=document.sql,
            used_by_tables=tables_info,
            created_at=document.created_at,
            updated_at=document.updated_at,
            description=document.description,
        )
