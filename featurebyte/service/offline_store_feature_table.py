"""
OfflineStoreFeatureTableService class
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from bson import ObjectId

from featurebyte.models.offline_store_feature_table import (
    OfflineStoreFeatureTableModel,
    OfflineStoreFeatureTableUpdate,
)
from featurebyte.models.offline_store_ingest_query import OfflineFeatureTableSignature
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.service.base_document import BaseDocumentService


class OfflineStoreFeatureTableService(
    BaseDocumentService[
        OfflineStoreFeatureTableModel, OfflineStoreFeatureTableModel, OfflineStoreFeatureTableUpdate
    ]
):
    """
    OfflineStoreFeatureTableService class
    """

    document_class = OfflineStoreFeatureTableModel
    document_update_class = OfflineStoreFeatureTableUpdate

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
        )

    async def get_catalog_name_part(
        self, catalog_id: ObjectId, feature_store_id: Optional[ObjectId]
    ) -> str:
        """
        Get catalog name part

        Parameters
        ----------
        catalog_id: ObjectId
            Catalog id
        feature_store_id: Optional[ObjectId]
            Feature store id

        Returns
        -------
        str
        """
        with self.allow_use_raw_query_filter():
            query_filter = {"catalog_id": catalog_id, "name_prefix": {"$exists": True, "$ne": None}}
            if feature_store_id:
                query_filter["feature_store_id"] = feature_store_id
            query_result = await self.list_documents_as_dict(
                query_filter=query_filter, use_raw_query_filter=True, page_size=1
            )
            if query_result["total"]:
                return query_result["data"][0]["name_prefix"]

            results, total = await self.persistent.aggregate_find(
                collection_name=self.collection_name,
                pipeline=[
                    {"$match": {"feature_store_id": feature_store_id}},
                    {"$group": {"_id": "$catalog_id"}},
                    {"$count": "uniqueCount"},
                ],
            )
            assert len(results) == 1
            catalog_count = results[0]["uniqueCount"] + 1
            return f"cat{catalog_count}"

    async def create_document(
        self, data: OfflineStoreFeatureTableModel
    ) -> OfflineStoreFeatureTableModel:
        """
        Create a new document

        Parameters
        ----------
        data : OfflineStoreFeatureTableModel
            Document data

        Returns
        -------
        OfflineStoreFeatureTableModel
        """
        with self.allow_use_raw_query_filter():
            catalog_part = await self.get_catalog_name_part(
                catalog_id=data.catalog_id, feature_store_id=data.feature_store_id
            )
            data.name_prefix = catalog_part

        data.name = data.get_basename()
        with self.allow_use_raw_query_filter():
            query_filter = {"name_prefix": catalog_part, "name": data.name}
            if data.feature_store_id:
                query_filter["feature_store_id"] = data.feature_store_id
            query_result = await self.list_documents_as_dict(
                query_filter=query_filter, use_raw_query_filter=True, page_size=1
            )

        count = query_result["total"]
        data.name_suffix = None
        if count:
            data.name_suffix = str(count)
        return await super().create_document(data)

    async def get_existing_document(
        self, table_signature: OfflineFeatureTableSignature
    ) -> Optional[OfflineStoreFeatureTableModel]:
        """
        Get existing document from the persistent

        Parameters
        ----------
        table_signature: OfflineFeatureTableSignature
            Table signature

        Returns
        -------
        Optional[OfflineStoreFeatureTableModel]
        """
        query_result = await self.list_documents_as_dict(query=table_signature.dict())
        if query_result["total"]:
            return OfflineStoreFeatureTableModel(**query_result["data"][0])
        return None
