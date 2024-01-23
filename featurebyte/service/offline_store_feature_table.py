"""
OfflineStoreFeatureTableService class
"""
from __future__ import annotations

from typing import Optional

from bson import ObjectId

from featurebyte.models.offline_store_feature_table import (
    OfflineStoreFeatureTableModel,
    OfflineStoreFeatureTableUpdate,
)
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

    async def get_or_create_name_prefix(
        self, catalog_id: ObjectId, feature_store_id: Optional[ObjectId]
    ) -> str:
        """
        Get name prefix based on catalog id and feature store id

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
                return str(query_result["data"][0]["name_prefix"])

            res, _ = await self.persistent.aggregate_find(
                collection_name=self.collection_name,
                pipeline=[
                    {"$match": {"feature_store_id": feature_store_id}},
                    {"$group": {"_id": "$catalog_id"}},
                    {"$count": "uniqueCount"},
                ],
            )
            results = list(res)
            assert len(results) == 1
            catalog_count = results[0]["uniqueCount"] + 1
            return f"cat{catalog_count}"

    async def get_or_create_document(
        self, data: OfflineStoreFeatureTableModel
    ) -> OfflineStoreFeatureTableModel:
        """
        Get or create an offline store feature table

        Parameters
        ----------
        data: OfflineStoreFeatureTableModel
            Offline store feature table model

        Returns
        -------
        OfflineStoreFeatureTableModel
        """
        query_result = await self.list_documents_as_dict(
            query_filter=data.table_signature, page_size=1
        )
        if query_result["total"]:
            return OfflineStoreFeatureTableModel(**query_result["data"][0])
        return await self.create_document(data)

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
        if data.entity_lookup_info is None:
            # special handling for non-entity lookup feature tables to set name
            with self.allow_use_raw_query_filter():
                name_prefix = await self.get_or_create_name_prefix(
                    catalog_id=data.catalog_id, feature_store_id=data.feature_store_id
                )
                data.name_prefix = name_prefix

            # check if name already exists
            data.name = data.get_name()
            query_filter = {"name": data.name}
            query_result = await self.list_documents_as_dict(query_filter=query_filter, page_size=1)

            count = query_result["total"]
            data.name_suffix = None
            if count:
                # if name already exists, append a suffix
                data.name_suffix = str(count)
                data.name = data.get_name()

        output = await super().create_document(data)
        assert output.catalog_id == data.catalog_id
        return output
