"""
OfflineStoreFeatureTableService class
"""
from __future__ import annotations

from datetime import datetime

from bson import ObjectId

from featurebyte.models.offline_store_feature_table import (
    OfflineStoreFeatureTableModel,
    OfflineStoreFeatureTableUpdate,
    OnlineStoreLastMaterializedAt,
    OnlineStoresLastMaterializedAtUpdate,
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

    async def update_online_last_materialized_at(
        self,
        document_id: ObjectId,
        online_store_id: ObjectId,
        last_materialized_at: datetime,
    ) -> None:
        document = await self.get_document(document_id=document_id)
        new_entry = OnlineStoreLastMaterializedAt(
            online_store_id=online_store_id,
            value=last_materialized_at,
        )
        updated_online_stores_last_materialized_at = [new_entry] + [
            entry
            for entry in document.online_stores_last_materialized_at
            if entry.online_store_id != online_store_id
        ]
        update_schema = OnlineStoresLastMaterializedAtUpdate(
            online_stores_last_materialized_at=updated_online_stores_last_materialized_at
        )
        await self.update_document(
            document_id,
            update_schema,
            document=document,
        )
