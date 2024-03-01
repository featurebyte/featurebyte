"""
OfflineStoreFeatureTableService class
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from bson import ObjectId, json_util

from featurebyte.models.feature_list import FeatureCluster
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

    async def _populate_remote_attributes(
        self, document: OfflineStoreFeatureTableModel
    ) -> OfflineStoreFeatureTableModel:
        if document.feature_cluster_path:
            feature_cluster_json = await self.storage.get_text(Path(document.feature_cluster_path))
            document.internal_feature_cluster = json_util.loads(feature_cluster_json)
        return document

    async def _move_feature_cluster_to_storage(
        self, document: OfflineStoreFeatureTableModel
    ) -> OfflineStoreFeatureTableModel:
        feature_cluster_path = self.get_full_remote_file_path(
            f"offline_store_feature_table/{document.id}/feature_cluster.json"
        )
        assert isinstance(document.internal_feature_cluster, FeatureCluster)
        await self.storage.put_text(
            json_util.dumps(document.internal_feature_cluster.json_dict()), feature_cluster_path
        )
        document.feature_cluster_path = str(feature_cluster_path)
        document.internal_feature_cluster = None
        return document

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

        Raises
        ------
        Exception
            If the document creation fails
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

        await self._move_feature_cluster_to_storage(data)
        try:
            output = await super().create_document(data)
            assert output.catalog_id == data.catalog_id
            return output
        except Exception as exc:
            if data.feature_cluster_path:
                await self.storage.delete(Path(data.feature_cluster_path))
            raise exc

    async def update_online_last_materialized_at(
        self,
        document_id: ObjectId,
        online_store_id: ObjectId,
        last_materialized_at: datetime,
    ) -> None:
        """
        Update the last materialized at timestamp for the given online_store_id

        Parameters
        ----------
        document_id: ObjectId
            OfflineStoreFeatureTableModel id
        online_store_id: ObjectId
            Online store id
        last_materialized_at: datetime
            Last materialized at timestamp to use
        """
        document = await self.get_document(
            document_id=document_id, populate_remote_attributes=False
        )
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
