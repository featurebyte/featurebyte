"""
OfflineStoreFeatureTableService class
"""
from __future__ import annotations

from typing import Optional

from datetime import datetime
from pathlib import Path

from bson import ObjectId, json_util
from redis.lock import Lock

from featurebyte.models.feature_list import FeatureCluster
from featurebyte.models.offline_store_feature_table import (
    FeaturesUpdate,
    OfflineStoreFeatureTableModel,
    OfflineStoreFeatureTableUpdate,
    OnlineStoreLastMaterializedAt,
    OnlineStoresLastMaterializedAtUpdate,
)
from featurebyte.service.base_document import BaseDocumentService

OFFLINE_STORE_FEATURE_TABLE_REDIS_LOCK_TIMEOUT = 120  # a maximum life for the lock in seconds


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

    def get_feature_cluster_storage_lock(self, document_id: ObjectId, timeout: int) -> Lock:
        """
        Get feature cluster storage lock

        Parameters
        ----------
        document_id: ObjectId
            OfflineStoreFeatureTableModel id
        timeout: int
            Maximum life for the lock in seconds

        Returns
        -------
        Lock
        """
        return self.redis.lock(f"offline_store_feature_table_update:{document_id}", timeout=timeout)

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
            feature_cluster_dict = json_util.loads(feature_cluster_json)
            document.feature_cluster = FeatureCluster(**feature_cluster_dict)
        return document

    async def _move_feature_cluster_to_storage(
        self, document: OfflineStoreFeatureTableModel
    ) -> OfflineStoreFeatureTableModel:
        feature_cluster_path = self.get_full_remote_file_path(
            f"offline_store_feature_table/{document.id}/feature_cluster.json"
        )
        assert isinstance(document.feature_cluster, FeatureCluster)
        await self.storage.put_text(
            json_util.dumps(document.feature_cluster.json_dict()), feature_cluster_path
        )
        document.feature_cluster_path = str(feature_cluster_path)
        document.feature_cluster = None
        return document

    async def _create_document(
        self, data: OfflineStoreFeatureTableModel
    ) -> OfflineStoreFeatureTableModel:
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

        data = await self._move_feature_cluster_to_storage(data)
        try:
            output = await super().create_document(data)
            assert output.catalog_id == data.catalog_id
            return output
        except Exception as exc:
            if data.feature_cluster_path:
                await self.storage.delete(Path(data.feature_cluster_path))
            raise exc

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
        with self.get_feature_cluster_storage_lock(
            data.id, timeout=OFFLINE_STORE_FEATURE_TABLE_REDIS_LOCK_TIMEOUT
        ):
            return await self._create_document(data)

    async def _update_offline_feature_table(
        self,
        document_id: ObjectId,
        data: OfflineStoreFeatureTableUpdate,
        exclude_none: bool = True,
        skip_block_modification_check: bool = False,
    ) -> OfflineStoreFeatureTableModel:
        original_doc = await self.get_document(
            document_id=document_id, populate_remote_attributes=False
        )
        if isinstance(data, FeaturesUpdate):
            assert (
                data.feature_cluster_path is None
            ), "feature_cluster_path should not be set in update"
            if original_doc.feature_cluster_path:
                # attempt to remove the old feature cluster
                await self.storage.try_delete_if_exists(Path(original_doc.feature_cluster_path))

            table = OfflineStoreFeatureTableModel(
                **{**original_doc.dict(by_alias=True), **data.dict(by_alias=True)}
            )
            table = await self._move_feature_cluster_to_storage(table)
            data.feature_cluster = None
            data.feature_cluster_path = table.feature_cluster_path

        output = await super().update_document(
            document_id,
            data,
            exclude_none=exclude_none,
            document=original_doc,
            skip_block_modification_check=skip_block_modification_check,
        )
        assert output is not None
        return output

    async def update_document(
        self,
        document_id: ObjectId,
        data: OfflineStoreFeatureTableUpdate,
        exclude_none: bool = True,
        document: Optional[OfflineStoreFeatureTableModel] = None,
        return_document: bool = True,
        skip_block_modification_check: bool = False,
    ) -> Optional[OfflineStoreFeatureTableModel]:
        if isinstance(data, FeaturesUpdate):
            with self.get_feature_cluster_storage_lock(
                document_id, timeout=OFFLINE_STORE_FEATURE_TABLE_REDIS_LOCK_TIMEOUT
            ):
                return await self._update_offline_feature_table(
                    document_id,
                    data,
                    exclude_none=exclude_none,
                    skip_block_modification_check=skip_block_modification_check,
                )

        return await self._update_offline_feature_table(
            document_id,
            data,
            exclude_none=exclude_none,
            skip_block_modification_check=skip_block_modification_check,
        )

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
