"""
OfflineStoreFeatureTableService class
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

from bson import ObjectId, json_util
from redis.lock import Lock
from typeguard import typechecked

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
        if data.entity_lookup_info is None and data.precomputed_lookup_feature_table_info is None:
            # check if name already exists
            data.base_name = data.get_basename()
            data.name = data.get_name()
            query_filter = {
                "name_prefix": data.name_prefix,
                "base_name": data.base_name,
                "precomputed_lookup_feature_table_info": None,
            }
            query_result = await self.list_documents_as_dict(query_filter=query_filter, page_size=1)

            count = query_result["total"]
            data.name_suffix = None
            if count:
                # if name already exists, append a suffix
                data.name_suffix = str(count)
                data.name = data.get_name()

        if data.feature_cluster is not None:
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
        populate_remote_attributes: bool = True,
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

            table = OfflineStoreFeatureTableModel(**{
                **original_doc.model_dump(by_alias=True),
                **data.model_dump(by_alias=True),
            })
            table = await self._move_feature_cluster_to_storage(table)
            data.feature_cluster = None
            data.feature_cluster_path = table.feature_cluster_path

        output = await super().update_document(
            document_id,
            data,
            exclude_none=exclude_none,
            document=original_doc,
            skip_block_modification_check=skip_block_modification_check,
            populate_remote_attributes=populate_remote_attributes,
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
        populate_remote_attributes: bool = True,
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
                    populate_remote_attributes=populate_remote_attributes,
                )

        return await self._update_offline_feature_table(
            document_id,
            data,
            exclude_none=exclude_none,
            skip_block_modification_check=skip_block_modification_check,
            populate_remote_attributes=populate_remote_attributes,
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

    async def add_deployment_id(self, document_id: ObjectId, deployment_id: ObjectId) -> None:
        """
        Add deployment id to the offline store feature table

        Parameters
        ----------
        document_id: ObjectId
            Offline store feature table id
        deployment_id: ObjectId
            Deployment id
        """
        await self.update_documents(
            query_filter={"_id": document_id},
            update={"$addToSet": {"deployment_ids": deployment_id}},
        )

    async def remove_deployment_id(self, document_id: ObjectId, deployment_id: ObjectId) -> None:
        """
        Remove deployment id from the offline store feature table

        Parameters
        ----------
        document_id: ObjectId
            Offline store feature table id
        deployment_id: ObjectId
            Deployment id
        """
        await self.update_documents(
            query_filter={"_id": document_id},
            update={"$pull": {"deployment_ids": deployment_id}},
        )

    async def list_deprecated_entity_lookup_feature_tables_as_dict(
        self,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Retrieve entity lookup feature tables that deprecated for clean up purpose

        Yields
        -------
        AsyncIterator[OfflineStoreFeatureTableModel]
            List query output
        """
        async for doc in self.list_documents_as_dict_iterator(
            query_filter={"entity_lookup_info": {"$ne": None}},
            projection={"_id": 1, "name": 1},
        ):
            yield doc

    async def list_source_feature_tables_for_deployment(
        self, deployment_id: ObjectId
    ) -> AsyncIterator[OfflineStoreFeatureTableModel]:
        """
        Retrieve list of source feature tables in the catalog

        Parameters
        ----------
        deployment_id: ObjectId
            Deployment id to search for

        Yields
        -------
        AsyncIterator[OfflineStoreFeatureTableModel]
            List query output
        """
        async for doc in self.list_documents_iterator(
            query_filter={
                "precomputed_lookup_feature_table_info": {"$eq": None},
                "deployment_ids": deployment_id,
            }
        ):
            yield doc

    async def list_source_feature_tables(self) -> AsyncIterator[OfflineStoreFeatureTableModel]:
        """
        Retrieve list of source feature tables in the catalog

        Yields
        -------
        AsyncIterator[OfflineStoreFeatureTableModel]
            List query output
        """
        async for doc in self.list_documents_iterator(
            query_filter={"precomputed_lookup_feature_table_info": {"$eq": None}}
        ):
            yield doc

    async def list_precomputed_lookup_feature_tables_from_source(
        self,
        source_feature_table_id: ObjectId,
    ) -> AsyncIterator[OfflineStoreFeatureTableModel]:
        """
        Retrieve list of precomputed lookup feature tables associated with a source feature table

        Parameters
        ----------
        source_feature_table_id: ObjectId
            Feature table id to search for

        Yields
        -------
        AsyncIterator[OfflineStoreFeatureTableModel]
            List query output
        """
        async for doc in self.list_documents_iterator(
            query_filter={
                "precomputed_lookup_feature_table_info.source_feature_table_id": source_feature_table_id
            }
        ):
            yield doc

    async def list_precomputed_lookup_feature_tables_for_deployment(
        self, deployment_id: ObjectId
    ) -> AsyncIterator[OfflineStoreFeatureTableModel]:
        """
        Retrieve list of precomputed lookup feature tables in the catalog

        Parameters
        ----------
        deployment_id: ObjectId
            Deployment id to search for

        Yields
        -------
        AsyncIterator[OfflineStoreFeatureTableModel]
            List query output
        """
        async for doc in self.list_documents_iterator(
            query_filter={
                "precomputed_lookup_feature_table_info": {"$ne": None},
                "deployment_ids": deployment_id,
            }
        ):
            yield doc

    @typechecked
    async def list_feature_tables_for_aggregation_ids(
        self, aggregation_ids: list[str]
    ) -> AsyncIterator[OfflineStoreFeatureTableModel]:
        """
        Retrieve list of offline store feature tables associated with any of the given aggregation_ids

        Parameters
        ----------
        aggregation_ids: list[str]
            List of aggregation IDs of interest

        Yields
        ------
        AsyncIterator[OfflineStoreFeatureTableModel]
            Matching feature tables
        """
        async for doc in self.list_documents_iterator(
            query_filter={"aggregation_ids": {"$in": aggregation_ids}}
        ):
            yield doc
