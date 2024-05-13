"""
Offline store feature table migration service
"""

from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import BaseDocumentServiceT, BaseMongoCollectionMigration
from featurebyte.persistent import Persistent
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService


class OfflineStoreFeatureTableMigrationServiceV9(BaseMongoCollectionMigration):
    """
    OfflineStoreFeatureTableMigrationService class

    This class is used to migrate the offline store feature table records to add base_name field.
    """

    # skip audit migration for this migration
    skip_audit_migration = True

    def __init__(
        self,
        persistent: Persistent,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
    ):
        super().__init__(persistent)
        self.offline_store_feature_table_service = offline_store_feature_table_service

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.offline_store_feature_table_service  # type: ignore[return-value]

    @migrate(version=9, description="Add base_name to offline store feature table records")
    async def add_primary_entity_ids_and_relationships_info(self) -> None:
        """Add base_name to offline store feature table records"""
        query_filter = {"precomputed_lookup_feature_table_info": None}
        async for table in self.offline_store_feature_table_service.list_documents_iterator(
            query_filter=query_filter
        ):
            await self.offline_store_feature_table_service.update_documents(
                query_filter={"_id": table.id},
                update={"$set": {"base_name": table.get_basename()}},
            )
            updated_table = await self.offline_store_feature_table_service.get_document(table.id)
            assert updated_table.base_name is not None
