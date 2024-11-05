"""
Offline store feature table migration service
"""

from featurebyte.logging import get_logger
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import BaseDocumentServiceT, BaseMongoCollectionMigration
from featurebyte.persistent import Persistent
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService

logger = get_logger(__name__)


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
    async def add_base_name_to_offline_store_feature_table_records(self) -> None:
        """Add base_name to offline store feature table records"""
        query_filter = {"precomputed_lookup_feature_table_info": None}
        async for table in self.offline_store_feature_table_service.list_documents_iterator(
            query_filter=query_filter, populate_remote_attributes=False
        ):
            await self.offline_store_feature_table_service.update_documents(
                query_filter={"_id": table.id},
                update={"$set": {"base_name": table.get_basename()}},
            )
            updated_table = await self.offline_store_feature_table_service.get_document(
                table.id, populate_remote_attributes=False
            )
            assert updated_table.base_name is not None

    @migrate(
        version=11,
        description="Migrate feature job settings to new format (offline_store_feature_table collection).",
    )
    async def migrate_feature_job_setting_to_new_format(self) -> None:
        """Migrate feature job settings to new format"""
        sanity_check_sample_size = 10

        # use the normal query filter (contains catalog ID filter)
        query_filter = await self.delegate_service.construct_list_query_filter()
        total_before = await self.get_total_record(query_filter=query_filter)

        # migrate all records
        await self.migrate_all_records(query_filter=query_filter)

        # check the sample records after migration
        sample_docs_after, total_after = await self.persistent.find(
            collection_name=self.collection_name,
            query_filter=query_filter,
            page_size=sanity_check_sample_size,
        )
        assert total_before == total_after, (total_before, total_after)
        for doc in sample_docs_after:
            fjs = doc.get("feature_job_setting")
            if fjs:
                assert "blind_spot" in fjs, fjs
                assert "period" in fjs, fjs
                assert "offset" in fjs, fjs
                assert "execution_buffer" in fjs, fjs

        logger.info("Migrated all records successfully (total: %d)", total_after)
