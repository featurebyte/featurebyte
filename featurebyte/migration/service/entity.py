"""
EntityMigrationService class
"""
from __future__ import annotations

from featurebyte.migration.service import migrate
from featurebyte.migration.service.context import ContextMigrationService
from featurebyte.migration.service.dimension_data import DimensionDataMigrationService
from featurebyte.migration.service.event_data import EventDataMigrationService
from featurebyte.migration.service.feature import FeatureMigrationService
from featurebyte.migration.service.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisMigrationService,
)
from featurebyte.migration.service.feature_list import FeatureListMigrationService
from featurebyte.migration.service.feature_list_namespace import (
    FeatureListNamespaceMigrationService,
)
from featurebyte.migration.service.feature_namespace import FeatureNamespaceMigrationService
from featurebyte.migration.service.item_data import ItemDataMigrationService
from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.migration.service.scd_data import SCDDataMigrationService
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService


class EntityMigrationService(EntityService, MigrationServiceMixin):
    """EntityMigrationService class"""

    @migrate(version=5, description="Add fields tabular_data_ids and primary_tabular_data_ids")
    async def add_field_tabular_data_ids_and_primary_tabular_data_ids(self) -> None:
        """Add field name from tabular_data_ids and primary_tabular_data_ids"""
        # sample first 10 records before migration
        sample_docs_before, total_before = await self.persistent.find(
            collection_name="entity", query_filter={}, page_size=10
        )
        sample_docs_before_map = {doc["_id"]: doc for doc in sample_docs_before}

        # migrate all records and audit records
        await self.migrate_all_records(query_filter={})

        # check the sample records after migration
        sample_docs_after, total_after = await self.persistent.find(
            collection_name="entity",
            query_filter={"_id": {"$in": list(sample_docs_before_map)}},
        )
        sample_docs_after_map = {doc["_id"]: doc for doc in sample_docs_after}
        assert total_before == total_after
        for doc in sample_docs_after_map.values():
            assert "tabular_data_ids" in doc
            assert "primary_tabular_data_ids" in doc

    @migrate(version=9, description="Add field catalog_id")
    async def add_field_catalog_id(self) -> None:
        """Add catalog_id field"""

        for migrate_service_class in [
            EntityMigrationService,
            ContextMigrationService,
            DimensionDataMigrationService,
            EventDataMigrationService,
            ItemDataMigrationService,
            SCDDataMigrationService,
            FeatureMigrationService,
            FeatureNamespaceMigrationService,
            FeatureListMigrationService,
            FeatureListNamespaceMigrationService,
            FeatureJobSettingAnalysisMigrationService,
        ]:
            migrate_service = migrate_service_class(  # type: ignore
                user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
            )
            assert isinstance(migrate_service, BaseDocumentService)
            assert migrate_service.is_catalog_specific

            # sample first 10 records before migration
            sample_docs_before, total_before = await self.persistent.find(
                collection_name=migrate_service.collection_name, query_filter={}, page_size=10
            )
            sample_docs_before_map = {doc["_id"]: doc for doc in sample_docs_before}

            # migrate all records and audit records
            await migrate_service.migrate_all_records(query_filter=None)

            # check the sample records after migration
            sample_docs_after, total_after = await self.persistent.find(
                collection_name=migrate_service.collection_name,
                query_filter={"_id": {"$in": list(sample_docs_before_map)}},
            )
            sample_docs_after_map = {doc["_id"]: doc for doc in sample_docs_after}
            assert total_before == total_after
            for doc in sample_docs_after_map.values():
                assert "catalog_id" in doc
                assert doc["catalog_id"] == DEFAULT_CATALOG_ID
