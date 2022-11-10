"""
EntityMigrationService class
"""
from __future__ import annotations

from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import MigrationServiceMixin
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
