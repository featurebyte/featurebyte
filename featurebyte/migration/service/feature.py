"""
FeatureMigrationService class
"""
from __future__ import annotations

from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.service.feature import FeatureService


class FeatureMigrationService(FeatureService, MigrationServiceMixin):
    """FeatureMigrationService class"""

    @migrate(version=2, description="Rename field event_data_ids to tabular_data_ids")
    async def change_field_name_from_event_data_ids_to_tabular_data_ids(self) -> None:
        """Change field name from event_data_ids to tabular_data_ids"""
        # sample first 10 few records before migration
        sample_docs_before, total_before = await self.persistent.find(
            collection_name="feature", query_filter={}, page_size=10
        )
        sample_docs_before_map = {doc["_id"]: doc for doc in sample_docs_before}

        # migrate all records and audit records
        await self.migrate_all_records(query_filter={})

        # drop old field
        await self.persistent._update_many(  # pylint: disable=protected-access
            collection_name="feature", query_filter={}, update={"$unset": {"event_data_ids": 1}}
        )

        # check the sample records after migration
        sample_docs_after, total_after = await self.persistent.find(
            collection_name="feature",
            query_filter={"_id": {"$in": list(sample_docs_before_map)}},
        )
        sample_docs_after_map = {doc["_id"]: doc for doc in sample_docs_after}
        assert total_before == total_after
        for doc in sample_docs_after_map.values():
            assert "event_data_ids" not in doc
            assert "tabular_data_ids" in doc
