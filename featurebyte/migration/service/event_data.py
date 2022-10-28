"""
EventDataMigrationService class
"""
from __future__ import annotations

from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.service.event_data import EventDataService


class EventDataMigrationService(EventDataService, MigrationServiceMixin):
    """EventDataMigrationService class"""

    @migrate(version=1, description="Rename collection name from event_data to tabular_data")
    async def change_collection_name_from_event_data_to_table_data(self) -> None:
        """Change collection name from event data to tabular_data"""
        await self.persistent.rename_collection(
            collection_name="event_data", new_collection_name="tabular_data"
        )

        # sample first 10 few records before migration
        sample_docs_before, total_before = await self.persistent.find(
            collection_name="tabular_data", query_filter={}, page_size=10
        )
        sample_docs_before_map = {doc["_id"]: doc for doc in sample_docs_before}

        # migrate all records and audit records
        await self.migrate_all_records(query_filter={})

        # check the sample records after migration
        sample_docs_after, total_after = await self.persistent.find(
            collection_name="tabular_data",
            query_filter={"_id": {"$in": list(sample_docs_before_map)}},
        )
        sample_docs_after_map = {doc["_id"]: doc for doc in sample_docs_after}

        assert total_after == total_before
        for doc_id, doc_before in sample_docs_after_map.items():
            doc_after = sample_docs_after_map[doc_id]
            assert doc_after == {**doc_before, "type": "event_data"}
