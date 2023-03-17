"""
EventDataMigrationService class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Any, Dict

from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.service.event_data import EventDataService


class EventDataMigrationService(EventDataService, MigrationServiceMixin):
    """EventDataMigrationService class"""

    @migrate(version=1, description="Rename collection name from event_data to tabular_data")
    async def change_collection_name_from_event_data_to_table_data(self) -> None:
        """Change collection name from event data to tabular_data"""
        collection_names = await self.persistent.list_collection_names()
        if "event_data" not in collection_names:
            # do nothing if the collection does not exist
            return

        await self.persistent.rename_collection(
            collection_name="event_data", new_collection_name="tabular_data"
        )

        # sample first 10 few records before migration
        query_filter: Dict[str, Any] = {}
        page_size = 10
        _, total_before = await self.persistent.find(
            collection_name="tabular_data", query_filter=query_filter, page_size=page_size
        )

        # migrate all records and audit records
        await self.migrate_all_records(query_filter=query_filter)

        # check the sample records after migration
        sample_docs_after, total_after = await self.persistent.find(
            collection_name="tabular_data", query_filter=query_filter, page_size=page_size
        )
        assert total_before == total_after, (total_before, total_after)
        for doc in sample_docs_after:
            assert doc["type"] == "event_data"
