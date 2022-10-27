"""
EventDataMigrationService class
"""
from featurebyte.migration.service import migrate
from featurebyte.models.event_data import EventDataModel
from featurebyte.service.event_data import EventDataService


class EventDataMigrationService(EventDataService):
    """EventDataMigrationService class"""

    @migrate(version=1, description="Rename collection name from event_data to tabular_data")
    async def change_collection_name_from_event_data_to_table_data(self) -> None:
        """Change collection name from event data to tabular_data"""
        await self.persistent.rename_collection(
            collection_name="event_data", new_collection_name="tabular_data"
        )

        def migrate_func(record):
            return EventDataModel(**record).dict(by_alias=True)

        # migrate all records and audit records
        to_iterate, page = True, 1
        while to_iterate:
            docs = await self.list_documents(page=1)
            for doc in docs["data"]:
                await self.persistent.migrate_record(
                    collection_name="tabular_data",
                    document_id=doc["_id"],
                    migrate_func=migrate_func,
                )
                await self.persistent.migrate_audit_records(
                    collection_name="tabular_data",
                    document_id=doc["_id"],
                    migrate_func=migrate_func,
                )

            to_iterate = bool(docs["total"] > (page * docs["page_size"]))

        # select a record & check
        sample_record = await self.persistent.find_one(
            collection_name="tabular_data", query_filter={}
        )
        assert sample_record["type"] == "event_data"
