"""
EventDataMigrationService class
"""
from featurebyte.migration.service import migrate
from featurebyte.service.event_data import EventDataService


class EventDataMigrationService(EventDataService):
    """EventDataMigrationService class"""

    @migrate(version=1, description="Rename collection name from event_data to tabular_data")
    async def change_collection_name_from_event_data_to_table_data(self) -> None:
        """Change collection name from event data to tabular_data"""
        # TODO: to be implemented in the next PR
