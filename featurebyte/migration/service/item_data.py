"""
ItemDataMigrationService class
"""
from featurebyte.migration.service import migrate
from featurebyte.service.item_data import ItemDataService


class ItemDataMigrationService(ItemDataService):
    """ItemDataMigrationService class"""

    @migrate(version=2, description="Move records from item_data to tabular_data collection")
    async def move_documents_from_item_data_to_tabular_data(self) -> None:
        """Move document records from item_data to tabular_data collection"""
        # TODO: to be implemented in the next PR
