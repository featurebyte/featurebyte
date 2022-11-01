"""
Test EventDataMigrationService
"""
import pytest

from featurebyte.migration.service.event_data import EventDataMigrationService


@pytest.mark.asyncio
async def test_migration_v1__when_event_data_collection_not_exists(user, persistent):
    """Test migration when the collection does not exist"""
    collection_names_before = await persistent.list_collection_names()
    assert "event_data" not in collection_names_before

    # run migration (do nothing)
    event_data_migration_service = EventDataMigrationService(user=user, persistent=persistent)
    await event_data_migration_service.change_collection_name_from_event_data_to_table_data()

    # check no change in collection names
    collection_names = await persistent.list_collection_names()
    assert collection_names == collection_names_before
