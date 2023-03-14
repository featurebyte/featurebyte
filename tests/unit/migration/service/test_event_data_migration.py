"""
Test EventDataMigrationService
"""
import copy
import os
from unittest.mock import AsyncMock, Mock, patch

import pytest
from bson import json_util
from bson.objectid import ObjectId

from featurebyte.migration.service.event_data import EventDataMigrationService
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.schema.event_data import EventDataCreate


@pytest.mark.asyncio
async def test_migration_v1__when_event_data_collection_not_exists(user, persistent):
    """Test migration when the collection does not exist"""
    collection_names_before = await persistent.list_collection_names()
    assert "event_data" not in collection_names_before

    # run migration (do nothing)
    event_data_migration_service = EventDataMigrationService(
        user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
    )
    await event_data_migration_service.change_collection_name_from_event_data_to_table_data()

    # check no change in collection names
    collection_names = await persistent.list_collection_names()
    assert collection_names == collection_names_before


@pytest.mark.asyncio
@patch("featurebyte.service.base_data_document.FeatureStoreService")
async def test_migrate_all_records(
    mock_feature_store_service, snowflake_feature_store, user, persistent, test_dir
):
    """Test migrate all records"""
    mock_feature_store_service.return_value.get_document = AsyncMock(
        return_value=snowflake_feature_store
    )

    event_data_migration_service = EventDataMigrationService(
        user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
    )
    total_records = 15
    event_data_fixture_path = os.path.join(test_dir, "fixtures/migration/event_data")
    event_data_payload = json_util.loads(open(event_data_fixture_path).read())[0]

    for i in range(total_records):
        payload = copy.deepcopy(event_data_payload)
        payload["_id"] = ObjectId()
        payload["name"] = f"{payload['name']}_{i}"
        table_name = payload["tabular_source"]["table_details"]["table_name"]
        payload["tabular_source"]["table_details"]["table_name"] = f"{table_name}_{i}"
        await event_data_migration_service.create_document(data=EventDataCreate(**payload))

    list_res = await event_data_migration_service.list_documents()
    assert list_res["total"] == total_records

    # check migrate_all_records run without any error
    await event_data_migration_service.migrate_all_records()

    list_res = await event_data_migration_service.list_documents()
    assert list_res["total"] == total_records
