"""
Test online store cleanup task
"""

import pytest
from bson import ObjectId

from featurebyte.schema.worker.task.online_store_cleanup import OnlineStoreCleanupTaskPayload
from featurebyte.worker.task.online_store_cleanup import OnlineStoreCleanupTask


@pytest.mark.asyncio
async def test_get_task_description(app_container):
    """
    Test get task description
    """
    payload = OnlineStoreCleanupTaskPayload(
        catalog_id=ObjectId(),
        feature_store_id=ObjectId(),
        online_store_table_name="Test Online Store Table",
    )
    app_container.override_instance_for_test("payload", payload.dict(by_alias=True))
    task = app_container.get(OnlineStoreCleanupTask)
    assert (
        await task.get_task_description() == 'Clean up online store table "Test Online Store Table"'
    )
