"""
Test online store cleanup task
"""
from unittest.mock import Mock
from uuid import uuid4

import pytest
from bson import ObjectId

from featurebyte.schema.worker.task.online_store_cleanup import OnlineStoreCleanupTaskPayload
from featurebyte.worker.task.online_store_cleanup import OnlineStoreCleanupTask


@pytest.mark.asyncio
async def test_get_task_description():
    """
    Test get task description
    """
    payload = OnlineStoreCleanupTaskPayload(
        catalog_id=ObjectId(),
        feature_store_id=ObjectId(),
        online_store_table_name="Test Online Store Table",
    )
    task = OnlineStoreCleanupTask(
        task_id=uuid4(),
        payload=payload.dict(by_alias=True),
        progress=Mock(),
        get_credential=Mock(),
        app_container=Mock(),
    )
    assert (
        await task.get_task_description() == 'Clean up online store table "Test Online Store Table"'
    )
