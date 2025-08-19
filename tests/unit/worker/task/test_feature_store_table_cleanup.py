"""
Tests for FeatureStoreTableCleanupTask
"""

import pytest
from bson import ObjectId

from featurebyte.schema.worker.task.feature_store_table_cleanup import (
    FeatureStoreTableCleanupTaskPayload,
)
from featurebyte.worker.task.feature_store_table_cleanup import FeatureStoreTableCleanupTask


@pytest.mark.asyncio
async def test_get_task_description(app_container):
    """
    Test get task description
    """
    catalog_id = ObjectId()
    feature_store_id = ObjectId()
    payload = FeatureStoreTableCleanupTaskPayload(
        catalog_id=catalog_id, feature_store_id=feature_store_id
    )
    task = app_container.get(FeatureStoreTableCleanupTask)
    expected = f'Clean up feature store tables for feature store "{feature_store_id}"'
    assert await task.get_task_description(payload) == expected
