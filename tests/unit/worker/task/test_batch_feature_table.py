"""
Test batch feature table
"""

import pytest
from bson import ObjectId

from featurebyte.schema.worker.task.batch_feature_table import BatchFeatureTableTaskPayload
from featurebyte.worker.task.batch_feature_table import BatchFeatureTableTask


@pytest.mark.asyncio
async def test_get_task_description(catalog, app_container):
    """
    Test get task description
    """
    payload = BatchFeatureTableTaskPayload(
        name="Test Features",
        feature_store_id=ObjectId(),
        batch_request_table_id=ObjectId(),
        deployment_id=ObjectId(),
        catalog_id=catalog.id,
    )
    app_container.override_instance_for_test("payload", payload.dict(by_alias=True))
    task = app_container.get(BatchFeatureTableTask)
    assert await task.get_task_description() == 'Save batch feature table "Test Features"'
