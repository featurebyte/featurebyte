"""
Test batch feature table
"""
from unittest.mock import Mock
from uuid import uuid4

import pytest
from bson import ObjectId

from featurebyte.schema.worker.task.batch_feature_table import BatchFeatureTableTaskPayload
from featurebyte.worker.task.batch_feature_table import BatchFeatureTableTask


@pytest.mark.asyncio
async def test_get_task_description(catalog):
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
    task = BatchFeatureTableTask(
        task_id=uuid4(),
        payload=payload.dict(by_alias=True),
        progress=Mock(),
        get_credential=Mock(),
        app_container=Mock(),
    )
    assert await task.get_task_description() == 'Save batch feature table "Test Features"'
