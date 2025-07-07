"""
Test batch feature table
"""

import pytest
from bson import ObjectId

from featurebyte.schema.batch_feature_table import OutputTableInfo
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
    task = app_container.get(BatchFeatureTableTask)
    assert await task.get_task_description(payload) == 'Save batch feature table "Test Features"'


@pytest.mark.asyncio
async def test_get_task_description_external_feature_table(catalog, app_container):
    """
    Test get task description for external feature table
    """
    payload = BatchFeatureTableTaskPayload(
        name="Test Features",
        feature_store_id=ObjectId(),
        batch_request_table_id=ObjectId(),
        deployment_id=ObjectId(),
        catalog_id=catalog.id,
        output_table_info=OutputTableInfo(
            name='"db"."schema"."table"',
        ),
    )
    task = app_container.get(BatchFeatureTableTask)
    assert (
        await task.get_task_description(payload)
        == 'Compute and append batch features to table "db"."schema"."table"'
    )
