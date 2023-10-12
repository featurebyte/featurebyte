"""
Test feature list batch feature create
"""

import pytest
from bson import ObjectId

from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTaskPayload,
)
from featurebyte.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTask,
)


@pytest.mark.asyncio
async def test_get_task_description(app_container):
    """
    Test get task description
    """
    payload = FeatureListCreateWithBatchFeatureCreationTaskPayload(
        name="Test Feature list",
        output_feature_ids=[ObjectId(), ObjectId()],
        graph=QueryGraph(),
        features=[],
        catalog_id=ObjectId(),
        conflict_resolution="raise",
    )
    app_container.override_instance_for_test("payload", payload.dict(by_alias=True))
    task = app_container.get(FeatureListCreateWithBatchFeatureCreationTask)
    assert await task.get_task_description() == 'Save feature list "Test Feature list"'
