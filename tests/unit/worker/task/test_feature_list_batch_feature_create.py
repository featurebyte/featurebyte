"""
Test feature list batch feature create
"""
from unittest.mock import Mock
from uuid import uuid4

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
async def test_get_task_description():
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
    task = FeatureListCreateWithBatchFeatureCreationTask(
        task_id=uuid4(),
        payload=payload.dict(by_alias=True),
        progress=Mock(),
        get_credential=Mock(),
        app_container=Mock(),
    )
    assert await task.get_task_description() == 'Save feature list "Test Feature list"'
