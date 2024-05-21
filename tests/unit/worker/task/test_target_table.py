"""
Test target table task
"""

import pytest
from bson import ObjectId

from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.worker.task.target_table import TargetTableTask


@pytest.mark.asyncio
async def test_get_task_description(catalog, app_container):
    """
    Test get task description
    """
    payload = TargetTableTaskPayload(
        name="Test Target Table",
        feature_store_id=ObjectId(),
        catalog_id=catalog.id,
        graph=QueryGraph(),
        node_names=["node_1"],
        observation_table_id=ObjectId(),
    )
    task = app_container.get(TargetTableTask)
    assert await task.get_task_description(payload) == 'Save target table "Test Target Table"'
