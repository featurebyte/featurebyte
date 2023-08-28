"""
Test target table task
"""
from unittest.mock import Mock
from uuid import uuid4

import pytest
from bson import ObjectId

from featurebyte.models.observation_table import SourceTableObservationInput
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.worker.task.target_table import TargetTableTask


@pytest.mark.asyncio
async def test_get_task_description(catalog):
    """
    Test get task description
    """
    payload = TargetTableTaskPayload(
        name="Test Target Table",
        feature_store_id=ObjectId(),
        catalog_id=catalog.id,
        target_id=ObjectId(),
        graph=QueryGraph(),
        node_names=["node_1"],
        request_input=SourceTableObservationInput(
            source=TabularSource(
                feature_store_id=ObjectId(),
                table_details=TableDetails(table_name="test_table"),
            ),
        ),
    )
    task = TargetTableTask(
        task_id=uuid4(),
        payload=payload.dict(by_alias=True),
        progress=Mock(),
        get_credential=Mock(),
        app_container=Mock(),
    )
    assert await task.get_task_description() == 'Save target table "Test Target Table"'
