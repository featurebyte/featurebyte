"""
Test observation table
"""
from unittest.mock import Mock
from uuid import uuid4

import pytest
from bson import ObjectId

from featurebyte.models.observation_table import SourceTableObservationInput
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.worker.task.observation_table import ObservationTableTask


@pytest.mark.asyncio
async def test_get_task_description(catalog):
    """
    Test get task description
    """
    payload = ObservationTableTaskPayload(
        name="Test Observation Table",
        feature_store_id=ObjectId(),
        catalog_id=catalog.id,
        request_input=SourceTableObservationInput(
            source=TabularSource(
                feature_store_id=ObjectId(),
                table_details=TableDetails(table_name="test_table"),
            ),
        ),
    )
    task = ObservationTableTask(
        task_id=uuid4(),
        payload=payload.dict(by_alias=True),
        progress=Mock(),
        app_container=Mock(),
    )
    assert await task.get_task_description() == 'Save observation table "Test Observation Table"'
