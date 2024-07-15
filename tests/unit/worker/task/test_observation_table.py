"""
Test observation table
"""

import pytest
from bson import ObjectId

from featurebyte.models.observation_table import SourceTableObservationInput
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.worker.task.observation_table import ObservationTableTask


@pytest.mark.asyncio
async def test_get_task_description(catalog, app_container):
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
        primary_entity_ids=["63f94ed6ea1f050131379214"],
    )
    task = app_container.get(ObservationTableTask)
    assert (
        await task.get_task_description(payload) == 'Save observation table "Test Observation Table" from source table.'
    )
