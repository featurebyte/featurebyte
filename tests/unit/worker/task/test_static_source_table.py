"""
Test static source table
"""

import pytest
from bson import ObjectId

from featurebyte.models.static_source_table import SourceTableStaticSourceInput
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.worker.task.static_source_table import StaticSourceTableTaskPayload
from featurebyte.worker.task.static_source_table import StaticSourceTableTask


@pytest.mark.asyncio
async def test_get_task_description(catalog, app_container):
    """
    Test get task description
    """
    payload = StaticSourceTableTaskPayload(
        name="Test Static Source Table",
        feature_store_id=ObjectId(),
        catalog_id=catalog.id,
        request_input=SourceTableStaticSourceInput(
            source=TabularSource(
                feature_store_id=ObjectId(),
                table_details=TableDetails(table_name="test_table"),
            ),
        ),
    )
    task = app_container.get(StaticSourceTableTask)
    assert await task.get_task_description(payload) == 'Save static source table "Test Static Source Table"'
