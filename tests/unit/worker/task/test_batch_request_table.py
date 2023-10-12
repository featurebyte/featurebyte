"""
Test batch request table
"""

import pytest
from bson import ObjectId

from featurebyte.models.batch_request_table import SourceTableBatchRequestInput
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.worker.task.batch_request_table import BatchRequestTableTaskPayload
from featurebyte.worker.task.batch_request_table import BatchRequestTableTask


@pytest.mark.asyncio
async def test_get_task_description(catalog, app_container):
    """
    Test get task description
    """
    payload = BatchRequestTableTaskPayload(
        name="Test Batch Request Table",
        feature_store_id=ObjectId(),
        catalog_id=catalog.id,
        request_input=SourceTableBatchRequestInput(
            source=TabularSource(
                feature_store_id=ObjectId(),
                table_details=TableDetails(table_name="test_table"),
            ),
        ),
    )
    app_container.override_instance_for_test("payload", payload.dict(by_alias=True))
    task = app_container.get(BatchRequestTableTask)
    assert (
        await task.get_task_description() == 'Save batch request table "Test Batch Request Table"'
    )
