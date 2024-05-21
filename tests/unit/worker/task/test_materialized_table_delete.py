"""
Test materialized table delete
"""

import pytest
from bson import ObjectId

from featurebyte.models.batch_request_table import SourceTableBatchRequestInput
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.schema.worker.task.materialized_table_delete import (
    MaterializedTableCollectionName,
    MaterializedTableDeleteTaskPayload,
)
from featurebyte.worker.task.materialized_table_delete import MaterializedTableDeleteTask


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "collection_name,expected_description",
    [
        (MaterializedTableCollectionName.OBSERVATION, "observation table"),
        (MaterializedTableCollectionName.HISTORICAL_FEATURE, "historical feature table"),
        (MaterializedTableCollectionName.BATCH_REQUEST, "batch request table"),
        (MaterializedTableCollectionName.BATCH_FEATURE, "batch feature table"),
        (MaterializedTableCollectionName.STATIC_SOURCE, "static source table"),
    ],
)
async def test_get_task_description(
    persistent, collection_name, expected_description, app_container: LazyAppContainer
):
    """
    Test get task description for materialized table delete
    """
    document_id = ObjectId()
    catalog_id = ObjectId()
    await persistent.insert_one(
        collection_name=collection_name,
        document={
            "_id": document_id,
            "catalog_id": catalog_id,
            "name": f"Test {expected_description}",
            "location": TabularSource(
                feature_store_id=ObjectId(), table_details=TableDetails(table_name="test")
            ).dict(by_alias=True),
            "columns_info": [],
            "num_rows": 100,
            "feature_list_id": ObjectId(),
            "request_input": SourceTableBatchRequestInput(
                source=TabularSource(
                    feature_store_id=ObjectId(),
                    table_details=TableDetails(table_name="test_table"),
                ),
            ).dict(by_alias=True),
            "most_recent_point_in_time": "2021-01-01",
            "target_id": ObjectId(),
            "batch_request_table_id": ObjectId(),
            "deployment_id": ObjectId(),
        },
        user_id=ObjectId(),
    )
    payload = MaterializedTableDeleteTaskPayload(
        catalog_id=catalog_id,
        collection_name=collection_name,
        document_id=document_id,
    )
    app_container.override_instance_for_test("persistent", persistent)
    app_container.override_instance_for_test("catalog_id", catalog_id)
    task = app_container.get(MaterializedTableDeleteTask)
    assert (
        await task.get_task_description(payload)
        == f'Delete {expected_description} "Test {expected_description}"'
    )
