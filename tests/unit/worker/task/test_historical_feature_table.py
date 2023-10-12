"""
Test historical feature table task
"""
from unittest.mock import Mock
from uuid import uuid4

import pytest
from bson import ObjectId

from featurebyte.models.feature_list import FeatureCluster
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.feature_list import FeatureListGetHistoricalFeatures
from featurebyte.schema.worker.task.historical_feature_table import (
    HistoricalFeatureTableTaskPayload,
)
from featurebyte.worker.task.historical_feature_table import HistoricalFeatureTableTask


@pytest.mark.asyncio
async def test_get_task_description(app_container):
    """
    Test get task description for historical feature table
    """
    payload = HistoricalFeatureTableTaskPayload(
        catalog_id=ObjectId(),
        featurelist_get_historical_features=FeatureListGetHistoricalFeatures(
            feature_clusters=[
                FeatureCluster(
                    feature_store_id=ObjectId(),
                    graph=QueryGraph(),
                    node_names=["node_1"],
                )
            ],
            feature_list_id=None,
        ),
        name="Test historical feature table",
        feature_store_id=ObjectId(),
    )
    app_container.override_instance_for_test("task_id", uuid4())
    app_container.override_instance_for_test("progress", Mock())
    app_container.override_instance_for_test("payload", payload.dict(by_alias=True))
    task = app_container.get(HistoricalFeatureTableTask)
    assert (
        await task.get_task_description()
        == 'Save historical feature table "Test historical feature table"'
    )
