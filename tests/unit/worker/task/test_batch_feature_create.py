"""
Test batch feature creation task
"""

import os
import textwrap
from unittest.mock import AsyncMock, patch

import pytest
from bson import ObjectId

from featurebyte.api.catalog import Catalog, Entity
from featurebyte.exception import RecordRetrievalException
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.feature import BatchFeatureItem
from featurebyte.worker.task.batch_feature_create import (
    BatchFeatureCreateTask,
    BatchFeatureCreateTaskPayload,
)
from featurebyte.worker.util.batch_feature_creator import execute_sdk_code, set_environment_variable


@pytest.fixture(name="test_catalog")
def fixture_catalog(snowflake_feature_store):
    """Fixture for catalog"""
    catalog = Catalog(name="test_catalog", default_feature_store_ids=[snowflake_feature_store.id])
    catalog.save()
    return catalog


def test_set_environment_variable():
    """Test set environment variable"""
    assert "TEST_ENV" not in os.environ
    with set_environment_variable("TEST_ENV", "TEST"):
        assert os.environ["TEST_ENV"] == "TEST"
    assert "TEST_ENV" not in os.environ


@pytest.mark.asyncio
async def test_execute_sdk_code(test_catalog, catalog):
    """Test execute sdk code"""
    _ = catalog

    # save the current active catalog
    Catalog.get_active()

    # check that the entity doesn't exist
    with pytest.raises(RecordRetrievalException):
        Entity.get(name="test_entity")

    # prepare the SDK code to create & save an entity
    sdk_code = textwrap.dedent(
        """
    from featurebyte import Catalog, Entity

    Entity.create(name="test_entity", serving_names=["test_entity_serving_name"])
    """
    ).strip()

    # execute the SDK code & check the entity's catalog id
    controller = AsyncMock()
    controller.service.collection_name = "entity"  # above code contains a call to Entity.create
    with patch("featurebyte.worker.util.batch_feature_creator.FeatureCreate"):
        await execute_sdk_code(
            catalog_id=test_catalog.id, code=sdk_code, feature_controller=controller
        )

    controller.create_feature.assert_called_once()


@pytest.mark.asyncio
async def test_get_task_description(app_container):
    """
    Test get task description
    """
    tabular_source = TabularSource(
        feature_store_id=ObjectId(),
        table_details=TableDetails(
            database_name="test_database",
            schema_name="test_schema",
            table_name="test_table",
        ),
    )
    payload = BatchFeatureCreateTaskPayload(
        graph=QueryGraph(),
        features=[
            BatchFeatureItem(
                id=ObjectId(),
                name="test_feature_1",
                node_name="node_1",
                tabular_source=tabular_source,
            ),
            BatchFeatureItem(
                id=ObjectId(),
                name="test_feature_2",
                node_name="node_2",
                tabular_source=tabular_source,
            ),
        ],
        catalog_id=ObjectId(),
        conflict_resolution="raise",
    )
    task = app_container.get(BatchFeatureCreateTask)
    assert await task.get_task_description(payload) == "Save 2 features"
