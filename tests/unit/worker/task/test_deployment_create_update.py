"""
Test deployment create and update
"""

import pytest
from bson import ObjectId

from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.schema.worker.task.deployment_create_update import (
    CreateDeploymentPayload,
    DeploymentCreateUpdateTaskPayload,
    UpdateDeploymentPayload,
)
from featurebyte.worker.task.deployment_create_update import DeploymentCreateUpdateTask


@pytest.mark.asyncio
async def test_get_task_description_create(app_container):
    """
    Test get task description for deployment create
    """
    payload = DeploymentCreateUpdateTaskPayload(
        catalog_id=ObjectId(),
        deployment_payload=CreateDeploymentPayload(
            name="Test deployment",
            feature_list_id=ObjectId(),
            enabled=False,
        ),
    )
    task = app_container.get(DeploymentCreateUpdateTask)
    assert await task.get_task_description(payload) == 'Create deployment "Test deployment"'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "enabled,expected",
    [
        (True, 'Enable deployment "Test deployment"'),
        (False, 'Disable deployment "Test deployment"'),
    ],
)
async def test_get_task_description_update(persistent, enabled, expected, app_container: LazyAppContainer):
    """
    Test get task description for deployment update
    """
    deployment_id = ObjectId()
    catalog_id = ObjectId()
    await persistent.insert_one(
        collection_name="deployment",
        document={
            "_id": deployment_id,
            "catalog_id": catalog_id,
            "name": "Test deployment",
            "enabled": False,
            "feature_list_id": ObjectId(),
        },
        user_id=ObjectId(),
    )
    payload = DeploymentCreateUpdateTaskPayload(
        catalog_id=catalog_id,
        deployment_payload=UpdateDeploymentPayload(enabled=enabled),
        output_document_id=deployment_id,
    )
    app_container.override_instance_for_test("persistent", persistent)
    app_container.override_instance_for_test("catalog_id", catalog_id)
    task = app_container.get(DeploymentCreateUpdateTask)
    assert await task.get_task_description(payload) == expected
