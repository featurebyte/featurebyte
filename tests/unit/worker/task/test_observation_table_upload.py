"""
Test observation table upload
"""
from unittest.mock import Mock
from uuid import uuid4

import pytest
from bson import ObjectId

from featurebyte.models.observation_table import UploadedFileInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.schema.worker.task.observation_table_upload import (
    ObservationTableUploadTaskPayload,
)
from featurebyte.worker.task.observation_table_upload import ObservationTableUploadTask


@pytest.mark.asyncio
async def test_get_task_description(catalog, app_container):
    """
    Test get task description
    """
    payload = ObservationTableUploadTaskPayload(
        name="Test Observation Table Upload",
        feature_store_id=ObjectId(),
        catalog_id=catalog.id,
        request_input=UploadedFileInput(type=RequestInputType.UPLOADED_FILE),
        observation_set_storage_path="filepath",
    )
    app_container.override_instance_for_test("task_id", uuid4())
    app_container.override_instance_for_test("progress", Mock())
    app_container.override_instance_for_test("payload", payload.dict(by_alias=True))
    task = app_container.get(ObservationTableUploadTask)
    assert (
        await task.get_task_description()
        == 'Upload observation table "Test Observation Table Upload" from CSV.'
    )
