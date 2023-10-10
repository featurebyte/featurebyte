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
async def test_get_task_description(catalog):
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
    task = ObservationTableUploadTask(
        task_id=uuid4(),
        payload=payload.dict(by_alias=True),
        progress=Mock(),
        app_container=Mock(),
    )
    assert (
        await task.get_task_description()
        == 'Upload observation table "Test Observation Table Upload" from CSV.'
    )
