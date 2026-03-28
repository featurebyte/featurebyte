"""
Test observation table upload
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.models.observation_table import UploadedFileInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
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
        request_input=UploadedFileInput(
            type=RequestInputType.UPLOADED_FILE, file_name="random.csv"
        ),
        observation_set_storage_path="filepath",
        primary_entity_ids=["63f94ed6ea1f050131379214"],
        file_format="csv",
        uploaded_file_name="random.csv",
    )
    task = app_container.get(ObservationTableUploadTask)
    assert (
        await task.get_task_description(payload)
        == 'Save observation table "Test Observation Table Upload" from csv file.'
    )


def _make_task_with_mocks(mock_create_observation_table):
    """Create an ObservationTableUploadTask with mocked dependencies."""
    feature_store_id = ObjectId()
    table_details = TableDetails(table_name="test_table", schema_name="schema", database_name="db")
    location = TabularSource(feature_store_id=feature_store_id, table_details=table_details)

    task = ObservationTableUploadTask.__new__(ObservationTableUploadTask)
    task.catalog_service = Mock()
    task.catalog_service.get_document = AsyncMock(
        return_value=Mock(default_feature_store_ids=[feature_store_id])
    )
    task.feature_store_service = Mock()
    task.feature_store_service.get_document = AsyncMock(return_value=Mock())
    task.session_manager_service = Mock()
    task.session_manager_service.get_feature_store_session = AsyncMock(return_value=AsyncMock())
    task.temp_storage = Mock()
    task.temp_storage.get_dataframe = AsyncMock(
        return_value=pd.DataFrame({"POINT_IN_TIME": ["2021-01-01"], "entity_col": ["a"]})
    )
    task.task_progress_updater = Mock()
    task.task_progress_updater.update_progress = AsyncMock()
    task.observation_table_service = Mock()
    task.observation_table_service.generate_materialized_table_location = AsyncMock(
        return_value=location
    )
    task.observation_table_service.get_observation_table_task_payload = AsyncMock(
        return_value=Mock()
    )
    task.observation_table_task = Mock()
    task.observation_table_task.create_observation_table = mock_create_observation_table
    return task


@asynccontextmanager
async def _noop_drop_table_on_error(*args, **kwargs):
    yield


@pytest.mark.asyncio
async def test_override_model_params_does_not_overwrite_target_namespace_id_when_none():
    """
    Test that when payload.target_namespace_id is None, the override_model_params
    passed to create_observation_table does not include target_namespace_id.

    This prevents overwriting a target_namespace_id that was determined by
    create_observation_table via silent target computation from a use case.
    """
    captured_params = {}

    async def mock_create_observation_table(payload, override_model_params=None):
        captured_params["override_model_params"] = override_model_params

    task = _make_task_with_mocks(mock_create_observation_table)

    payload = ObservationTableUploadTaskPayload(
        name="Test Upload",
        feature_store_id=ObjectId(),
        catalog_id=ObjectId(),
        request_input=UploadedFileInput(
            type=RequestInputType.UPLOADED_FILE, file_name="test.parquet"
        ),
        observation_set_storage_path="obs/test.parquet",
        primary_entity_ids=["63f94ed6ea1f050131379214"],
        file_format="parquet",
        uploaded_file_name="test.parquet",
        use_case_id=ObjectId(),
        target_namespace_id=None,
    )

    with patch.object(task, "drop_table_on_error", _noop_drop_table_on_error):
        await task.execute(payload)

    override_params = captured_params["override_model_params"]
    assert "target_namespace_id" not in override_params, (
        "override_model_params should not include target_namespace_id when it is None, "
        "otherwise it overwrites the value determined by silent target computation"
    )


@pytest.mark.asyncio
async def test_override_model_params_includes_target_namespace_id_when_set():
    """
    Test that when payload.target_namespace_id is set (user specified target_column),
    the override_model_params correctly includes it.
    """
    target_namespace_id = ObjectId()

    captured_params = {}

    async def mock_create_observation_table(payload, override_model_params=None):
        captured_params["override_model_params"] = override_model_params

    task = _make_task_with_mocks(mock_create_observation_table)

    payload = ObservationTableUploadTaskPayload(
        name="Test Upload With Target",
        feature_store_id=ObjectId(),
        catalog_id=ObjectId(),
        request_input=UploadedFileInput(
            type=RequestInputType.UPLOADED_FILE, file_name="test.parquet"
        ),
        observation_set_storage_path="obs/test.parquet",
        primary_entity_ids=["63f94ed6ea1f050131379214"],
        file_format="parquet",
        uploaded_file_name="test.parquet",
        target_namespace_id=target_namespace_id,
    )

    with patch.object(task, "drop_table_on_error", _noop_drop_table_on_error):
        await task.execute(payload)

    override_params = captured_params["override_model_params"]
    assert override_params["target_namespace_id"] == target_namespace_id
