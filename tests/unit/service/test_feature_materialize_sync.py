"""
Unit tests for FeatureMaterializeSyncService
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from threading import Thread
from unittest.mock import patch

import pytest
import pytest_asyncio
from bson import ObjectId
from freezegun import freeze_time

from featurebyte.enum import DBVarType
from featurebyte.models.feature_materialize_prerequisite import PrerequisiteTileTask
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.service.feature_materialize_prerequisite import (
    FeatureMaterializePrerequisiteService,
)
from featurebyte.service.feature_materialize_sync import FeatureMaterializeSyncService


@pytest.fixture(autouse=True)
def mock_poll_period():
    """
    Fixture to patch POLL_PERIOD_SECONDS to a lower value to allow tests to complete faster
    """
    with patch("featurebyte.service.feature_materialize_sync.POLL_PERIOD_SECONDS", 1):
        yield


@pytest.fixture
def service(app_container) -> FeatureMaterializeSyncService:
    """
    Fixture for a FeatureMaterializeSyncService
    """
    return app_container.feature_materialize_sync_service


@pytest.fixture
def feature_materialize_prerequisite_service(
    app_container,
) -> FeatureMaterializePrerequisiteService:
    """
    Fixture for a FeatureMaterializePrerequisiteService
    """
    return app_container.feature_materialize_prerequisite_service


@pytest.fixture
def mock_task_manager_submit():
    """
    Fixture to mock TaskManager submit method
    """
    with patch("featurebyte.service.task_manager.TaskManager.submit") as mock_submit:
        yield mock_submit


@pytest_asyncio.fixture
async def offline_store_feature_table_1(app_container):
    """
    Fixture for a saved offline store feature table
    """
    model = OfflineStoreFeatureTableModel(
        name="feature_table_1",
        feature_ids=[ObjectId()],
        primary_entity_ids=[ObjectId()],
        serving_names=["cust_id"],
        has_ttl=False,
        output_column_names=["x"],
        output_dtypes=[DBVarType.FLOAT],
        catalog_id=app_container.catalog_id,
        feature_job_setting=FeatureJobSetting(period="1h", offset="10m", blind_spot="5m"),
        aggregation_ids=["agg_id_x"],
    )
    doc = await app_container.offline_store_feature_table_service.create_document(model)
    yield doc


@pytest_asyncio.fixture
async def offline_store_feature_table_2(app_container):
    """
    Fixture for a saved offline store feature table
    """
    model = OfflineStoreFeatureTableModel(
        name="feature_table_2",
        feature_ids=[ObjectId()],
        primary_entity_ids=[ObjectId()],
        serving_names=["cust_id"],
        has_ttl=False,
        output_column_names=["x_plus_y"],
        output_dtypes=[DBVarType.FLOAT],
        catalog_id=app_container.catalog_id,
        feature_job_setting=FeatureJobSetting(period="1h", offset="10m", blind_spot="5m"),
        aggregation_ids=["agg_id_x", "agg_id_y"],
    )
    doc = await app_container.offline_store_feature_table_service.create_document(model)
    yield doc


@pytest_asyncio.fixture
async def offline_store_feature_table_no_aggregation_ids(app_container):
    """
    Fixture for a saved offline store feature table
    """
    model = OfflineStoreFeatureTableModel(
        name="feature_table_3",
        feature_ids=[ObjectId()],
        primary_entity_ids=[ObjectId()],
        serving_names=["cust_id"],
        has_ttl=False,
        output_column_names=["lookup_feature"],
        output_dtypes=[DBVarType.FLOAT],
        catalog_id=app_container.catalog_id,
        feature_job_setting=FeatureJobSetting(period="1d", offset="0s", blind_spot="0s"),
        aggregation_ids=[],
    )
    doc = await app_container.offline_store_feature_table_service.create_document(model)
    yield doc


@pytest.mark.asyncio
async def test_initialize_prerequisite(service, offline_store_feature_table_1):
    """
    Test initializing and retrieving prerequisite
    """
    current_time = datetime(2024, 1, 15, 9, 12, 0)
    with freeze_time(current_time):
        prerequisite = await service.initialize_prerequisite(offline_store_feature_table_1.id)
    assert prerequisite is not None
    prerequisite_dict = prerequisite.model_dump(
        include={"offline_store_feature_table_id", "scheduled_job_ts", "completed"}, by_alias=True
    )
    assert prerequisite_dict == {
        "offline_store_feature_table_id": offline_store_feature_table_1.id,
        "scheduled_job_ts": datetime(2024, 1, 15, 9, 10, 0),
        "completed": [],
    }


@freeze_time(datetime(2024, 1, 15, 9, 12, 0))
@pytest.mark.asyncio
async def test_update_tile_prerequisite(
    service,
    feature_materialize_prerequisite_service,
    offline_store_feature_table_1,
):
    """
    Test update_tile_prerequisite
    """
    # Initialize prerequisite
    prerequisite = await service.initialize_prerequisite(offline_store_feature_table_1.id)
    assert prerequisite is not None

    # Simulate tile task completion update
    await service.update_tile_prerequisite(
        tile_task_ts=datetime(2024, 1, 15, 9, 20, 0),
        aggregation_ids=["agg_id_x"],
        status="success",
    )

    # Check prerequisite updated
    prerequisite = await feature_materialize_prerequisite_service.get_document(prerequisite.id)
    assert prerequisite.completed == [
        PrerequisiteTileTask(
            aggregation_id="agg_id_x",
            status="success",
        )
    ]


@freeze_time(datetime(2024, 1, 15, 9, 12, 0))
@pytest.mark.asyncio
async def test_update_tile_prerequisite_multiple_tables(
    service,
    feature_materialize_prerequisite_service,
    offline_store_feature_table_1,
    offline_store_feature_table_2,
):
    """
    Test update_tile_prerequisite when multiple offline store feature tables are involved
    """
    # Initialize prerequisite
    prerequisite_1 = await service.initialize_prerequisite(offline_store_feature_table_1.id)
    prerequisite_2 = await service.initialize_prerequisite(offline_store_feature_table_2.id)
    assert prerequisite_1 is not None
    assert prerequisite_2 is not None

    # Simulate tile task completion update
    await service.update_tile_prerequisite(
        tile_task_ts=datetime(2024, 1, 15, 9, 20, 0),
        aggregation_ids=["agg_id_x"],
        status="success",
    )
    await service.update_tile_prerequisite(
        tile_task_ts=datetime(2024, 1, 15, 9, 25, 0),
        aggregation_ids=["agg_id_y"],
        status="failure",
    )

    # Check prerequisite updated
    prerequisite_1 = await feature_materialize_prerequisite_service.get_document(prerequisite_1.id)
    assert prerequisite_1.completed == [
        PrerequisiteTileTask(
            aggregation_id="agg_id_x",
            status="success",
        )
    ]
    prerequisite_2 = await feature_materialize_prerequisite_service.get_document(prerequisite_2.id)
    assert prerequisite_2.completed == [
        PrerequisiteTileTask(
            aggregation_id="agg_id_x",
            status="success",
        ),
        PrerequisiteTileTask(
            aggregation_id="agg_id_y",
            status="failure",
        ),
    ]


@freeze_time(datetime(2024, 1, 15, 9, 12, 0), tick=True)
@pytest.mark.asyncio
async def test_run_feature_materialize__no_prerequisites(
    app_container,
    service,
    feature_materialize_prerequisite_service,
    mock_task_manager_submit,
    offline_store_feature_table_no_aggregation_ids,
):
    """
    Test run_feature_materialize when the feature table has no prerequisites
    """
    await service.run_feature_materialize(offline_store_feature_table_no_aggregation_ids.id)

    # No prerequisite document should be created
    docs = await feature_materialize_prerequisite_service.list_documents_as_dict(query_filter={})
    assert len(docs["data"]) == 0

    # Check materialize task is triggered
    assert len(mock_task_manager_submit.call_args_list) == 1
    submitted_payload = mock_task_manager_submit.call_args[0][0].model_dump()
    assert {
        "command": "SCHEDULED_FEATURE_MATERIALIZE",
        "offline_store_feature_table_name": offline_store_feature_table_no_aggregation_ids.name,
        "offline_store_feature_table_id": offline_store_feature_table_no_aggregation_ids.id,
    }.items() <= submitted_payload.items()

    submit_kwargs = mock_task_manager_submit.call_args[1]
    assert submit_kwargs["mark_as_scheduled_task"] is True

    # Check that the feature materialize run document is created and passed to task
    feature_materialize_run_id = submitted_payload["feature_materialize_run_id"]
    run_model = await app_container.feature_materialize_run_service.get_document(
        feature_materialize_run_id
    )
    assert {
        "offline_store_feature_table_id": offline_store_feature_table_no_aggregation_ids.id,
        "offline_store_feature_table_name": "cust_id_1d",
        "scheduled_job_ts": datetime(2024, 1, 15, 0, 0, 0),
        "incomplete_tile_tasks": None,
    }.items() <= run_model.model_dump().items()


@freeze_time(datetime(2024, 1, 15, 9, 12, 0), tick=True)
@pytest.mark.asyncio
async def test_run_feature_materialize__prerequisite_met(
    app_container,
    service,
    feature_materialize_prerequisite_service,
    mock_task_manager_submit,
    offline_store_feature_table_1,
    caplog,
):
    """
    Test run_feature_materialize
    """

    async def simulate_task_completion(service):
        """
        Simulate tile task completion
        """
        await asyncio.sleep(1)
        await service.update_tile_prerequisite(
            tile_task_ts=datetime(2024, 1, 15, 9, 20, 0),
            aggregation_ids=["agg_id_x"],
            status="success",
        )

    thread = Thread(target=asyncio.run, args=(simulate_task_completion(service),))
    thread.start()
    await service.run_feature_materialize(offline_store_feature_table_1.id)

    # Check prerequisite document
    docs = await feature_materialize_prerequisite_service.list_documents_as_dict(query_filter={})
    assert len(docs["data"]) == 1
    assert docs["data"][0]["completed"] == [{"aggregation_id": "agg_id_x", "status": "success"}]

    # Check materialize task is triggered
    assert caplog.records[-1].msg == "Prerequisites for feature materialize task met"
    assert len(mock_task_manager_submit.call_args_list) == 1
    submitted_payload = mock_task_manager_submit.call_args[0][0].model_dump()
    assert {
        "command": "SCHEDULED_FEATURE_MATERIALIZE",
        "offline_store_feature_table_name": offline_store_feature_table_1.name,
        "offline_store_feature_table_id": offline_store_feature_table_1.id,
    }.items() <= submitted_payload.items()

    submit_kwargs = mock_task_manager_submit.call_args[1]
    assert submit_kwargs["mark_as_scheduled_task"] is True

    # Check that the feature materialize run document is created and passed to task
    feature_materialize_run_id = submitted_payload["feature_materialize_run_id"]
    run_model = await app_container.feature_materialize_run_service.get_document(
        feature_materialize_run_id
    )
    assert {
        "offline_store_feature_table_id": offline_store_feature_table_1.id,
        "offline_store_feature_table_name": "cust_id_1h",
        "scheduled_job_ts": datetime(2024, 1, 15, 9, 10, 0),
        "incomplete_tile_tasks": None,
    }.items() <= run_model.model_dump().items()


@freeze_time(datetime(2024, 1, 15, 9, 12, 0), tick=True)
@pytest.mark.asyncio
async def test_run_feature_materialize__timeout(
    app_container,
    service,
    feature_materialize_prerequisite_service,
    mock_task_manager_submit,
    offline_store_feature_table_1,
    caplog,
):
    """
    Test run_feature_materialize when waiting for prequisites times out
    """
    with patch(
        "featurebyte.service.feature_materialize_sync.get_allowed_waiting_time_seconds"
    ) as patched_get_allowed_waiting_time:
        patched_get_allowed_waiting_time.return_value = 1
        await service.run_feature_materialize(offline_store_feature_table_1.id)

    # Check prerequisite document
    docs = await feature_materialize_prerequisite_service.list_documents_as_dict(query_filter={})
    assert len(docs["data"]) == 1
    assert len(docs["data"][0]["completed"]) == 0

    # Check materialize task is triggered with a warning
    assert (
        caplog.records[-1].msg == "Running feature materialize task but prerequisites are not met"
    )
    assert len(mock_task_manager_submit.call_args_list) == 1
    submitted_payload = mock_task_manager_submit.call_args[0][0].model_dump()
    assert {
        "command": "SCHEDULED_FEATURE_MATERIALIZE",
        "offline_store_feature_table_name": offline_store_feature_table_1.name,
        "offline_store_feature_table_id": offline_store_feature_table_1.id,
    }.items() <= submitted_payload.items()

    submit_kwargs = mock_task_manager_submit.call_args[1]
    assert submit_kwargs["mark_as_scheduled_task"] is True

    # Check that the feature materialize run document is created and passed to task
    feature_materialize_run_id = submitted_payload["feature_materialize_run_id"]
    run_model = await app_container.feature_materialize_run_service.get_document(
        feature_materialize_run_id
    )
    assert {
        "offline_store_feature_table_id": offline_store_feature_table_1.id,
        "offline_store_feature_table_name": "cust_id_1h",
        "scheduled_job_ts": datetime(2024, 1, 15, 9, 10, 0),
        "incomplete_tile_tasks": [{"aggregation_id": "agg_id_x", "reason": "timeout"}],
    }.items() <= run_model.model_dump().items()
