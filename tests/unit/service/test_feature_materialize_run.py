"""
Unit tests for FeatureMaterializeRunService
"""

from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from bson import ObjectId
from freezegun import freeze_time

from featurebyte.models.feature_materialize_run import FeatureMaterializeRun, IncompleteTileTask


@pytest.fixture
def service(app_container):
    """
    Fixture for a FeatureMaterializePrerequisiteService
    """
    return app_container.feature_materialize_run_service


@pytest.fixture
def offline_store_feature_table_id():
    """
    Fixture for offline store feature table id
    """
    return ObjectId()


@pytest.fixture
def scheduled_job_ts():
    """
    Fixture for a scheduled job timestamp
    """
    return datetime(2024, 7, 15, 0, 0, 0)


@pytest.fixture
def completion_ts(scheduled_job_ts):
    """
    Fixture for a completion timestamp
    """
    return scheduled_job_ts + timedelta(seconds=10)


@pytest.fixture
def feature_materialize_run_model(offline_store_feature_table_id, scheduled_job_ts):
    """
    Fixture for a FeatureMaterializeRun
    """
    return FeatureMaterializeRun(
        offline_store_feature_table_id=offline_store_feature_table_id,
        scheduled_job_ts=scheduled_job_ts,
    )


@pytest.fixture
def deployment_id():
    """
    Fixture for a deployment id
    """
    return ObjectId()


@pytest.fixture
def another_deployment_id():
    """
    Fixture for another deployment id
    """
    return ObjectId()


@pytest_asyncio.fixture
async def saved_feature_materialize_run_models(
    service, offline_store_feature_table_id, scheduled_job_ts, deployment_id, another_deployment_id
):
    """
    Fixture for a list of saved FeatureMaterializeRun models
    """
    models = []
    for current_deployment_id in [deployment_id, another_deployment_id]:
        for i in range(10):
            current_scheduled_job_ts = scheduled_job_ts + i * timedelta(hours=1)
            model = FeatureMaterializeRun(
                offline_store_feature_table_id=offline_store_feature_table_id,
                scheduled_job_ts=current_scheduled_job_ts,
                completion_ts=current_scheduled_job_ts + timedelta(seconds=10),
                completion_status="success",
                duration_from_scheduled_seconds=10,
                deployment_ids=[current_deployment_id],
            )
            models.append(await service.create_document(model))
    yield models


@pytest.mark.asyncio
async def test_update_incomplete_tile_tasks(service, feature_materialize_run_model):
    """
    Test update_incomplete_tile_tasks method
    """
    await service.create_document(feature_materialize_run_model)

    incomplete_tile_tasks = [
        IncompleteTileTask(
            aggregation_id="agg_id_1",
            reason="failure",
        ),
        IncompleteTileTask(
            aggregation_id="agg_id_2",
            reason="timeout",
        ),
    ]
    await service.update_incomplete_tile_tasks(
        feature_materialize_run_model.id, incomplete_tile_tasks
    )

    # Check document is correctly updated
    retrieved_model = await service.get_document(feature_materialize_run_model.id)
    assert retrieved_model.incomplete_tile_tasks == incomplete_tile_tasks


@pytest.mark.asyncio
async def test_feature_materialize_ts(service, feature_materialize_run_model):
    """
    Test update_feature_materialize_ts
    """
    await service.create_document(feature_materialize_run_model)

    feature_materialize_ts = datetime(2024, 1, 15, 10, 0, 0)
    await service.update_feature_materialize_ts(
        feature_materialize_run_model.id, feature_materialize_ts
    )

    # Check document is correctly updated
    retrieved_model = await service.get_document(feature_materialize_run_model.id)
    assert retrieved_model.feature_materialize_ts == feature_materialize_ts


@pytest.mark.asyncio
async def test_set_completion(service, feature_materialize_run_model, completion_ts):
    """
    Test set_completion
    """
    await service.create_document(feature_materialize_run_model)

    await service.set_completion(
        feature_materialize_run_model.id, completion_ts=completion_ts, completion_status="success"
    )

    # Check document is correctly updated
    retrieved_model = await service.get_document(feature_materialize_run_model.id)
    assert retrieved_model.completion_ts == completion_ts
    assert retrieved_model.completion_status == "success"
    assert retrieved_model.duration_from_scheduled_seconds == 10.0


@pytest.mark.usefixtures("saved_feature_materialize_run_models")
@pytest.mark.asyncio
async def test_get_recent_runs_by_deployment_id(service, deployment_id):
    """
    Test get_recent_runs_by_deployment_id
    """
    runs = await service.get_recent_runs_by_deployment_id(deployment_id, num_runs=3)
    assert len(runs) == 3
    for run in runs:
        assert run.deployment_ids == [deployment_id]
    runs_scheduled_ts = [run.scheduled_job_ts for run in runs]
    assert runs_scheduled_ts == [
        datetime(2024, 7, 15, 9, 0, 0),
        datetime(2024, 7, 15, 8, 0, 0),
        datetime(2024, 7, 15, 7, 0, 0),
    ]
