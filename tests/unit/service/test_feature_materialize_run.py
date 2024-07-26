"""
Unit tests for FeatureMaterializeRunService
"""
# ruff: noqa: F405

from featurebyte.models.feature_materialize_run import IncompleteTileTask
from tests.unit.service.fixtures_feature_materialize_runs import *  # noqa


@pytest.fixture
def service(app_container):
    """
    Fixture for a FeatureMaterializePrerequisiteService
    """
    return app_container.feature_materialize_run_service


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
