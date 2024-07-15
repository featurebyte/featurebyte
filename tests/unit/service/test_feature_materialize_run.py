"""
Unit tests for FeatureMaterializeRunService
"""

from datetime import datetime, timedelta

import pytest
from bson import ObjectId

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
    return datetime.utcnow().replace(microsecond=0)


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
async def test_completion_ts(service, feature_materialize_run_model, completion_ts):
    """
    Test update_completion_ts
    """
    await service.create_document(feature_materialize_run_model)

    await service.update_completion_ts(
        feature_materialize_run_model.id, completion_ts=completion_ts
    )

    # Check document is correctly updated
    retrieved_model = await service.get_document(feature_materialize_run_model.id)
    assert retrieved_model.completion_ts == completion_ts
    assert retrieved_model.duration_from_scheduled_seconds == 10.0
