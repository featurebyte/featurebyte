"""
Unit tests for FeatureMaterializePrerequisiteService
"""

from datetime import datetime

import pytest
from bson import ObjectId

from featurebyte.models.feature_materialize_prerequisite import (
    FeatureMaterializePrerequisite,
    PrerequisiteTileTask,
)


@pytest.fixture
def service(app_container):
    """
    Fixture for a FeatureMaterializePrerequisiteService
    """
    return app_container.feature_materialize_prerequisite_service


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
    return datetime.utcnow()


@pytest.fixture
def feature_materialize_prerequisite_model(offline_store_feature_table_id, scheduled_job_ts):
    """
    Fixture for a FeatureMaterializePrerequisite
    """
    return FeatureMaterializePrerequisite(
        offline_store_feature_table_id=offline_store_feature_table_id,
        scheduled_job_ts=scheduled_job_ts,
    )


@pytest.mark.asyncio
async def test_save_and_retrieve(
    service,
    feature_materialize_prerequisite_model,
    offline_store_feature_table_id,
    scheduled_job_ts,
):
    """
    Test saving and retrieving a FeatureMaterializePrerequisite document
    """
    await service.create_document(feature_materialize_prerequisite_model)

    retrieved_model = await service.get_document_for_feature_table(
        offline_store_feature_table_id, scheduled_job_ts
    )
    assert retrieved_model.id == feature_materialize_prerequisite_model.id


@pytest.mark.asyncio
async def test_add_completed_task(
    service,
    feature_materialize_prerequisite_model,
    offline_store_feature_table_id,
    scheduled_job_ts,
):
    """
    Test updating a FeatureMaterializePrerequisite document by adding completed tasks
    """
    await service.create_document(feature_materialize_prerequisite_model)
    retrieved_model = await service.get_document_for_feature_table(
        offline_store_feature_table_id, scheduled_job_ts
    )
    assert retrieved_model.completed == []

    # Add task item 1
    tile_task_1 = PrerequisiteTileTask(
        aggregation_id="agg_id_1",
        status="success",
    )
    await service.add_completed_prerequisite(
        offline_store_feature_table_id, scheduled_job_ts, [tile_task_1]
    )

    # Add task item 2
    tile_task_2 = PrerequisiteTileTask(
        aggregation_id="agg_id_2",
        status="failure",
    )
    await service.add_completed_prerequisite(
        offline_store_feature_table_id, scheduled_job_ts, [tile_task_2]
    )

    # Check document is correctly updated
    retrieved_model = await service.get_document_for_feature_table(
        offline_store_feature_table_id, scheduled_job_ts
    )
    assert retrieved_model.completed == [tile_task_1, tile_task_2]


@pytest.mark.asyncio
async def test_add_completed_task_not_found(
    service,
    offline_store_feature_table_id,
    scheduled_job_ts,
):
    """
    Test updating a FeatureMaterializePrerequisite document by adding completed tasks before the
    prerequisite document is created.
    """
    tile_task_1 = PrerequisiteTileTask(
        aggregation_id="agg_id_1",
        status="success",
    )
    await service.add_completed_prerequisite(
        offline_store_feature_table_id, scheduled_job_ts, [tile_task_1]
    )
    docs = await service.list_documents_as_dict()
    assert len(docs["data"]) == 1
    assert docs["data"][0]["completed"] == [{"aggregation_id": "agg_id_1", "status": "success"}]
