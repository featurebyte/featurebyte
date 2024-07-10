"""
Unit tests for FeatureMaterializeSyncService
"""

from __future__ import annotations

from datetime import datetime

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


@pytest.mark.asyncio
async def test_initialize_prerequisite(service, offline_store_feature_table_1):
    """
    Test initializing and retrieving prerequisite
    """
    current_time = datetime(2024, 1, 15, 9, 12, 0)
    with freeze_time(current_time):
        prerequisite = await service.initialize_prerequisite(offline_store_feature_table_1.id)
    assert prerequisite is not None
    prerequisite_dict = prerequisite.dict(
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
        aggregation_id="agg_id_x",
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
        aggregation_id="agg_id_x",
        status="success",
    )
    await service.update_tile_prerequisite(
        tile_task_ts=datetime(2024, 1, 15, 9, 25, 0),
        aggregation_id="agg_id_y",
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
