"""
This module contains integration tests for TileManager scheduler
"""
import importlib
import json
from datetime import datetime
from unittest import mock

import pytest
import pytest_asyncio
from apscheduler.triggers.cron import CronTrigger
from bson import ObjectId

from featurebyte import SourceType
from featurebyte.common import date_util
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.feature import FeatureReadiness
from featurebyte.models.tile import TileType
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.tile.celery.server import celery_instance
from featurebyte.tile.celery.tasks import execute_internal
from featurebyte.tile.scheduler import TileSchedulerFactory


@pytest.fixture(name="tile_scheduler")
def mock_tile_scheduler_fixture():
    tile_scheduler = TileSchedulerFactory.get_instance()
    yield tile_scheduler
    celery_instance.shutdown()


@pytest_asyncio.fixture(name="feature")
async def mock_feature_fixture(feature_model_dict, session, feature_store):
    """
    Fixture for a ExtendedFeatureModel object
    """

    # this fixture was written to work for snowflake only
    assert session.source_type == SourceType.SPARK

    feature_model_dict.update(
        {
            "user_id": ObjectId(),
            "tabular_source": {
                "feature_store_id": feature_store.id,
                "table_details": TableDetails(table_name="some_random_table"),
            },
            "version": "v1",
            "readiness": FeatureReadiness.DRAFT,
            "online_enabled": False,
            "tabular_data_ids": [
                ObjectId("626bccb9697a12204fb22ea3"),
                ObjectId("726bccb9697a12204fb22ea3"),
            ],
        }
    )
    feature = ExtendedFeatureModel(**feature_model_dict)

    yield feature

    tile_id = feature.tile_specs[0].tile_id
    await session.execute_query(f"DELETE FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'")
    celery_instance.shutdown()


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tiles_with_scheduler(tile_spec, session, tile_manager, tile_scheduler):
    """
    Test generate_tiles method in TileSnowflake
    """
    schedule_time = datetime.utcnow()
    next_job_time = date_util.get_next_job_datetime(
        input_dt=schedule_time,
        frequency_minutes=tile_spec.frequency_minute,
        time_modulo_frequency_seconds=tile_spec.time_modulo_frequency_second,
    )
    tile_spec.tile_sql = "SELECT * FROM TEMP_TABLE"
    tile_spec.aggregation_id = f"{tile_spec.tile_id}_{schedule_time.strftime('%Y%m%d%H%M%S')}"

    await tile_manager.schedule_online_tiles(tile_spec=tile_spec, schedule_time=schedule_time)
    job_id = f"{TileType.ONLINE}_{tile_spec.aggregation_id}"

    job_ids = tile_scheduler.get_jobs()
    assert job_id in job_ids

    job_details = tile_scheduler.get_job_details(job_id=job_id)

    # verifying scheduling trigger
    trigger = job_details.trigger
    assert isinstance(trigger, CronTrigger)
    scheduled = [int(str(f)) for f in trigger.fields if not f.is_default]
    expected = [f for f in next_job_time.timetuple()][:6]
    assert scheduled == expected

    # verifying the scheduling function
    module_path = job_details.func.__self__.args[0]
    class_name = job_details.func.__self__.args[1]
    instance_str = job_details.func.__self__.args[2]
    module = importlib.import_module(module_path)
    instance_class = getattr(module, class_name)
    instance_json = json.loads(instance_str)

    instance = instance_class(spark_session=session, **instance_json)
    await instance.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_spec.tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 100


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_tile_celery_worker_execute(
    session, feature, tile_spec, feature_store, tile_manager, tile_scheduler
):
    _ = tile_scheduler
    schedule_time = datetime.utcnow()
    tile_spec.tile_sql = "SELECT * FROM TEMP_TABLE"
    tile_spec.aggregation_id = f"{tile_spec.tile_id}_{schedule_time.strftime('%Y%m%d%H%M%S')}"

    instance_json = await tile_manager.schedule_online_tiles(
        tile_spec=tile_spec,
        schedule_time=schedule_time,
        user_id=feature.user_id,
        feature_store_id=feature.tabular_source.feature_store_id,
        workspace_id=feature.workspace_id,
    )
    module_path = "featurebyte.sql.spark.tile_generate_schedule"
    class_name = "TileGenerateSchedule"
    with mock.patch(
        "featurebyte.service.feature_store.FeatureStoreService.get_document"
    ) as mock_feature_store_service:
        mock_feature_store_service.return_value = feature_store
        await execute_internal(module_path, class_name, instance_json)

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_spec.tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 100
