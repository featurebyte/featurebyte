"""
Tests for TileManager

Some tests are in tests/unit/tile/test_unit_snowflake_tile.py and should be cleaned up or moved
here.
"""

import pytest
from bson import ObjectId


@pytest.fixture(name="service")
def service_fixture(app_container):
    """
    TileManagerService object fixture
    """
    return app_container.tile_manager_service


@pytest.fixture(name="periodic_task_service")
def periodic_task_service_fixture(app_container):
    """
    PeriodicTaskService object
    """
    return app_container.periodic_task_service


@pytest.fixture(name="deployed_tile_table_id")
def deployed_tile_table_id_fixture():
    """
    Fixture for deployed tile table ID
    """
    return ObjectId()


@pytest.mark.asyncio
async def test_schedule_tile_job__deployed_tile_table(
    service,
    periodic_task_service,
    mock_snowflake_tile,
    snowflake_feature_store_id,
    deployed_tile_table_id,
):
    """
    Test schedule_online_tiles method in TileManagerService
    """
    await service.schedule_online_tiles(
        tile_spec=mock_snowflake_tile,
        deployed_tile_table_id=deployed_tile_table_id,
    )

    # Check if the periodic task was created correctly
    periodic_tasks = await periodic_task_service.list_documents(query_filter={})
    assert len(periodic_tasks) == 1
    periodic_task = periodic_tasks[0]
    assert periodic_task.kwargs == {
        "task_type": "io_task",
        "priority": 0,
        "output_document_id": periodic_task.kwargs["output_document_id"],
        "is_scheduled_task": True,
        "user_id": "63f9506dd478b94127123456",
        "catalog_id": "646f6c1c0ed28a5271fb02db",
        "feature_store_id": str(snowflake_feature_store_id),
        "parameters": {
            "feature_store_id": str(snowflake_feature_store_id),
            "tile_id": "TILE_ID1",
            "aggregation_id": "agg_id1",
            "deployed_tile_table_id": str(deployed_tile_table_id),
            "time_modulo_frequency_second": 183,
            "blind_spot_second": 3,
            "frequency_minute": 5,
            "sql": "select c1 from dummy where tile_start_ts >= __FB_START_DATE and tile_start_ts < __FB_END_DATE",
            "tile_compute_query": None,
            "entity_column_names": ["col1"],
            "value_column_names": ["col2"],
            "value_column_types": ["FLOAT"],
            "offline_period_minute": 1440,
            "tile_type": "ONLINE",
            "monitor_periods": 10,
            "job_schedule_ts": None,
        },
        "command": "TILE_COMPUTE",
        "output_collection_name": None,
        "is_revocable": False,
        "is_rerunnable": False,
    }

    # Test removal of deployed tile table job
    await service.remove_deployed_tile_table_job(deployed_tile_table_id)
    periodic_tasks = await periodic_task_service.list_documents(query_filter={})
    assert len(periodic_tasks) == 0


@pytest.mark.asyncio
async def test_schedule_tile_job__legacy(
    service,
    periodic_task_service,
    mock_snowflake_tile,
    snowflake_feature_store_id,
):
    """
    Test schedule_online_tiles method in TileManagerService
    """
    await service.schedule_online_tiles(
        tile_spec=mock_snowflake_tile,
        deployed_tile_table_id=None,
    )

    # Check if the periodic task was created correctly
    periodic_tasks = await periodic_task_service.list_documents(query_filter={})
    assert len(periodic_tasks) == 1
    periodic_task = periodic_tasks[0]
    assert periodic_task.kwargs == {
        "task_type": "io_task",
        "priority": 0,
        "output_document_id": periodic_task.kwargs["output_document_id"],
        "is_scheduled_task": True,
        "user_id": "63f9506dd478b94127123456",
        "catalog_id": "646f6c1c0ed28a5271fb02db",
        "feature_store_id": str(snowflake_feature_store_id),
        "parameters": {
            "feature_store_id": str(snowflake_feature_store_id),
            "tile_id": "TILE_ID1",
            "aggregation_id": "agg_id1",
            "deployed_tile_table_id": None,
            "time_modulo_frequency_second": 183,
            "blind_spot_second": 3,
            "frequency_minute": 5,
            "sql": "select c1 from dummy where tile_start_ts >= __FB_START_DATE and tile_start_ts < __FB_END_DATE",
            "tile_compute_query": None,
            "entity_column_names": ["col1"],
            "value_column_names": ["col2"],
            "value_column_types": ["FLOAT"],
            "offline_period_minute": 1440,
            "tile_type": "ONLINE",
            "monitor_periods": 10,
            "job_schedule_ts": None,
        },
        "command": "TILE_COMPUTE",
        "output_collection_name": None,
        "is_revocable": False,
        "is_rerunnable": False,
    }

    # Test removal of deployed tile table job
    await service.remove_legacy_tile_jobs(mock_snowflake_tile.aggregation_id)
    periodic_tasks = await periodic_task_service.list_documents(query_filter={})
    assert len(periodic_tasks) == 0
