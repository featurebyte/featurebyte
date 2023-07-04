"""
Unit tests for TileTaskExecutor
"""
from unittest.mock import Mock, patch

import pytest

from featurebyte import SourceType
from featurebyte.models.tile import TileScheduledJobParameters
from featurebyte.session.snowflake import SnowflakeSession


@pytest.fixture(name="session")
def session_fixture():
    """
    Fixture for the db session object
    """
    return Mock(
        name="mock_snowflake_session",
        spec=SnowflakeSession,
        source_type=SourceType.SNOWFLAKE,
    )


@pytest.fixture(name="tile_task_parameters")
def tile_task_parameters_fixture() -> TileScheduledJobParameters:
    """
    Fixture for TileScheduledJobParameters
    """
    return TileScheduledJobParameters(
        tile_id="some_tile_id",
        aggregation_id="some_agg_id",
        time_modulo_frequency_second=10,
        blind_spot_second=30,
        frequency_minute=5,
        sql="SELECT * FROM some_table",
        entity_column_names=["cust_id"],
        value_column_names=["sum_value"],
        value_column_types=["FLOAT"],
        offline_period_minute=1440,
        tile_type="online",
        monitor_periods=10,
    )


@pytest.fixture(name="patched_tile_classes")
def patched_tile_classes_fixture():
    """
    Fixture to patch TileMonitor, TileGenerate and TileScheduleOnlineStore used in TileTaskExecutor
    """
    mocks = {}
    patchers = []
    classes_to_patch = ["TileMonitor", "TileGenerate", "TileScheduleOnlineStore"]
    for class_name in classes_to_patch:
        patcher = patch(f"featurebyte.service.tile.tile_task_executor.{class_name}", autospec=True)
        mocks[class_name] = patcher.start()
        patchers.append(patcher)
    yield mocks
    for patcher in patchers:
        patcher.stop()


@pytest.mark.asyncio
async def test_online_store_job_schedule_ts(
    tile_task_executor, tile_task_parameters, session, patched_tile_classes
):
    """
    Test that the job_schedule_ts_str parameter passed to TileScheduleOnlineStore is correct
    """
    # The scheduled task is run 1 second past the intended schedule
    tile_task_parameters.job_schedule_ts = "2023-01-15 10:00:11"
    await tile_task_executor.execute(session, tile_task_parameters)

    # The online store calculation should use the corrected schedule time as point in time
    _, kwargs = patched_tile_classes["TileScheduleOnlineStore"].call_args
    assert kwargs["job_schedule_ts_str"] == "2023-01-15 10:00:10"
