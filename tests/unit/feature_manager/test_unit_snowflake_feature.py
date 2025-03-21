"""
This module contains unit tests for FeatureManagerSnowflake
"""

from unittest import mock
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from featurebyte.common.model_util import get_version
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.online_store_spec import OnlineFeatureSpec


@pytest.fixture(name="mock_snowflake_feature")
def mock_snowflake_feature_fixture(mock_snowflake_feature):
    """
    ExtendedFeatureModel object fixture
    """
    return ExtendedFeatureModel(
        **mock_snowflake_feature.model_dump(exclude={"version": True}),
        version=get_version(),
    )


@pytest.fixture(name="feature_manager_service")
def feature_manager_service_fixture(app_container):
    """
    FeatureManagerService object fixture
    """
    return app_container.feature_manager_service


@pytest.fixture(name="feature_spec")
def feature_spec_fixture(mock_snowflake_feature):
    """
    OnlineFeatureSpec object fixture
    """
    feature_spec = OnlineFeatureSpec(
        feature=mock_snowflake_feature,
        feature_sql="select * from temp",
        feature_store_table_name="feature_store_table_1",
    )
    with mock.patch(
        "featurebyte.service.feature_manager.FeatureManagerService._get_unscheduled_aggregation_result_names",
        AsyncMock(return_value=[feature_spec.precompute_queries[0].result_name]),
    ):
        yield feature_spec


@pytest.fixture(name="feature_spec_with_scheduled_aggregations")
def feature_spec_with_scheduled_aggregations_fixture(mock_snowflake_feature):
    """
    OnlineFeatureSpec object fixture
    """
    feature_spec = OnlineFeatureSpec(
        feature=mock_snowflake_feature,
        feature_sql="select * from temp",
        feature_store_table_name="feature_store_table_1",
    )
    with mock.patch(
        "featurebyte.service.feature_manager.FeatureManagerService._get_unscheduled_aggregation_result_names",
        AsyncMock(return_value=[]),
    ):
        yield feature_spec


@mock.patch("featurebyte.service.tile_manager.TileManagerService.schedule_online_tiles")
@mock.patch("featurebyte.service.tile_manager.TileManagerService.schedule_offline_tiles")
@mock.patch("featurebyte.service.tile_manager.TileManagerService.generate_tiles")
@pytest.mark.asyncio
async def test_online_enable(
    mock_generate_tiles,
    mock_schedule_offline_tiles,
    mock_schedule_online_tiles,
    mock_snowflake_feature,
    feature_spec,
    feature_manager_service,
    mock_snowflake_session,
):
    """
    Test online_enable
    """
    mock_snowflake_session.execute_query.return_value = []

    with mock.patch(
        "featurebyte.service.tile_manager.TileManagerService.tile_job_exists"
    ) as mock_tile_job_exists:
        with mock.patch(
            "featurebyte.service.tile_registry_service.TileRegistryService.update_backfill_metadata"
        ):
            mock_tile_job_exists.return_value = False
            await feature_manager_service.online_enable(mock_snowflake_session, feature_spec)

    mock_schedule_online_tiles.assert_called_once()
    mock_schedule_offline_tiles.assert_called_once()
    mock_generate_tiles.assert_called_once()

    # Expected execute_query calls are triggered by TileScheduleOnlineStore:

    # 1. Check if online store table exists (execute_query)
    args, _ = mock_snowflake_session.execute_query_long_running.call_args_list[0]
    assert args[0].strip().startswith('SELECT\n  *\nFROM "ONLINE_STORE_')

    # 2. Compute online store values and store in a temporary table
    args, _ = mock_snowflake_session.execute_query_long_running.call_args_list[1]
    assert args[0].strip().startswith('CREATE TABLE "__SESSION_TEMP_TABLE_')

    # 3. Insert into online store table (execute_query_long_running)
    args, _ = mock_snowflake_session.execute_query_long_running.call_args_list[2]
    assert args[0].strip().startswith("INSERT INTO ONLINE_STORE_")


@mock.patch("featurebyte.service.tile_manager.TileManagerService.schedule_online_tiles")
@mock.patch("featurebyte.service.tile_manager.TileManagerService.schedule_offline_tiles")
@pytest.mark.asyncio
async def test_online_enable_duplicate_tile_task(
    mock_schedule_offline_tiles,
    mock_schedule_online_tiles,
    mock_snowflake_feature,
    mock_snowflake_session,
    feature_spec,
    feature_manager_service,
):
    """
    Test online_enable
    """
    _ = mock_schedule_offline_tiles
    _ = mock_schedule_online_tiles

    mock_snowflake_session.execute_query.side_effect = [
        None,
        None,
        pd.DataFrame.from_dict({"name": ["task_1"]}),
        None,
        None,
        None,
    ]
    with mock.patch(
        "featurebyte.service.tile_manager.TileManagerService.tile_job_exists"
    ) as mock_tile_job_exists:
        with mock.patch(
            "featurebyte.service.feature_manager.FeatureManagerService._backfill_tiles"
        ) as mock_generate_historical_tiles:
            with mock.patch(
                "featurebyte.service.feature_manager.FeatureManagerService._populate_feature_store"
            ) as _:
                mock_tile_job_exists.return_value = True
                await feature_manager_service.online_enable(mock_snowflake_session, feature_spec)

    mock_schedule_online_tiles.assert_not_called()
    mock_schedule_offline_tiles.assert_not_called()
    mock_generate_historical_tiles.assert_called()


@mock.patch("featurebyte.service.tile_manager.TileManagerService.schedule_online_tiles")
@mock.patch("featurebyte.service.tile_manager.TileManagerService.schedule_offline_tiles")
@pytest.mark.asyncio
async def test_online_enable_aggregation_results_already_scheduled(
    mock_schedule_offline_tiles,
    mock_schedule_online_tiles,
    mock_snowflake_feature,
    mock_snowflake_session,
    feature_spec_with_scheduled_aggregations,
    feature_manager_service,
):
    """
    Test online_enable
    """
    _ = mock_schedule_offline_tiles
    _ = mock_schedule_online_tiles

    mock_snowflake_session.execute_query.side_effect = [
        None,
        None,
        pd.DataFrame.from_dict({"name": ["task_1"]}),
        None,
        None,
        None,
    ]
    with mock.patch(
        "featurebyte.service.tile_manager.TileManagerService.tile_job_exists"
    ) as mock_tile_job_exists:
        with mock.patch(
            "featurebyte.service.feature_manager.FeatureManagerService._backfill_tiles"
        ) as mock_generate_historical_tiles:
            with mock.patch(
                "featurebyte.service.feature_manager.FeatureManagerService._populate_feature_store"
            ) as _:
                mock_tile_job_exists.return_value = True
                await feature_manager_service.online_enable(
                    mock_snowflake_session, feature_spec_with_scheduled_aggregations
                )

    mock_schedule_online_tiles.assert_not_called()
    mock_schedule_offline_tiles.assert_not_called()
    mock_generate_historical_tiles.assert_called()


@pytest.mark.asyncio
async def test_online_disable(
    mock_snowflake_feature,
    mock_snowflake_session,
    feature_manager_service,
):
    """
    Test online_enable
    """

    feature_spec = OnlineFeatureSpec(
        feature=mock_snowflake_feature,
        feature_sql="select * from temp",
        feature_store_table_name="feature_store_table_1",
    )

    mock_snowflake_session.execute_query.side_effect = [None, None, None, None]
    with mock.patch(
        "featurebyte.service.tile_manager.TileManagerService.remove_tile_jobs"
    ) as mock_tile_manager:
        mock_tile_manager.side_effect = None
        with mock.patch(
            "featurebyte.service.online_store_compute_query_service.OnlineStoreComputeQueryService.delete_by_result_name"
        ) as mock_delete_by_result_name:
            await feature_manager_service.online_disable(mock_snowflake_session, feature_spec)

    mock_delete_by_result_name.assert_called_once()
