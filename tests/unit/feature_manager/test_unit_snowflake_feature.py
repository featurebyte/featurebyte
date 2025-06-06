"""
This module contains unit tests for FeatureManagerSnowflake
"""

from unittest import mock

import pandas as pd
import pytest

from featurebyte.common.model_util import get_version
from featurebyte.feature_manager.model import ExtendedFeatureModel
from tests.unit.service.test_feature_manager import feature_manager_deployment_setup


@pytest.fixture(name="mock_snowflake_feature")
def mock_snowflake_feature_fixture(mock_snowflake_feature):
    """
    ExtendedFeatureModel object fixture
    """
    mock_snowflake_feature.save()
    feature_model = mock_snowflake_feature.cached_model
    return ExtendedFeatureModel(
        **feature_model.model_dump(exclude={"version": True}, by_alias=True),
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
    return mock_snowflake_feature


@mock.patch("featurebyte.service.tile_manager.TileManagerService.schedule_online_tiles")
@mock.patch("featurebyte.service.tile_manager.TileManagerService.schedule_offline_tiles")
@mock.patch("featurebyte.service.tile_manager.TileManagerService.generate_tiles")
@pytest.mark.asyncio
async def test_online_enable(
    mock_generate_tiles,
    mock_schedule_offline_tiles,
    mock_schedule_online_tiles,
    app_container,
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
            "featurebyte.service.deployed_tile_table.DeployedTileTableService.update_backfill_metadata"
        ):
            mock_tile_job_exists.return_value = False
            await feature_manager_deployment_setup(app_container, mock_snowflake_feature)
            await feature_manager_service.online_enable(mock_snowflake_session, feature_spec)

    mock_schedule_online_tiles.assert_called_once()
    mock_schedule_offline_tiles.assert_called_once()
    mock_generate_tiles.assert_called_once()


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
    app_container,
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
            mock_tile_job_exists.return_value = True
            await feature_manager_deployment_setup(app_container, mock_snowflake_feature)
            await feature_manager_service.online_enable(mock_snowflake_session, feature_spec)

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
    mock_snowflake_session.execute_query.side_effect = [None, None, None, None]
    with mock.patch(
        "featurebyte.service.tile_manager.TileManagerService.remove_legacy_tile_jobs"
    ) as mock_tile_manager:
        mock_tile_manager.side_effect = None
        with mock.patch(
            "featurebyte.service.online_store_compute_query_service.OnlineStoreComputeQueryService.delete_by_result_name"
        ) as mock_delete_by_result_name:
            await feature_manager_service.online_disable(
                mock_snowflake_session, mock_snowflake_feature
            )

    mock_delete_by_result_name.assert_called_once()
