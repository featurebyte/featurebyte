"""
This module contains unit tests for FeatureManagerSnowflake
"""
from unittest import mock
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from featurebyte import SourceType
from featurebyte.common.model_util import get_version
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.sql_template import (
    tm_delete_online_store_mapping,
    tm_feature_tile_monitor,
    tm_upsert_online_store_mapping,
)
from featurebyte.models.online_store import OnlineFeatureSpec
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.utils.snowflake.sql import escape_column_names


@pytest.fixture(name="mock_snowflake_feature")
def mock_snowflake_feature_fixture(mock_snowflake_feature):
    """
    ExtendedFeatureModel object fixture
    """
    return ExtendedFeatureModel(
        **mock_snowflake_feature.dict(exclude={"version": True}),
        version=get_version(),
    )


@pytest.fixture(name="feature_manager_service")
def feature_manager_service_fixture(app_container):
    """
    FeatureManagerService object fixture
    """
    return app_container.feature_manager_service


@pytest.fixture(name="mock_snowflake_session")
def mock_snowflake_session_fixture():
    """
    SnowflakeSession object fixture
    """
    return Mock(
        name="mock_snowflake_session",
        spec=SnowflakeSession,
        source_type=SourceType.SNOWFLAKE,
    )


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
        mock_tile_job_exists.return_value = False
        await feature_manager_service.online_enable(mock_snowflake_session, feature_spec)

    mock_schedule_online_tiles.assert_called_once()
    mock_schedule_offline_tiles.assert_called_once()
    mock_generate_tiles.assert_called_once()

    # Expected execute_query calls:
    # 1. merge into TILE_FEATURE_MAPPING
    # 2. merge into ONLINE_STORE_MAPPING
    # 3. call SP_TILES_SCHEDULE_ONLINE_STORE
    assert mock_snowflake_session.execute_query.call_count == 3

    upsert_sql = tm_upsert_tile_feature_mapping.render(
        tile_id=feature_spec.tile_ids[0],
        aggregation_id=feature_spec.aggregation_ids[0],
        feature_name=feature_spec.feature.name,
        feature_type=feature_spec.value_type,
        feature_version=feature_spec.feature.version.to_str(),
        feature_readiness=str(mock_snowflake_feature.readiness),
        feature_event_table_ids=",".join([str(i) for i in feature_spec.event_table_ids]),
        is_deleted=False,
    )
    assert mock_snowflake_session.execute_query.call_args_list[0] == mock.call(upsert_sql)

    queries = feature_spec.precompute_queries
    assert len(queries) == 1
    query = queries[0]
    upsert_sql = tm_upsert_online_store_mapping.render(
        tile_id=query.tile_id,
        aggregation_id=query.aggregation_id,
        result_id=query.result_name,
        result_type=query.result_type,
        sql_query=query.sql.replace("'", "''"),
        online_store_table_name=query.table_name,
        entity_column_names=",".join(escape_column_names(query.serving_names)),
        is_deleted=False,
    )
    assert mock_snowflake_session.execute_query.call_args_list[1] == mock.call(upsert_sql)


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
            "featurebyte.service.feature_manager.FeatureManagerService._generate_historical_tiles"
        ) as mock_generate_historical_tiles:
            with mock.patch(
                "featurebyte.service.feature_manager.FeatureManagerService._populate_feature_store"
            ) as _:
                mock_tile_job_exists.return_value = True
                await feature_manager_service.online_enable(mock_snowflake_session, feature_spec)

    mock_schedule_online_tiles.assert_not_called()
    mock_schedule_offline_tiles.assert_not_called()
    mock_generate_historical_tiles.assert_not_called()


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
            "featurebyte.service.feature_manager.FeatureManagerService._generate_historical_tiles"
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
    mock_generate_historical_tiles.assert_not_called()

    for execute_query_call in mock_snowflake_session.execute_query.call_args_list:
        args, _ = execute_query_call
        query = args[0]
        # Updating TILE_FEATURE_MAPPING is expected
        assert "TILE_FEATURE_MAPPING" in query
        # Updating ONLINE_STORE_MAPPING is not expected because the aggregation result is already
        # scheduled
        assert "ONLINE_STORE_MAPPING" not in query


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
        await feature_manager_service.online_disable(mock_snowflake_session, feature_spec)

    delete_sql = tm_delete_tile_feature_mapping.render(
        aggregation_id=feature_spec.aggregation_ids[0],
        feature_name=feature_spec.feature.name,
        feature_version=feature_spec.feature.version.to_str(),
    )
    assert mock_snowflake_session.execute_query.call_args_list[0] == mock.call(delete_sql)

    delete_sql = tm_delete_online_store_mapping.render(
        result_id=feature_spec.precompute_queries[0].result_name,
    )
    assert mock_snowflake_session.execute_query.call_args_list[1] == mock.call(delete_sql)


@pytest.mark.asyncio
async def test_retrieve_feature_tile_inconsistency_data(
    mock_snowflake_session, feature_manager_service
):
    """
    Test retrieve_feature_tile_inconsistency_data
    """
    mock_snowflake_session.execute_query.return_value = pd.DataFrame.from_dict(
        {
            "NAME": ["sum_30m", "sum_30m"],
            "VERSION": ["v1", "v1"],
            "TILE_START_DATE": ["2022-06-05 16:03:00", "2022-06-05 15:58:00"],
            "TILE_MONITOR_DATE": ["2022-06-05 16:03:00", "2022-06-05 15:58:00"],
        }
    )
    result = await feature_manager_service.retrieve_feature_tile_inconsistency_data(
        mock_snowflake_session,
        query_start_ts="2022-06-05 15:43:00",
        query_end_ts="2022-06-05 16:03:00",
    )
    assert len(result) == 2
    assert result.iloc[0]["TILE_START_DATE"] == "2022-06-05 16:03:00"
    assert result.iloc[1]["TILE_START_DATE"] == "2022-06-05 15:58:00"

    retrieve_sql = tm_feature_tile_monitor.render(
        query_start_ts="2022-06-05 15:43:00",
        query_end_ts="2022-06-05 16:03:00",
    )

    calls = [
        mock.call(retrieve_sql),
    ]
    mock_snowflake_session.execute_query.assert_has_calls(calls, any_order=True)
