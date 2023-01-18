"""
This module contains unit tests for FeatureManagerSnowflake
"""
from unittest import mock

import pandas as pd
import pytest

from featurebyte.common.model_util import get_version
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_sql_template import (
    tm_delete_tile_feature_mapping,
    tm_feature_tile_monitor,
    tm_upsert_tile_feature_mapping,
)
from featurebyte.models.online_store import OnlineFeatureSpec
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


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.schedule_online_tiles")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.schedule_offline_tiles")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.generate_tiles")
@pytest.mark.asyncio
async def test_online_enable(
    mock_generate_tiles,
    mock_schedule_offline_tiles,
    mock_schedule_online_tiles,
    mock_execute_query,
    mock_snowflake_feature,
    feature_manager,
):
    """
    Test online_enable
    """

    feature_spec = OnlineFeatureSpec(
        feature=mock_snowflake_feature,
        feature_sql="select * from temp",
        feature_store_table_name="feature_store_table_1",
    )

    mock_execute_query.return_value = []
    await feature_manager.online_enable(feature_spec)

    mock_schedule_online_tiles.assert_called_once()
    mock_schedule_offline_tiles.assert_called_once()
    mock_generate_tiles.assert_called_once()

    assert mock_execute_query.call_count == 3

    upsert_sql = tm_upsert_tile_feature_mapping.render(
        tile_id=feature_spec.tile_ids[0],
        feature_name=feature_spec.feature.name,
        feature_type=feature_spec.value_type,
        feature_version=feature_spec.feature.version.to_str(),
        feature_readiness=str(mock_snowflake_feature.readiness),
        feature_event_data_ids=",".join([str(i) for i in feature_spec.event_data_ids]),
        feature_sql=feature_spec.feature_sql.replace("'", "''"),
        feature_store_table_name=feature_spec.feature_store_table_name,
        entity_column_names_str=",".join(escape_column_names(feature_spec.serving_names)),
        is_deleted=False,
    )
    assert mock_execute_query.call_args_list[0] == mock.call(upsert_sql)


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.schedule_online_tiles")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.schedule_offline_tiles")
@pytest.mark.asyncio
async def test_online_enable_duplicate_tile_task(
    mock_schedule_offline_tiles,
    mock_schedule_online_tiles,
    mock_execute_query,
    mock_snowflake_feature,
    feature_manager,
):
    """
    Test online_enable
    """
    _ = mock_schedule_offline_tiles
    _ = mock_schedule_online_tiles

    feature_spec = OnlineFeatureSpec(
        feature=mock_snowflake_feature,
        feature_sql="select * from temp",
        feature_store_table_name="feature_store_table_1",
    )

    mock_execute_query.side_effect = [
        None,
        pd.DataFrame.from_dict({"name": ["task_1"]}),
        None,
        None,
    ]
    await feature_manager.online_enable(feature_spec)

    mock_schedule_online_tiles.assert_not_called()
    mock_schedule_offline_tiles.assert_not_called()


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_online_disable(
    mock_execute_query,
    mock_snowflake_feature,
    feature_manager,
):
    """
    Test online_enable
    """

    feature_spec = OnlineFeatureSpec(
        feature=mock_snowflake_feature,
        feature_sql="select * from temp",
        feature_store_table_name="feature_store_table_1",
    )

    mock_execute_query.side_effect = [None, None, None]
    await feature_manager.online_disable(feature_spec)

    delete_sql = tm_delete_tile_feature_mapping.render(
        tile_id=feature_spec.tile_ids[0],
        feature_name=feature_spec.feature.name,
        feature_version=feature_spec.feature.version.to_str(),
    )
    assert mock_execute_query.call_args_list[0] == mock.call(delete_sql)


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_get_last_tile_index(mock_execute_query, mock_snowflake_feature, feature_manager):
    """
    Test get_last_tile_index
    """
    mock_execute_query.return_value = pd.DataFrame.from_dict(
        {
            "TILE_ID": ["TILE_ID1"],
            "LAST_TILE_INDEX_ONLINE": [100],
            "LAST_TILE_INDEX_OFFLINE": [80],
        }
    )
    last_index_df = await feature_manager.retrieve_last_tile_index(mock_snowflake_feature)
    assert last_index_df.iloc[0]["TILE_ID"] == "TILE_ID1"
    assert last_index_df.iloc[0]["LAST_TILE_INDEX_ONLINE"] == 100


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_retrieve_feature_tile_inconsistency_data(mock_execute_query, feature_manager):
    """
    Test retrieve_feature_tile_inconsistency_data
    """
    mock_execute_query.return_value = pd.DataFrame.from_dict(
        {
            "NAME": ["sum_30m", "sum_30m"],
            "VERSION": ["v1", "v1"],
            "TILE_START_DATE": ["2022-06-05 16:03:00", "2022-06-05 15:58:00"],
            "TILE_MONITOR_DATE": ["2022-06-05 16:03:00", "2022-06-05 15:58:00"],
        }
    )
    result = await feature_manager.retrieve_feature_tile_inconsistency_data(
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
    mock_execute_query.assert_has_calls(calls, any_order=True)
