"""
This module contains unit tests for FeatureManagerSnowflake
"""
from unittest import mock

import pandas as pd
import pytest

from featurebyte.common.model_util import get_version
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.sql_template import (
    tm_delete_online_store_mapping,
    tm_delete_tile_feature_mapping,
    tm_feature_tile_monitor,
    tm_upsert_online_store_mapping,
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

    # Expected execute_query calls:
    # 1. merge into TILE_FEATURE_MAPPING
    # 2. merge into ONLINE_STORE_MAPPING
    # 3. SHOW TASKS LIKE
    # 4. call SP_TILES_SCHEDULE_ONLINE_STORE
    # 5. check TILE_REGISTRY FOR LAST_TILE_START_DATE
    assert mock_execute_query.call_count == 5

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
    assert mock_execute_query.call_args_list[0] == mock.call(upsert_sql)

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
    assert mock_execute_query.call_args_list[1] == mock.call(upsert_sql)


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
        None,
        pd.DataFrame.from_dict({"name": ["task_1"]}),
        None,
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

    mock_execute_query.side_effect = [None, None, None, None]
    with mock.patch("featurebyte.tile.base.BaseTileManager.remove_tile_jobs") as mock_tile_manager:
        mock_tile_manager.side_effect = None
        await feature_manager.online_disable(feature_spec)

    delete_sql = tm_delete_tile_feature_mapping.render(
        aggregation_id=feature_spec.aggregation_ids[0],
        feature_name=feature_spec.feature.name,
        feature_version=feature_spec.feature.version.to_str(),
    )
    assert mock_execute_query.call_args_list[0] == mock.call(delete_sql)

    delete_sql = tm_delete_online_store_mapping.render(
        aggregation_id=feature_spec.aggregation_ids[0],
    )
    assert mock_execute_query.call_args_list[1] == mock.call(delete_sql)

    assert mock_execute_query.call_args_list[2] == mock.call(
        f"SELECT * FROM TILE_FEATURE_MAPPING WHERE AGGREGATION_ID = '{feature_spec.aggregation_ids[0]}' and IS_DELETED = FALSE"
    )


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
