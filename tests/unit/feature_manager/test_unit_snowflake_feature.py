"""
This module contains unit tests for FeatureSnowflake
"""
from unittest import mock

import pandas as pd

from featurebyte.feature_manager.snowflake_feature import tm_ins_feature_registry


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_insert_feature_registry(mock_execute_query, mock_snowflake_feature):
    """
    Test insert_feature_registry
    """
    mock_execute_query.size_effect = None
    mock_snowflake_feature.insert_feature_registry()
    assert mock_execute_query.call_count == 3

    insert_sql = tm_ins_feature_registry.render(
        feature=mock_snowflake_feature.feature, tile_ids_str='["TILE_ID1"]'
    )
    calls = [
        mock.call(
            f"SELECT * FROM FEATURE_REGISTRY WHERE NAME = '{mock_snowflake_feature.feature.name}'"
            f" AND VERSION = '{mock_snowflake_feature.feature.version}'"
        ),
        mock.call(
            f"UPDATE FEATURE_REGISTRY SET IS_DEFAULT = False WHERE NAME = '{mock_snowflake_feature.feature.name}'"
        ),
        mock.call(insert_sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_retrieve_features(mock_execute_query, mock_snowflake_feature):
    """
    Test retrieve_features
    """
    mock_execute_query.return_value = pd.DataFrame.from_dict(
        {
            "VERSION": ["v1"],
            "STATUS": ["DRAFT"],
            "IS_DEFAULT": [True],
            "TIME_MODULO_FREQUENCY_SECOND": [183],
            "BLIND_SPOT_SECOND": [3],
            "FREQUENCY_MINUTES": [5],
            "TILE_SQL": ["SELECT DUMMY"],
            "COLUMN_NAMES": ["c1"],
            "TILE_IDS": ['["tile_id1"]'],
            "ONLINE_ENABLED": [True],
        }
    )
    fv_list = mock_snowflake_feature.retrieve_features()
    assert mock_execute_query.call_count == 1
    calls = [
        mock.call(
            f"SELECT * FROM FEATURE_REGISTRY WHERE NAME = '{mock_snowflake_feature.feature.name}'"
        ),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)

    assert len(fv_list) == 1
    assert fv_list[0].name == mock_snowflake_feature.feature.name
    assert fv_list[0].version == "v1"
    assert fv_list[0].tile_ids == ["tile_id1"]


@mock.patch("featurebyte.tile.snowflake_tile.TileSnowflake.insert_tile_registry")
@mock.patch("featurebyte.tile.snowflake_tile.TileSnowflake.schedule_online_tiles")
@mock.patch("featurebyte.tile.snowflake_tile.TileSnowflake.schedule_offline_tiles")
def test_online_enable(
    mock_insert_tile_registry,
    mock_schedule_online_tiles,
    mock_schedule_offline_tiles,
    mock_snowflake_feature,
):
    """
    Test online_enable
    """
    mock_snowflake_feature.online_enable()

    mock_insert_tile_registry.assert_called_once()
    mock_schedule_online_tiles.assert_called_once()
    mock_schedule_offline_tiles.assert_called_once()


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_get_last_tile_index(mock_execute_query, mock_snowflake_feature):
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
    last_index_df = mock_snowflake_feature.get_last_tile_index()
    assert last_index_df.iloc[0]["TILE_ID"] == "TILE_ID1"
    assert last_index_df.iloc[0]["LAST_TILE_INDEX_ONLINE"] == 100
