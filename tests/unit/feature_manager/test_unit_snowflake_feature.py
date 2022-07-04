"""
This module contains unit tests for FeatureSnowflake
"""
from unittest import mock

import pandas as pd

from featurebyte.feature_manager.snowflake_feature import (
    tm_insert_feature_registry,
    tm_update_feature_registry,
)
from featurebyte.feature_manager.snowflake_sql_template import tm_select_feature_registry


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_insert_feature_registry(mock_execute_query, mock_snowflake_feature):
    """
    Test insert_feature_registry
    """
    mock_execute_query.size_effect = None
    mock_snowflake_feature.insert_feature_registry()
    assert mock_execute_query.call_count == 3

    update_sql = tm_update_feature_registry.render(
        feature_name=mock_snowflake_feature.feature.name, is_default=False
    )

    tile_specs_lst = [tile_spec.dict() for tile_spec in mock_snowflake_feature.feature.tile_specs]
    tile_specs_str = str(tile_specs_lst).replace("'", '"')
    insert_sql = tm_insert_feature_registry.render(
        feature=mock_snowflake_feature.feature, tile_specs_str=tile_specs_str
    )

    calls = [
        mock.call(update_sql),
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
            "READINESS": ["DRAFT"],
            "IS_DEFAULT": [True],
            "TIME_MODULO_FREQUENCY_SECOND": [183],
            "BLIND_SPOT_SECOND": [3],
            "FREQUENCY_MINUTES": [5],
            "TILE_SQL": ["SELECT DUMMY"],
            "TILE_SPECS": ["[]"],
            "COLUMN_NAMES": ["c1"],
            "ONLINE_ENABLED": [True],
        }
    )
    fv_list = mock_snowflake_feature.retrieve_feature_registries()
    assert mock_execute_query.call_count == 1

    sql = tm_select_feature_registry.render(feature_name=mock_snowflake_feature.feature.name)
    calls = [
        mock.call(sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)

    assert len(fv_list) == 1
    assert fv_list[0].name == mock_snowflake_feature.feature.name
    assert fv_list[0].version == "v1"
    assert fv_list[0].tile_specs == []


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
