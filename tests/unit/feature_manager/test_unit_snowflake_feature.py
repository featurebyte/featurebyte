"""
This module contains unit tests for FeatureManagerSnowflake
"""
from unittest import mock

import pandas as pd

from featurebyte.feature_manager.snowflake_sql_template import (
    tm_insert_feature_registry,
    tm_select_feature_registry,
    tm_update_feature_registry,
)


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_insert_feature_registry(mock_execute_query, mock_snowflake_feature, feature_manager):
    """
    Test insert_feature_registry
    """
    mock_execute_query.size_effect = None
    feature_manager.insert_feature_registry(mock_snowflake_feature)
    assert mock_execute_query.call_count == 3

    update_sql = tm_update_feature_registry.render(
        feature_name=mock_snowflake_feature.name, col_name="is_default", col_value=False
    )

    tile_specs_lst = [tile_spec.dict() for tile_spec in mock_snowflake_feature.tile_specs]
    tile_specs_str = str(tile_specs_lst).replace("'", '"')
    insert_sql = tm_insert_feature_registry.render(
        feature=mock_snowflake_feature, tile_specs_str=tile_specs_str
    )

    calls = [
        mock.call(update_sql),
        mock.call(insert_sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_retrieve_features(mock_execute_query, mock_snowflake_feature, feature_manager):
    """
    Test retrieve_features
    """
    mock_execute_query.return_value = pd.DataFrame.from_dict(
        {
            "NAME": ["sum_30m"],
            "VERSION": ["v1"],
            "READINESS": ["DRAFT"],
            "IS_DEFAULT": [True],
            "TIME_MODULO_FREQUENCY_SECOND": [183],
            "BLIND_SPOT_SECOND": [3],
            "FREQUENCY_MINUTES": [5],
            "TILE_SQL": ["SELECT DUMMY"],
            "TILE_SPECS": [[]],
            "COLUMN_NAMES": ["c1"],
            "ONLINE_ENABLED": [True],
        }
    )
    f_reg_df = feature_manager.retrieve_feature_registries(mock_snowflake_feature)
    assert mock_execute_query.call_count == 1

    sql = tm_select_feature_registry.render(feature_name=mock_snowflake_feature.name)
    calls = [
        mock.call(sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)

    assert len(f_reg_df) == 1
    assert f_reg_df.iloc[0]["NAME"] == "sum_30m"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["TILE_SPECS"] == []


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_update_feature_list(mock_execute_query, mock_snowflake_feature, feature_manager):
    """
    Test retrieve_features
    """
    mock_execute_query.return_value = ["feature_list1"]
    feature_manager.update_feature_registry(
        mock_snowflake_feature, attribute_name="status", attribute_value="DRAFT"
    )
    assert mock_execute_query.call_count == 2

    sql = tm_update_feature_registry.render(
        feature_name=mock_snowflake_feature.name, col_name="status", col_value="'DRAFT'"
    )
    calls = [
        mock.call(sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)


@mock.patch("featurebyte.tile.snowflake_tile.TileSnowflake.insert_tile_registry")
@mock.patch("featurebyte.tile.snowflake_tile.TileSnowflake.schedule_online_tiles")
@mock.patch("featurebyte.tile.snowflake_tile.TileSnowflake.schedule_offline_tiles")
def test_online_enable(
    mock_insert_tile_registry,
    mock_schedule_online_tiles,
    mock_schedule_offline_tiles,
    mock_snowflake_feature,
    feature_manager,
):
    """
    Test online_enable
    """
    feature_manager.online_enable(mock_snowflake_feature)

    mock_insert_tile_registry.assert_called_once()
    mock_schedule_online_tiles.assert_called_once()
    mock_schedule_offline_tiles.assert_called_once()


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_get_last_tile_index(mock_execute_query, mock_snowflake_feature, feature_manager):
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
    last_index_df = feature_manager.get_last_tile_index(mock_snowflake_feature)
    assert last_index_df.iloc[0]["TILE_ID"] == "TILE_ID1"
    assert last_index_df.iloc[0]["LAST_TILE_INDEX_ONLINE"] == 100
