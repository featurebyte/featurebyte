"""
This module contains unit tests for FeatureListManagerSnowflake
"""
from unittest import mock

import pandas as pd
import pytest

from featurebyte.feature_manager.snowflake_sql_template import (
    tm_insert_feature_list_registry,
    tm_select_feature_list_registry,
    tm_update_feature_list_registry,
)


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_insert_feature_registry(
    mock_execute_query, mock_snowflake_feature_list_model, feature_list_manager
):
    """
    Test insert_feature_list_registry
    """
    mock_execute_query.size_effect = None
    await feature_list_manager.insert_feature_list_registry(mock_snowflake_feature_list_model)
    assert mock_execute_query.call_count == 2

    feature_lst = [
        {"feature": f.name, "version": f.version.to_str()}
        for f in mock_snowflake_feature_list_model.feature_signatures
    ]
    feature_lst_str = str(feature_lst).replace("'", '"')

    insert_sql = tm_insert_feature_list_registry.render(
        feature_list=mock_snowflake_feature_list_model, feature_lst_str=feature_lst_str
    )

    calls = [
        mock.call(insert_sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_retrieve_feature_list(
    mock_execute_query, mock_snowflake_feature_list_model, feature_list_manager
):
    """
    Test retrieve_feature_list_registries
    """
    mock_execute_query.return_value = pd.DataFrame.from_dict(
        {
            "NAME": ["feature_list1"],
            "VERSION": ["v1"],
            "READINESS": ["DRAFT"],
            "STATUS": ["DRAFT"],
            "FEATURE_VERSIONS": [[]],
        }
    )
    f_reg_df = await feature_list_manager.retrieve_feature_list_registries(
        mock_snowflake_feature_list_model
    )
    assert mock_execute_query.call_count == 1

    sql = tm_select_feature_list_registry.render(
        feature_list_name=mock_snowflake_feature_list_model.name
    )
    calls = [
        mock.call(sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)

    assert len(f_reg_df) == 1
    assert f_reg_df.iloc[0]["NAME"] == "feature_list1"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["FEATURE_VERSIONS"] == []


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_update_feature_list(
    mock_execute_query, mock_snowflake_feature_list_model, feature_list_manager
):
    """
    Test retrieve_features
    """
    mock_execute_query.return_value = ["feature_list1"]
    await feature_list_manager.update_feature_list_registry(mock_snowflake_feature_list_model)
    assert mock_execute_query.call_count == 2

    sql = tm_update_feature_list_registry.render(feature_list=mock_snowflake_feature_list_model)
    calls = [
        mock.call(sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)


@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.generate_tiles")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.update_tile_entity_tracker")
@pytest.mark.asyncio
async def test_generate_tiles_on_demand(
    mock_generate_tiles,
    mock_update_tile_entity_tracker,
    mock_snowflake_tile,
    feature_list_manager,
):
    """
    Test generate_tiles_on_demand
    """
    mock_generate_tiles.size_effect = None
    mock_update_tile_entity_tracker.size_effect = None

    await feature_list_manager.generate_tiles_on_demand(
        [(mock_snowflake_tile, "temp_entity_table")]
    )

    mock_generate_tiles.assert_called_once()
    mock_update_tile_entity_tracker.assert_called_once()
