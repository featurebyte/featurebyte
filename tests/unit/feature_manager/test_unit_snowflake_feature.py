"""
This module contains unit tests for FeatureManagerSnowflake
"""
import json
from unittest import mock

import pandas as pd
import pytest

from featurebyte.common.model_util import get_version
from featurebyte.exception import (
    DuplicateTileTaskError,
    InvalidFeatureRegistryOperationError,
    MissingFeatureRegistryError,
    MissingTileSpecError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_sql_template import (
    tm_feature_tile_monitor,
    tm_insert_feature_registry,
    tm_remove_feature_registry,
    tm_select_feature_registry,
    tm_update_feature_registry,
    tm_update_feature_registry_default_false,
    tm_upsert_tile_feature_mapping,
)
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.tile import OnlineFeatureSpec, TileSpec, TileType


@pytest.fixture(name="mock_snowflake_feature")
def mock_snowflake_feature_fixture(mock_snowflake_feature):
    """
    ExtendedFeatureModel object fixture
    """
    return ExtendedFeatureModel(
        **mock_snowflake_feature.dict(exclude={"version": True}),
        feature_store=mock_snowflake_feature.feature_store,
        version=get_version(),
    )


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_insert_feature_registry(mock_execute_query, mock_snowflake_feature, feature_manager):
    """
    Test insert_feature_registry
    """
    mock_execute_query.size_effect = None
    mock_snowflake_feature.__dict__["event_data_ids"] = [
        PydanticObjectId("62d8d944d01041a098785131")
    ]
    await feature_manager.insert_feature_registry(mock_snowflake_feature)
    assert mock_execute_query.call_count == 3

    update_sql = tm_update_feature_registry_default_false.render(feature=mock_snowflake_feature)

    tile_specs_lst = [tile_spec.dict() for tile_spec in mock_snowflake_feature.tile_specs]
    tile_specs_str = json.dumps(tile_specs_lst).replace("\\", "\\\\")

    insert_sql = tm_insert_feature_registry.render(
        feature=mock_snowflake_feature,
        tile_specs_str=tile_specs_str,
        event_ids_str="62d8d944d01041a098785131",
    )

    calls = [
        mock.call(update_sql),
        mock.call(insert_sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_remove_feature_registry(mock_execute_query, mock_snowflake_feature, feature_manager):
    """
    Test remove_feature_registry
    """
    mock_execute_query.size_effect = None
    mock_execute_query.return_value = pd.DataFrame.from_dict(
        {
            "NAME": ["sum_30m"],
            "VERSION": ["v1"],
            "READINESS": ["DRAFT"],
        }
    )
    await feature_manager.remove_feature_registry(mock_snowflake_feature)
    assert mock_execute_query.call_count == 2

    remove_sql = tm_remove_feature_registry.render(feature=mock_snowflake_feature)

    calls = [
        mock.call(remove_sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_remove_feature_registry_no_feature(
    mock_execute_query, mock_snowflake_feature, feature_manager
):
    """
    Test remove_feature_registry no feature
    """
    mock_execute_query.size_effect = None
    mock_execute_query.return_value = []

    with pytest.raises(MissingFeatureRegistryError) as excinfo:
        await feature_manager.remove_feature_registry(mock_snowflake_feature)

    assert str(excinfo.value) == (
        f"Feature version does not exist for {mock_snowflake_feature.name} with version "
        f"{mock_snowflake_feature.version.to_str()}"
    )


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_remove_feature_registry_feature_version_not_draft(
    mock_execute_query, mock_snowflake_feature, feature_manager
):
    """
    Test remove_feature_registry feature version readiness not DRAFT
    """
    mock_execute_query.size_effect = None
    mock_execute_query.return_value = pd.DataFrame.from_dict(
        {
            "NAME": ["sum_30m"],
            "VERSION": ["v1"],
            "READINESS": ["PRODUCTION_READY"],
        }
    )

    with pytest.raises(InvalidFeatureRegistryOperationError) as excinfo:
        await feature_manager.remove_feature_registry(mock_snowflake_feature)

    assert str(excinfo.value) == (
        f"Feature version {mock_snowflake_feature.name} with version {mock_snowflake_feature.version.to_str()} "
        f"cannot be deleted with readiness PRODUCTION_READY"
    )


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_retrieve_features(mock_execute_query, mock_snowflake_feature, feature_manager):
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
            "EVENT_DATA_IDS": ["626bccb9697a12204fb22ea3,726bccb9697a12204fb22ea3"],
        }
    )
    f_reg_df = await feature_manager.retrieve_feature_registries(mock_snowflake_feature)
    assert mock_execute_query.call_count == 1

    sql = tm_select_feature_registry.render(feature_name=mock_snowflake_feature.name, version=None)
    calls = [
        mock.call(sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)

    assert len(f_reg_df) == 1
    assert f_reg_df.iloc[0]["NAME"] == "sum_30m"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["TILE_SPECS"] == []
    assert f_reg_df.iloc[0]["EVENT_DATA_IDS"] == "626bccb9697a12204fb22ea3,726bccb9697a12204fb22ea3"


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_update_feature_list(mock_execute_query, mock_snowflake_feature, feature_manager):
    """
    Test retrieve_features
    """
    mock_execute_query.return_value = ["feature_list1"]
    await feature_manager.update_feature_registry(mock_snowflake_feature, to_online_enable=True)
    assert mock_execute_query.call_count == 2

    sql = tm_update_feature_registry.render(feature=mock_snowflake_feature, online_enabled=True)
    calls = [
        mock.call(sql),
    ]
    mock_execute_query.assert_has_calls(calls, any_order=True)


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.schedule_online_tiles")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.schedule_offline_tiles")
@pytest.mark.asyncio
async def test_online_enable(
    mock_schedule_offline_tiles,
    mock_schedule_online_tiles,
    mock_execute_query,
    mock_snowflake_feature,
    feature_manager,
):
    """
    Test online_enable
    """

    online_feature_spec = OnlineFeatureSpec(
        feature_name=mock_snowflake_feature.name,
        feature_version=mock_snowflake_feature.version.to_str(),
        feature_sql="select * from temp",
        feature_store_table_name="feature_store_table_1",
        tile_specs=mock_snowflake_feature.tile_specs,
    )

    await feature_manager.online_enable(online_feature_spec)

    mock_schedule_online_tiles.assert_called_once()
    mock_schedule_offline_tiles.assert_called_once()

    assert mock_execute_query.call_count == 2

    entity_column_names_str = ",".join(sorted(online_feature_spec.entity_column_names))
    upsert_sql = tm_upsert_tile_feature_mapping.render(
        tile_id=online_feature_spec.tile_ids[0],
        feature_spec=online_feature_spec,
        entity_column_names_str=entity_column_names_str,
    )
    mock_execute_query.assert_called_with(upsert_sql)


@pytest.mark.asyncio
async def test_online_enable_missing_tile_spec(
    mock_snowflake_feature,
    feature_manager,
):
    """
    Test online_enable
    """

    online_feature_spec = OnlineFeatureSpec(
        feature_name=mock_snowflake_feature.name,
        feature_version=mock_snowflake_feature.version.to_str(),
        feature_sql="select * from temp",
        feature_store_table_name="feature_store_table_1",
        tile_specs=[],
    )

    with pytest.raises(MissingTileSpecError) as excinfo:
        await feature_manager.online_enable(online_feature_spec)

    assert str(excinfo.value) == "Missing tile_spec for online_enable"


@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.tile_task_exists")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.schedule_online_tiles")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.schedule_offline_tiles")
@pytest.mark.asyncio
async def test_online_enable_duplicate_tile_task(
    mock_schedule_offline_tiles,
    mock_schedule_online_tiles,
    mock_tile_task_exists,
    mock_snowflake_feature,
    feature_manager,
):
    """
    Test online_enable
    """
    _ = mock_schedule_offline_tiles
    _ = mock_schedule_online_tiles

    mock_tile_task_exists.return_value = True

    online_feature_spec = OnlineFeatureSpec(
        feature_name=mock_snowflake_feature.name,
        feature_version=mock_snowflake_feature.version.to_str(),
        feature_sql="select * from temp",
        feature_store_table_name="feature_store_table_1",
        tile_specs=mock_snowflake_feature.tile_specs,
    )

    with pytest.raises(DuplicateTileTaskError) as excinfo:
        await feature_manager.online_enable(online_feature_spec)

    assert str(excinfo.value) == "tile task already exists"


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
