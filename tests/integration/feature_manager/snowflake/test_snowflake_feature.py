"""
This module contains integration tests for FeatureSnowflake
"""
import json

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from featurebyte.models.feature import FeatureReadiness


def test_insert_feature_registry(snowflake_session, snowflake_feature, feature_manager):
    """
    Test insert_feature_registry
    """
    feature_manager.insert_feature_registry(snowflake_feature)

    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "sum_30m"
    assert result.iloc[0]["VERSION"] == "v1"

    expected_df = pd.DataFrame.from_dict(
        {
            "NAME": ["sum_30m"],
            "VERSION": ["v1"],
            "READINESS": ["DRAFT"],
        }
    )
    result_df = result[
        [
            "NAME",
            "VERSION",
            "READINESS",
        ]
    ]
    assert_frame_equal(expected_df, result_df)

    expected_tile_spec = {
        "blind_spot_second": 3,
        "entity_column_names": ["col1"],
        "value_column_names": ["col2"],
        "frequency_minute": 5,
        "tile_id": "tile_id1",
        "tile_sql": 'SELECT\n"col_a" AS "a"\nFROM DUMMY',
        "time_modulo_frequency_second": 183,
    }
    result_tile_spec = json.loads(result["TILE_SPECS"].iloc[0])[0]
    assert expected_tile_spec == result_tile_spec


def test_insert_feature_registry_duplicate(snowflake_session, snowflake_feature, feature_manager):
    """
    Test insert_feature_registry duplicate with exception
    """
    feature_manager.insert_feature_registry(snowflake_feature)

    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "sum_30m"
    assert result.iloc[0]["VERSION"] == "v1"

    with pytest.raises(ValueError) as excinfo:
        feature_manager.insert_feature_registry(snowflake_feature)

    assert (
        str(excinfo.value)
        == f"Feature version already exist for {snowflake_feature.name} with version {snowflake_feature.version}"
    )


def test_update_feature_registry(snowflake_session, snowflake_feature, feature_manager):
    """
    Test update_feature_registry
    """
    feature_manager.insert_feature_registry(snowflake_feature)
    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "sum_30m"
    assert result.iloc[0]["VERSION"] == "v1"
    assert result.iloc[0]["READINESS"] == "DRAFT"
    assert result.iloc[0]["DESCRIPTION"] == "test_description_1"
    assert bool(result.iloc[0]["IS_DEFAULT"]) is True

    snowflake_feature.readiness = FeatureReadiness.PRODUCTION_READY
    snowflake_feature.is_default = False
    snowflake_feature.description = "test_description_2"
    feature_manager.update_feature_registry(new_feature=snowflake_feature)
    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "sum_30m"
    assert result.iloc[0]["VERSION"] == "v1"
    assert result.iloc[0]["READINESS"] == "PRODUCTION_READY"
    assert result.iloc[0]["DESCRIPTION"] == "test_description_2"
    assert bool(result.iloc[0]["IS_DEFAULT"]) is False


def test_retrieve_features(snowflake_feature, feature_manager):
    """
    Test retrieve_features
    """
    feature_manager.insert_feature_registry(snowflake_feature)
    f_reg_df = feature_manager.retrieve_feature_registries(snowflake_feature)
    assert len(f_reg_df) == 1
    assert f_reg_df.iloc[0]["NAME"] == "sum_30m"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["READINESS"] == "DRAFT"

    f_reg_df = feature_manager.retrieve_feature_registries(feature=snowflake_feature, version="v1")
    assert len(f_reg_df) == 1
    assert f_reg_df.iloc[0]["NAME"] == "sum_30m"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["READINESS"] == "DRAFT"


def test_retrieve_features_multiple(snowflake_feature, feature_manager):
    """
    Test retrieve_features return multiple features
    """
    feature_manager.insert_feature_registry(snowflake_feature)

    snowflake_feature.version = "v2"
    snowflake_feature.readiness = FeatureReadiness.PRODUCTION_READY
    feature_manager.insert_feature_registry(snowflake_feature)

    f_reg_df = feature_manager.retrieve_feature_registries(snowflake_feature)
    assert len(f_reg_df) == 2
    assert f_reg_df.iloc[0]["NAME"] == "sum_30m"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["READINESS"] == "DRAFT"
    assert f_reg_df.iloc[1]["NAME"] == "sum_30m"
    assert f_reg_df.iloc[1]["VERSION"] == "v2"
    assert f_reg_df.iloc[1]["READINESS"] == "PRODUCTION_READY"


def test_online_enable_no_feature(snowflake_feature, feature_manager):
    """
    Test online_enable no feature
    """
    snowflake_feature.readiness = FeatureReadiness.PRODUCTION_READY
    with pytest.raises(ValueError) as excinfo:
        feature_manager.online_enable(snowflake_feature)

    assert (
        str(excinfo.value)
        == f"feature {snowflake_feature.name} with version {snowflake_feature.version} does not exist"
    )


def test_online_enable_not_production_ready(snowflake_feature, feature_manager):
    """
    Test online_enable not production_ready
    """
    feature_manager.insert_feature_registry(snowflake_feature)
    with pytest.raises(ValueError) as excinfo:
        feature_manager.online_enable(snowflake_feature)

    assert str(excinfo.value) == "feature readiness has to be PRODUCTION_READY before online_enable"


def test_online_enable_already_online_enabled(snowflake_feature, feature_manager):
    """
    Test online_enable already online_enabled
    """
    snowflake_feature.readiness = FeatureReadiness.PRODUCTION_READY
    feature_manager.insert_feature_registry(snowflake_feature)
    feature_manager.online_enable(snowflake_feature)

    with pytest.raises(ValueError) as excinfo:
        feature_manager.online_enable(snowflake_feature)

    assert (
        str(excinfo.value)
        == f"feature {snowflake_feature.name} with version {snowflake_feature.version} is already online enabled"
    )


def test_online_enable(snowflake_session, snowflake_feature, feature_manager):
    """
    Test online_enable
    """
    snowflake_feature.readiness = FeatureReadiness.PRODUCTION_READY
    feature_manager.insert_feature_registry(snowflake_feature)
    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "sum_30m"
    assert result.iloc[0]["VERSION"] == "v1"
    assert bool(result.iloc[0]["ONLINE_ENABLED"]) is False

    feature_manager.online_enable(snowflake_feature)

    tile_registry = snowflake_session.execute_query("SELECT * FROM TILE_REGISTRY")
    assert len(tile_registry) == 1
    assert tile_registry.iloc[0]["TILE_ID"] == "tile_id1"
    assert tile_registry.iloc[0]["TILE_SQL"] == 'SELECT\n"col_a" AS "a"\nFROM DUMMY'
    assert bool(tile_registry.iloc[0]["IS_ENABLED"]) is True

    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "sum_30m"
    assert result.iloc[0]["VERSION"] == "v1"
    assert bool(result.iloc[0]["ONLINE_ENABLED"]) is True

    tasks = snowflake_session.execute_query("SHOW TASKS")
    assert len(tasks) > 1
    assert tasks["name"].iloc[0] == "SHELL_TASK_TILE_ID1_OFFLINE"
    assert tasks["schedule"].iloc[0] == "USING CRON 3 0 * * * UTC"
    assert tasks["state"].iloc[0] == "started"
    assert tasks["name"].iloc[1] == "SHELL_TASK_TILE_ID1_ONLINE"
    assert tasks["schedule"].iloc[1] == "USING CRON 3-59/5 * * * * UTC"
    assert tasks["state"].iloc[1] == "started"


def test_get_last_tile_index(snowflake_feature, snowflake_tile, feature_manager, tile_manager):
    """
    Test get_last_tile_index
    """
    feature_manager.insert_feature_registry(snowflake_feature)
    tile_manager.insert_tile_registry(tile_spec=snowflake_tile)
    last_index_df = feature_manager.get_last_tile_index(snowflake_feature)
    assert len(last_index_df) == 1
    assert last_index_df.iloc[0]["TILE_ID"] == snowflake_tile.tile_id
    assert last_index_df.iloc[0]["LAST_TILE_INDEX_ONLINE"] == -1
