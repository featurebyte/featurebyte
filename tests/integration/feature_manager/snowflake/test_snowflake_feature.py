"""
This module contains integration tests for FeatureSnowflake
"""
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal


def test_insert_feature_registry(fb_db_session, snowflake_feature):
    """
    Test insert_feature_registry
    """
    snowflake_feature.insert_feature_registry()

    result = fb_db_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "test_feature1"
    assert result.iloc[0]["VERSION"] == "v1"

    expected_df = pd.DataFrame.from_dict(
        {
            "NAME": ["test_feature1"],
            "VERSION": ["v1"],
            "STATUS": ["DRAFT"],
            "TIME_MODULO_FREQUENCY_SECOND": [183],
            "BLIND_SPOT_SECOND": [3],
            "FREQUENCY_MINUTES": [5],
        }
    )

    result = result[
        [
            "NAME",
            "VERSION",
            "STATUS",
            "TIME_MODULO_FREQUENCY_SECOND",
            "BLIND_SPOT_SECOND",
            "FREQUENCY_MINUTES",
        ]
    ]
    assert_frame_equal(expected_df, result)


def test_insert_feature_registry_duplicate(fb_db_session, snowflake_feature):
    """
    Test insert_feature_registry duplicate with exception
    """
    snowflake_feature.insert_feature_registry()

    result = fb_db_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "test_feature1"
    assert result.iloc[0]["VERSION"] == "v1"

    with pytest.raises(ValueError) as excinfo:
        snowflake_feature.insert_feature_registry()

    assert (
        str(excinfo.value)
        == f"Feature {snowflake_feature.feature.name} with version {snowflake_feature.feature.version} already exists"
    )


def test_retrieve_features(snowflake_feature):
    """
    Test retrieve_features
    """
    snowflake_feature.insert_feature_registry()
    feature_versions = snowflake_feature.retrieve_features()
    assert len(feature_versions) == 1
    assert feature_versions[0].name == "test_feature1"
    assert feature_versions[0].version == "v1"

    feature_versions = snowflake_feature.retrieve_features(version="v1")
    assert len(feature_versions) == 1
    assert feature_versions[0].name == "test_feature1"
    assert feature_versions[0].version == "v1"


def test_retrieve_features_multiple(snowflake_feature):
    """
    Test retrieve_features return multiple features
    """
    snowflake_feature.insert_feature_registry()

    snowflake_feature.feature.version = "v2"
    snowflake_feature.insert_feature_registry()

    feature_versions = snowflake_feature.retrieve_features()
    assert len(feature_versions) == 2
    assert feature_versions[0].name == "test_feature1"
    assert feature_versions[0].version == "v1"
    assert feature_versions[1].name == "test_feature1"
    assert feature_versions[1].version == "v2"


def test_online_enable(fb_db_session, snowflake_feature):
    """
    Test online_enable
    """
    snowflake_feature.online_enable()

    tile_registry = fb_db_session.execute_query("SELECT * FROM TILE_REGISTRY")
    assert len(tile_registry) == 1
    assert tile_registry.iloc[0]["TILE_ID"] == "TILE_ID1"
    assert tile_registry.iloc[0]["TILE_SQL"] == "SELECT * FROM DUMMY"

    tasks = fb_db_session.execute_query("SHOW TASKS")
    assert len(tasks) == 2
    assert tasks["name"].iloc[0] == "SHELL_TASK_TILE_ID1_OFFLINE"
    assert tasks["schedule"].iloc[0] == "USING CRON 3 0 * * * UTC"
    assert tasks["state"].iloc[0] == "started"
    assert tasks["name"].iloc[1] == "SHELL_TASK_TILE_ID1_ONLINE"
    assert tasks["schedule"].iloc[1] == "USING CRON 3-59/5 * * * * UTC"
    assert tasks["state"].iloc[1] == "started"


def test_get_last_tile_index(snowflake_feature):
    """
    Test get_last_tile_index
    """
    snowflake_feature.insert_feature_registry()
    last_index = snowflake_feature.get_last_tile_index("online")
    assert last_index == -1

    last_index = snowflake_feature.get_last_tile_index("offline")
    assert last_index == -1
