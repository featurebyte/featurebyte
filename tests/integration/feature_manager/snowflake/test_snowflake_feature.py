"""
This module contains integration tests for FeatureSnowflake
"""
import json

import pandas as pd
from pandas.testing import assert_frame_equal

from featurebyte.models.feature import FeatureReadiness


def test_insert_feature_registry(snowflake_session, snowflake_feature):
    """
    Test insert_feature_registry
    """
    flag = snowflake_feature.insert_feature_registry()
    assert flag is True

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
        "column_names": "col1",
        "frequency_minute": 5,
        "tile_id": "tile_id1",
        "tile_sql": "SELECT * FROM DUMMY",
        "time_modulo_frequency_second": 183,
    }
    result_tile_spec = json.loads(result["TILE_SPECS"].iloc[0])[0]
    assert expected_tile_spec == result_tile_spec


def test_insert_feature_registry_duplicate(snowflake_session, snowflake_feature):
    """
    Test insert_feature_registry duplicate with exception
    """
    flag = snowflake_feature.insert_feature_registry()
    assert flag is True

    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "sum_30m"
    assert result.iloc[0]["VERSION"] == "v1"

    flag = snowflake_feature.insert_feature_registry()
    assert flag is False


def test_retrieve_features(snowflake_feature):
    """
    Test retrieve_features
    """
    snowflake_feature.insert_feature_registry()
    f_reg_df = snowflake_feature.retrieve_feature_registries()
    assert len(f_reg_df) == 1
    assert f_reg_df.iloc[0]["NAME"] == "sum_30m"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["READINESS"] == "DRAFT"

    f_reg_df = snowflake_feature.retrieve_feature_registries(version="v1")
    assert len(f_reg_df) == 1
    assert f_reg_df.iloc[0]["NAME"] == "sum_30m"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["READINESS"] == "DRAFT"


def test_retrieve_features_multiple(snowflake_feature):
    """
    Test retrieve_features return multiple features
    """
    snowflake_feature.insert_feature_registry()

    snowflake_feature.feature.version = "v2"
    snowflake_feature.feature.readiness = FeatureReadiness.PRODUCTION_READY
    snowflake_feature.insert_feature_registry()

    f_reg_df = snowflake_feature.retrieve_feature_registries()
    assert len(f_reg_df) > 1
    assert f_reg_df.iloc[0]["NAME"] == "sum_30m"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["READINESS"] == "DRAFT"
    assert f_reg_df.iloc[1]["NAME"] == "sum_30m"
    assert f_reg_df.iloc[1]["VERSION"] == "v2"
    assert f_reg_df.iloc[1]["READINESS"] == "PRODUCTION_READY"


def test_online_enable(snowflake_session, snowflake_feature):
    """
    Test online_enable
    """
    snowflake_feature.online_enable()

    tile_registry = snowflake_session.execute_query("SELECT * FROM TILE_REGISTRY")
    assert len(tile_registry) == 1
    assert tile_registry.iloc[0]["TILE_ID"] == "tile_id1"
    assert tile_registry.iloc[0]["TILE_SQL"] == "SELECT * FROM DUMMY"

    tasks = snowflake_session.execute_query("SHOW TASKS")
    assert len(tasks) > 1
    assert tasks["name"].iloc[0] == "SHELL_TASK_TILE_ID1_OFFLINE"
    assert tasks["schedule"].iloc[0] == "USING CRON 3 0 * * * UTC"
    assert tasks["state"].iloc[0] == "started"
    assert tasks["name"].iloc[1] == "SHELL_TASK_TILE_ID1_ONLINE"
    assert tasks["schedule"].iloc[1] == "USING CRON 3-59/5 * * * * UTC"
    assert tasks["state"].iloc[1] == "started"


def test_get_last_tile_index(snowflake_feature, snowflake_tile):
    """
    Test get_last_tile_index
    """
    snowflake_feature.insert_feature_registry()
    snowflake_tile.insert_tile_registry()
    last_index_df = snowflake_feature.get_last_tile_index()
    assert len(last_index_df) == 1
    assert last_index_df.iloc[0]["TILE_ID"] == snowflake_tile.tile_id
    assert last_index_df.iloc[0]["LAST_TILE_INDEX_ONLINE"] == -1
