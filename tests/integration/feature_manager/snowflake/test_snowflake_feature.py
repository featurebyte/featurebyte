"""
This module contains integration tests for FeatureSnowflake
"""
import json

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from featurebyte.enum import InternalName
from featurebyte.exception import (
    DuplicatedFeatureRegistryError,
    InvalidFeatureRegistryOperationError,
    MissingFeatureRegistryError,
)
from featurebyte.models.feature import FeatureReadiness


def test_insert_feature_registry(
    snowflake_session,
    snowflake_feature,
    snowflake_feature_expected_tile_spec_dict,
    feature_manager,
):
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

    result_tile_spec = json.loads(result["TILE_SPECS"].iloc[0])[0]
    assert snowflake_feature_expected_tile_spec_dict == result_tile_spec


def test_insert_feature_registry_duplicate(snowflake_session, snowflake_feature, feature_manager):
    """
    Test insert_feature_registry duplicate with exception
    """
    feature_manager.insert_feature_registry(snowflake_feature)

    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "sum_30m"
    assert result.iloc[0]["VERSION"] == "v1"

    with pytest.raises(DuplicatedFeatureRegistryError) as excinfo:
        feature_manager.insert_feature_registry(snowflake_feature)

    assert (
        str(excinfo.value)
        == f"Feature version already exist for {snowflake_feature.name} with version {snowflake_feature.version}"
    )


def test_remove_feature_registry(snowflake_session, snowflake_feature, feature_manager):
    """
    Test remove_feature_registry
    """
    snowflake_feature.readiness = FeatureReadiness.DRAFT.value
    feature_manager.insert_feature_registry(snowflake_feature)
    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "sum_30m"
    assert result.iloc[0]["VERSION"] == "v1"

    feature_manager.remove_feature_registry(snowflake_feature)
    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 0


def test_remove_feature_registry_no_feature(snowflake_feature, feature_manager):
    """
    Test remove_feature_registry no feature
    """
    with pytest.raises(MissingFeatureRegistryError) as excinfo:
        feature_manager.remove_feature_registry(snowflake_feature)

    assert (
        str(excinfo.value)
        == f"Feature version does not exist for {snowflake_feature.name} with version {snowflake_feature.version}"
    )


def test_remove_feature_registry_feature_version_not_draft(snowflake_feature, feature_manager):
    """
    Test remove_feature_registry feature version readiness not DRAFT
    """
    snowflake_feature.readiness = FeatureReadiness.PRODUCTION_READY.value
    feature_manager.insert_feature_registry(snowflake_feature)

    with pytest.raises(InvalidFeatureRegistryOperationError) as excinfo:
        feature_manager.remove_feature_registry(snowflake_feature)

    assert str(excinfo.value) == (
        f"Feature version {snowflake_feature.name} with version {snowflake_feature.version} cannot be deleted with "
        f"readiness PRODUCTION_READY"
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

    snowflake_feature.readiness = FeatureReadiness.PRODUCTION_READY.value
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
    snowflake_feature.readiness = FeatureReadiness.PRODUCTION_READY.value
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
    snowflake_feature.readiness = FeatureReadiness.PRODUCTION_READY.value
    with pytest.raises(MissingFeatureRegistryError) as excinfo:
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
    with pytest.raises(InvalidFeatureRegistryOperationError) as excinfo:
        feature_manager.online_enable(snowflake_feature)

    assert str(excinfo.value) == "feature readiness has to be PRODUCTION_READY before online_enable"


def test_online_enable_already_online_enabled(snowflake_feature, feature_manager):
    """
    Test online_enable already online_enabled
    """
    snowflake_feature.readiness = FeatureReadiness.PRODUCTION_READY.value
    feature_manager.insert_feature_registry(snowflake_feature)
    feature_manager.online_enable(snowflake_feature)

    with pytest.raises(InvalidFeatureRegistryOperationError) as excinfo:
        feature_manager.online_enable(snowflake_feature)

    assert (
        str(excinfo.value)
        == f"feature {snowflake_feature.name} with version {snowflake_feature.version} is already online enabled"
    )


def test_online_enable(
    snowflake_session,
    snowflake_feature,
    snowflake_feature_expected_tile_spec_dict,
    feature_manager,
):
    """
    Test online_enable
    """
    snowflake_feature.readiness = FeatureReadiness.PRODUCTION_READY.value
    feature_manager.insert_feature_registry(snowflake_feature)
    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "sum_30m"
    assert result.iloc[0]["VERSION"] == "v1"
    assert bool(result.iloc[0]["ONLINE_ENABLED"]) is False

    feature_manager.online_enable(snowflake_feature)

    tile_registry = snowflake_session.execute_query("SELECT * FROM TILE_REGISTRY")
    assert len(tile_registry) == 1
    expected_tile_id = snowflake_feature_expected_tile_spec_dict["tile_id"]
    assert tile_registry.iloc[0]["TILE_ID"] == expected_tile_id
    assert (
        tile_registry.iloc[0]["TILE_SQL"] == snowflake_feature_expected_tile_spec_dict["tile_sql"]
    )
    assert bool(tile_registry.iloc[0]["IS_ENABLED"]) is True

    result = snowflake_session.execute_query("SELECT * FROM FEATURE_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "sum_30m"
    assert result.iloc[0]["VERSION"] == "v1"
    assert bool(result.iloc[0]["ONLINE_ENABLED"]) is True

    tasks = snowflake_session.execute_query("SHOW TASKS")
    assert len(tasks) > 1
    assert tasks["name"].iloc[0] == f"SHELL_TASK_{expected_tile_id.upper()}_OFFLINE"
    assert tasks["schedule"].iloc[0] == "USING CRON 5 0 * * * UTC"
    assert tasks["state"].iloc[0] == "started"
    assert tasks["name"].iloc[1] == f"SHELL_TASK_{expected_tile_id.upper()}_ONLINE"
    assert tasks["schedule"].iloc[1] == "USING CRON 5-59/30 * * * * UTC"
    assert tasks["state"].iloc[1] == "started"


def test_get_last_tile_index(
    snowflake_feature,
    snowflake_feature_expected_tile_spec_dict,
    feature_manager,
    tile_manager,
):
    """
    Test get_last_tile_index
    """
    feature_manager.insert_feature_registry(snowflake_feature)

    tile_manager.insert_tile_registry(tile_spec=snowflake_feature.tile_specs[0])
    last_index_df = feature_manager.retrieve_last_tile_index(snowflake_feature)
    expected_tile_id = snowflake_feature_expected_tile_spec_dict["tile_id"]

    assert len(last_index_df) == 1
    assert last_index_df.iloc[0]["TILE_ID"] == expected_tile_id
    assert last_index_df.iloc[0]["LAST_TILE_INDEX_ONLINE"] == -1


def test_get_tile_monitor_summary(snowflake_feature, feature_manager, snowflake_session):
    """
    Test retrieve_feature_tile_inconsistency_data
    """
    feature_manager.insert_feature_registry(snowflake_feature)

    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    table_name = "TEMP_TABLE"
    tile_id = snowflake_feature.tile_specs[0].tile_id
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 95"
    monitor_tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 100"

    sql = (
        f"call SP_TILE_GENERATE('{tile_sql}', '{InternalName.TILE_START_DATE}', '{InternalName.TILE_LAST_START_DATE}', "
        f"183, 3, 5, '{entity_col_names}', '{value_col_names}', '{tile_id}', 'ONLINE', null)"
    )
    snowflake_session.execute_query(sql)

    sql = (
        f"call SP_TILE_MONITOR('{monitor_tile_sql}', '{InternalName.TILE_START_DATE}', 183, 3, 5, "
        f"'{entity_col_names}', '{value_col_names}', '{tile_id}', 'ONLINE')"
    )
    snowflake_session.execute_query(sql)
    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = snowflake_session.execute_query(sql)
    assert len(result) == 5

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5

    result = feature_manager.retrieve_feature_tile_inconsistency_data(
        query_start_ts="2022-06-05 15:43:00", query_end_ts="2022-06-05 16:03:00"
    )
    assert len(result) == 5
    expected_df = pd.DataFrame.from_dict(
        {
            "NAME": ["sum_30m", "sum_30m"],
            "VERSION": ["v1", "v1"],
            "TILE_ID": [tile_id, tile_id],
            "TILE_START_DATE": ["2022-06-05 16:03:00", "2022-06-05 15:58:00"],
        }
    )
    result_df = result[:2][
        [
            "NAME",
            "VERSION",
            "TILE_ID",
            "TILE_START_DATE",
        ]
    ]
    result_df["TILE_START_DATE"] = result_df["TILE_START_DATE"].dt.strftime("%Y-%m-%d %H:%M:%S")

    assert_frame_equal(expected_df, result_df)
