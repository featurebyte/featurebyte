"""
This module contains integration tests for FeatureListManagerSnowflake
"""
import json

import pandas as pd
from pandas.testing import assert_frame_equal

from featurebyte.models.feature import FeatureReadiness


def test_insert_feature_list_registry(
    snowflake_session, snowflake_feature_list, feature_list_manager
):
    """
    Test insert_feature_list_registry
    """
    flag = feature_list_manager.insert_feature_list_registry(snowflake_feature_list)
    assert flag is True

    result = snowflake_session.execute_query("SELECT * FROM FEATURE_LIST_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "feature_list1"
    assert result.iloc[0]["VERSION"] == "v1"

    expected_df = pd.DataFrame.from_dict(
        {
            "NAME": ["feature_list1"],
            "VERSION": ["v1"],
            "READINESS": ["DRAFT"],
            "STATUS": ["DRAFT"],
        }
    )
    result_df = result[
        [
            "NAME",
            "VERSION",
            "READINESS",
            "STATUS",
        ]
    ]
    assert_frame_equal(expected_df, result_df)

    expected_fv = {"feature": "sum_30m", "version": "v1"}
    result_fv = json.loads(result["FEATURE_VERSIONS"].iloc[0])[0]
    assert expected_fv == result_fv


def test_retrieve_feature_list_registry(snowflake_feature_list, feature_list_manager):
    """
    Test retrieve_features
    """
    feature_list_manager.insert_feature_list_registry(snowflake_feature_list)
    f_reg_df = feature_list_manager.retrieve_feature_list_registries(snowflake_feature_list)
    assert len(f_reg_df) == 1
    assert f_reg_df.iloc[0]["NAME"] == "feature_list1"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["READINESS"] == "DRAFT"

    f_reg_df = feature_list_manager.retrieve_feature_list_registries(
        feature_list=snowflake_feature_list, version="v1"
    )
    assert len(f_reg_df) == 1
    assert f_reg_df.iloc[0]["NAME"] == "feature_list1"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["READINESS"] == "DRAFT"


def test_retrieve_feature_list_registry_multiple(snowflake_feature_list, feature_list_manager):
    """
    Test retrieve_features return multiple features
    """
    feature_list_manager.insert_feature_list_registry(snowflake_feature_list)

    snowflake_feature_list.version = "v2"
    snowflake_feature_list.readiness = FeatureReadiness.PRODUCTION_READY
    feature_list_manager.insert_feature_list_registry(snowflake_feature_list)

    f_reg_df = feature_list_manager.retrieve_feature_list_registries(snowflake_feature_list)
    assert len(f_reg_df) > 1
    assert f_reg_df.iloc[0]["NAME"] == "feature_list1"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[0]["READINESS"] == "DRAFT"
    assert f_reg_df.iloc[1]["NAME"] == "feature_list1"
    assert f_reg_df.iloc[1]["VERSION"] == "v2"
    assert f_reg_df.iloc[1]["READINESS"] == "PRODUCTION_READY"


def test_update_feature_list_registry(
    snowflake_session, snowflake_feature_list, feature_list_manager
):
    """
    Test update_feature_registry
    """
    feature_list_manager.insert_feature_list_registry(snowflake_feature_list)
    result = snowflake_session.execute_query("SELECT * FROM FEATURE_LIST_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "feature_list1"
    assert result.iloc[0]["VERSION"] == "v1"
    assert result.iloc[0]["READINESS"] == "DRAFT"

    feature_list_manager.update_feature_list_registry(
        snowflake_feature_list,
        attribute_name="readiness",
        attribute_value=FeatureReadiness.PRODUCTION_READY,
    )
    result = snowflake_session.execute_query("SELECT * FROM FEATURE_LIST_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "feature_list1"
    assert result.iloc[0]["VERSION"] == "v1"
    assert result.iloc[0]["READINESS"] == "PRODUCTION_READY"
