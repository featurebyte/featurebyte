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
