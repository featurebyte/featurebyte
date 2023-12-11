"""
This module contains common fixtures for unit tests
"""
import pytest

from featurebyte import FeatureList
from featurebyte.feast.utils.registry_construction import FeastRegistryConstructor


@pytest.fixture(name="feature_list")
def feature_list_fixture(float_feature, non_time_based_feature):
    """Fixture for the feature list"""
    float_feature.save()
    feature_list = FeatureList([float_feature, non_time_based_feature], name="test_feature_list")
    feature_list.save()
    return feature_list


@pytest.fixture(name="feast_registry_proto")
def feast_registry_proto_fixture(
    snowflake_feature_store,
    cust_id_entity,
    transaction_entity,
    float_feature,
    non_time_based_feature,
    feature_list,
):
    """Fixture for the feast registry proto"""
    feast_registry_proto = FeastRegistryConstructor.create(
        feature_store=snowflake_feature_store.cached_model,
        entities=[cust_id_entity.cached_model, transaction_entity.cached_model],
        features=[float_feature.cached_model, non_time_based_feature.cached_model],
        feature_lists=[feature_list],
    )
    return feast_registry_proto


@pytest.fixture(name="latest_event_timestamp_feature")
def latest_event_timestamp_feature_fixture(
    snowflake_event_view_with_entity, feature_group_feature_job_setting
):
    """
    Fixture for a timestamp feature
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="event_timestamp",
        method="latest",
        windows=["90d"],
        feature_names=["latest_event_timestamp_90d"],
        feature_job_setting=feature_group_feature_job_setting,
    )["latest_event_timestamp_90d"]
    return feature
