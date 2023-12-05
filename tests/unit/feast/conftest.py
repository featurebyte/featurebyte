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
