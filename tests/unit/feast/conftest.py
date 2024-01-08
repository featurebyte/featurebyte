"""
This module contains common fixtures for unit tests
"""
import pytest

from featurebyte import FeatureList, RequestColumn
from featurebyte.feast.utils.registry_construction import FeastRegistryConstructor


@pytest.fixture(name="always_enable_feast_integration", autouse=True)
def always_enable_feast_integration_fixture(
    enable_feast_integration, patched_catalog_get_create_payload
):
    """Enable feast integration & patch catalog ID for all tests in this directory"""
    _ = enable_feast_integration, patched_catalog_get_create_payload
    yield


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
        feature_lists=[feature_list.cached_model],  # type: ignore
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


@pytest.fixture(name="composite_feature_ttl_req_col")
def composite_feature_ttl_req_col_fixture(
    latest_event_timestamp_feature, float_feature, non_time_based_feature
):
    """
    Fixture for a composite feature with TTL and required column
    """
    request_feature = (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day
    composite_feature = float_feature + non_time_based_feature + request_feature
    composite_feature.name = "composite_feature_ttl_req_col"
    composite_feature.save()
    return composite_feature
