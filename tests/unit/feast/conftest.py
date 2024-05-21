"""
This module contains common fixtures for unit tests
"""

import datetime
from unittest.mock import patch

import pytest
import pytest_asyncio

from featurebyte import FeatureList, RequestColumn
from featurebyte.common.model_util import get_version
from featurebyte.feast.utils.registry_construction import FeastRegistryBuilder
from featurebyte.models.precomputed_lookup_feature_table import get_lookup_steps_unique_identifier
from tests.util.helper import get_relationship_info


@pytest.fixture(name="always_enable_feast_integration", autouse=True)
def always_enable_feast_integration_fixture(
    enable_feast_integration, patched_catalog_get_create_payload
):
    """Enable feast integration & patch catalog ID for all tests in this directory"""
    _ = enable_feast_integration, patched_catalog_get_create_payload
    yield


@pytest.fixture(name="mock_pymysql_connect", autouse=True)
def mock_pymysql_connect_fixture():
    """Mock pymysql.connect"""
    with patch("pymysql.connect") as mock_pymysql_connect:
        yield mock_pymysql_connect


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


@pytest.fixture(name="feature_list_features", autouse=True)
def feature_list_features_fixture(
    float_feature, non_time_based_feature, feature_without_entity, composite_feature_ttl_req_col
):
    """Fixture for the feature list features"""
    float_feature.save()
    non_time_based_feature.save()
    feature_without_entity.save()
    return [
        float_feature,
        non_time_based_feature,
        feature_without_entity,
        composite_feature_ttl_req_col,
    ]


@pytest.fixture(name="feature_list")
def feature_list_fixture(feature_list_features):
    """Fixture for the feature list"""
    feature_list = FeatureList(feature_list_features, name="test_feature_list")
    feature_list.save()
    return feature_list


@pytest_asyncio.fixture(name="entity_lookup_steps_mapping")
async def entity_lookup_steps_mapping_fixture(app_container, feature_list):
    """Fixture for entity_lookup_steps_mapping"""
    return await app_container.entity_lookup_feature_table_service.get_entity_lookup_steps_mapping(
        [feature_list.cached_model]
    )


@pytest.fixture(name="feast_registry_proto")
def feast_registry_proto_fixture(
    snowflake_feature_store,
    mysql_online_store,
    cust_id_entity,
    transaction_entity,
    feature_list_features,
    feature_list,
    entity_lookup_steps_mapping,
    mock_deployment_flow,
):
    """Fixture for the feast registry proto"""
    _ = mock_deployment_flow

    deployment = feature_list.deploy(make_production_ready=True, ignore_guardrails=True)
    deployment.enable()

    feast_registry_proto = FeastRegistryBuilder.create(
        feature_store=snowflake_feature_store.cached_model,
        online_store=mysql_online_store.cached_model,
        entities=[cust_id_entity.cached_model, transaction_entity.cached_model],
        features=[feature.cached_model for feature in feature_list_features],
        feature_lists=[feature_list.cached_model],  # type: ignore
        entity_lookup_steps_mapping=entity_lookup_steps_mapping,
        serving_entity_ids=feature_list.cached_model.primary_entity_ids,
    )
    return feast_registry_proto


@pytest.fixture(name="expected_entity_names")
def expected_entity_names_fixture():
    """Fixture for expected entity names"""
    return {"__dummy", "cust_id", "transaction_id"}


@pytest_asyncio.fixture(name="expected_cust_id_via_transaction_id_table_name")
async def expected_cust_id_via_transaction_id_table_name_fixture(
    app_container, transaction_entity, cust_id_entity
):
    """
    Fixture for the precomputed lookup feature table name that serves cust_id via transaction_id
    """
    transaction_to_customer_relationship_info = await get_relationship_info(
        app_container,
        child_entity_id=transaction_entity.id,
        parent_entity_id=cust_id_entity.id,
    )
    expected_suffix = get_lookup_steps_unique_identifier(
        [transaction_to_customer_relationship_info]
    )
    return f"cat1_cust_id_30m_via_transaction_id_{expected_suffix}"


@pytest.fixture(name="expected_data_source_names")
def expected_data_source_names_fixture(expected_cust_id_via_transaction_id_table_name):
    """Fixture for expected data source names"""
    return {
        "POINT_IN_TIME",
        "cat1__no_entity_1d",
        "cat1_cust_id_30m",
        "cat1_transaction_id_1d",
        expected_cust_id_via_transaction_id_table_name,
    }


@pytest.fixture(name="expected_feature_view_name_to_ttl")
def expected_feature_view_name_to_ttl_fixture(expected_cust_id_via_transaction_id_table_name):
    """Fixture for expected feature view name to TTL"""
    return {
        "cat1_transaction_id_1d": datetime.timedelta(seconds=0),
        "cat1__no_entity_1d": datetime.timedelta(days=2),
        "cat1_cust_id_30m": datetime.timedelta(seconds=3600),
        expected_cust_id_via_transaction_id_table_name: datetime.timedelta(seconds=3600),
    }


@pytest.fixture(name="expected_on_demand_feature_view_names")
def expected_on_demand_feature_view_names_fixture(
    float_feature, feature_without_entity, composite_feature_ttl_req_col
):
    """Fixture for expected on demand feature view names"""
    version = get_version().lower()
    return {
        f"odfv_composite_feature_ttl_req_col_{version}_{composite_feature_ttl_req_col.id}",
        f"odfv_count_1d_{version}_{feature_without_entity.id}",
        f"odfv_sum_1d_{version}_{float_feature.id}",
    }
