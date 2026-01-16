"""Unit tests for feature decomposition logic."""

import freezegun
import pytest

from featurebyte.api.request_column import RequestColumn
from featurebyte.query_graph.transform.offline_store_ingest import (
    OfflineStoreIngestQueryGraphTransformer,
)


@pytest.fixture(name="scd_birthdate_feature")
def scd_birthdate_feature_fixture(snowflake_scd_table, cust_id_entity):
    """
    Fixture for an SCD lookup feature using date_of_birth column.
    Similar to CLIENT_BIRTHDATE feature in the user's code.
    """
    snowflake_scd_table["col_text"].as_entity(cust_id_entity.name)
    scd_view = snowflake_scd_table.get_view()
    grouped = scd_view.as_features(
        column_names=["date_of_birth"],
        feature_names=["CLIENT_BIRTHDATE"],
        offset=None,
    )
    return grouped["CLIENT_BIRTHDATE"]


@pytest.fixture(name="scd_age_feature")
def scd_age_feature_fixture(scd_birthdate_feature):
    """
    Fixture for an age calculation feature.
    Similar to CLIENT_Age feature in the user's code:
    ((request_col - birthdate).dt.day / 365.25).floor()
    """
    request_col = RequestColumn.point_in_time()
    age_feature = ((request_col - scd_birthdate_feature).dt.day / 365.25).floor()
    age_feature.name = "CLIENT_Age"
    return age_feature


@freezegun.freeze_time("2023-12-29")
def test_request_column_operations_decomposition(scd_age_feature):
    """
    Test decomposition with request column operations.
    """
    scd_age_feature.save()

    feature_model = scd_age_feature.cached_model
    transformer = OfflineStoreIngestQueryGraphTransformer(graph=feature_model.graph)
    output = transformer.transform(
        target_node=feature_model.node,
        relationships_info=feature_model.relationships_info,
        feature_name=feature_model.name,
        feature_version=feature_model.version.to_str(),
    )

    # Should be decomposed due to request column usage
    assert output.is_decomposed is True

    # check for deployment sql generation
    transformer = OfflineStoreIngestQueryGraphTransformer(graph=feature_model.graph)
    output = transformer.transform(
        target_node=feature_model.node,
        relationships_info=feature_model.relationships_info,
        feature_name=feature_model.name,
        feature_version=feature_model.version.to_str(),
        deployment_sql_generation=True,
    )

    # Point in time request column should not trigger decomposition for deployment SQL
    assert output.is_decomposed is False


def test_window_aggregate_feature_decomposition(float_feature):
    """
    Test decomposition with ttl window aggregate feature with request column.
    """
    request_and_ttl_component = (
        RequestColumn.point_in_time()
        + (RequestColumn.point_in_time() - RequestColumn.point_in_time())
    ).dt.day
    complex_feature = float_feature + request_and_ttl_component
    complex_feature.name = "feature"
    complex_feature.save()

    feature_model = complex_feature.cached_model
    transformer = OfflineStoreIngestQueryGraphTransformer(graph=feature_model.graph)
    output = transformer.transform(
        target_node=feature_model.node,
        relationships_info=feature_model.relationships_info,
        feature_name=feature_model.name,
        feature_version=feature_model.version.to_str(),
    )

    # Should be decomposed due to window aggregate usage
    assert output.is_decomposed is True

    # check for deployment sql generation
    transformer = OfflineStoreIngestQueryGraphTransformer(graph=feature_model.graph)
    output = transformer.transform(
        target_node=feature_model.node,
        relationships_info=feature_model.relationships_info,
        feature_name=feature_model.name,
        feature_version=feature_model.version.to_str(),
        deployment_sql_generation=True,
    )

    # Point in time request column and ttl aggregation should not trigger decomposition for deployment SQL
    assert output.is_decomposed is False
