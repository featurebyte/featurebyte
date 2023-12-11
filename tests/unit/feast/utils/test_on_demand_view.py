"""
Test on demand feature view
"""
from featurebyte import RequestColumn
from featurebyte.feast.utils.on_demand_view import OnDemandFeatureViewConstructor


def test_create_on_demand_feature_view__ttl_and_non_ttl_and_request_col_components(
    float_feature, non_time_based_feature, latest_event_timestamp_feature
):
    """Test create on demand feature view (ttl and non ttl components)"""
    request_feature = (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day
    composite_feature = float_feature + non_time_based_feature + request_feature
    composite_feature.name = "composite_feature"
    composite_feature.save()

    feature_model = composite_feature.cached_model
    feature_model.initialize_offline_store_info(
        entity_id_to_serving_name={"cust_id": "cust_id", "transaction": "tx_id"}
    )
    assert feature_model.offline_store_info is not None
    OnDemandFeatureViewConstructor.create_or_none(
        offline_store_info=feature_model.offline_store_info,
        name_to_feast_feature_view={},
        name_to_feast_request_source={},
    )
