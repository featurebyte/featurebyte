"""
Integration test for online enabling features
"""
import pytest

from featurebyte import FeatureList
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload


@pytest.fixture(name="online_enabled_feature_list", scope="module")
def online_enabled_feature_list_fixture(event_table, config):
    """
    Fixture for an online enabled feature

    To avoid side effects, this should not be shared with other tests.
    """

    event_view = event_table.get_view()
    event_view["ÀMOUNT"] = event_view["ÀMOUNT"] + 12345

    # Aggregate using a different entity than "ÜSER ID". Otherwise, it will be creating a feature
    # with the same online store table as the feature used in
    # tests/integration/query_graph/test_online_serving.py. That will cause that test to fail.
    feature_group = event_view.groupby("PRODUCT_ACTION").aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["24h"],
        feature_names=["FEATURE_FOR_ONLINE_ENABLE_TESTING"],
    )
    features = [feature_group["FEATURE_FOR_ONLINE_ENABLE_TESTING"]]
    for feature in features:
        feature.save()

    feature_list = FeatureList(
        features, name="My Feature List (tests/integration/api/test_feature.py)"
    )
    feature_list.save()
    deployment = feature_list.deploy(make_production_ready=True)
    deployment.enable()

    yield feature_list

    deployment.disable()


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_enable_non_time_aware_feature(item_table, config):
    """
    Test online enabling a non-time aware feature
    """
    item_view = item_table.get_view()
    feature = item_view.groupby("order_id").aggregate(
        method="count", feature_name="my_item_feature_for_online_enable_test"
    )
    feature_list = FeatureList([feature], "my_non_time_aware_list")
    feature_list.save()
    deployment = None

    try:
        deployment = feature_list.deploy(make_production_ready=True)
        deployment.enable()

        # Check feature request
        client = config.get_client()
        entity_serving_names = [{"order_id": "T1"}]
        data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)
        res = client.post(
            f"/deployment/{deployment.id}/online_features",
            json=data.json_dict(),
        )
    finally:
        if deployment:
            deployment.disable()

    assert res.status_code == 200
    assert res.json() == {
        "features": [{"order_id": "T1", "my_item_feature_for_online_enable_test": 3}]
    }
