"""
Integration test for online enabling features
"""
import pytest

from featurebyte import EventView, FeatureList, ItemView
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.schema.feature_list import FeatureListGetOnlineFeatures


@pytest.fixture(name="online_enabled_feature_list", scope="module")
def online_enabled_feature_list_fixture(event_data, config):
    """
    Fixture for an online enabled feature

    To avoid side effects, this should not be shared with other tests.
    """
    event_view = EventView.from_event_data(event_data)
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
    feature_list.deploy(enable=True, make_production_ready=True)

    yield feature_list

    feature_list.deploy(enable=False, make_production_ready=False)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_enabled_features_have_scheduled_jobs(session, online_enabled_feature_list):
    """
    Test online enabled features have scheduled jobs
    """
    feature = online_enabled_feature_list[online_enabled_feature_list.feature_names[0]]
    tile_specs = ExtendedFeatureModel(**feature.dict()).tile_specs
    assert len(tile_specs) == 1
    aggregation_id = tile_specs[0].aggregation_id

    # There should be two scheduled jobs for the tile id - one for online tile and another for
    # offline tile (online store pre-computation job is triggered by the online tile task once tile
    # computation completes)
    tasks = await session.execute_query(f"SHOW TASKS LIKE '%{aggregation_id}%'")
    assert len(tasks) == 2


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_enable_non_time_aware_feature(item_data, config):
    """
    Test online enabling a non-time aware feature
    """
    item_view = ItemView.from_item_data(item_data)
    feature = item_view.groupby("order_id").aggregate(
        method="count", feature_name="my_item_feature_for_online_enable_test"
    )
    feature_list = FeatureList([feature], "my_non_time_aware_list")
    feature_list.save()

    try:
        feature_list.deploy(enable=True, make_production_ready=True)
        # Check feature request
        client = config.get_client()
        entity_serving_names = [{"order_id": "T1"}]
        data = FeatureListGetOnlineFeatures(entity_serving_names=entity_serving_names)
        res = client.post(
            f"/feature_list/{str(feature_list.id)}/online_features",
            json=data.json_dict(),
        )
    finally:
        feature_list.deploy(enable=False, make_production_ready=False)

    assert res.status_code == 200
    assert res.json() == {
        "features": [{"order_id": "T1", "my_item_feature_for_online_enable_test": 3}]
    }
