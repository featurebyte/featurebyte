"""
Integration test for online enabling features
"""
import pytest

from featurebyte import EventView, FeatureList
from featurebyte.feature_manager.model import ExtendedFeatureModel


@pytest.fixture(name="online_enabled_feature_list", scope="module")
def online_enabled_feature_list_fixture(event_data, config):
    """
    Fixture for an online enabled feature

    To avoid side effects, this should not be shared with other tests.
    """
    event_view = EventView.from_event_data(event_data)
    event_view["AMOUNT"] = event_view["AMOUNT"] + 12345

    # Aggregate using a different entity than "USER ID". Otherwise, it will be creating a feature
    # with the same online store table as the feature used in
    # tests/integration/query_graph/test_online_serving.py. That will cause that test to fail.
    feature_group = event_view.groupby("PRODUCT_ACTION").aggregate_over(
        "AMOUNT",
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

    feature_list.deploy(enable=False, make_production_ready=True)


@pytest.mark.asyncio
async def test_online_enabled_features_have_scheduled_jobs(
    snowflake_session, online_enabled_feature_list
):
    """
    Test online enabled features have scheduled jobs
    """
    feature = online_enabled_feature_list[online_enabled_feature_list.feature_names[0]]
    tile_specs = ExtendedFeatureModel(**feature.dict()).tile_specs
    assert len(tile_specs) == 1
    tile_id = tile_specs[0].tile_id

    # There should be two scheduled jobs for the tile id - one for online tile and another for
    # offline tile (online store pre-computation job is triggered by the online tile task once tile
    # computation completes)
    tasks = await snowflake_session.execute_query(f"SHOW TASKS LIKE '%{tile_id}%'")
    assert len(tasks) == 1
