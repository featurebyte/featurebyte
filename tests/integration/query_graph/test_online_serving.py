import pandas as pd
import pytest

from featurebyte import EventView, FeatureList
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.models.online_store import OnlineFeatureSpec


@pytest.fixture(name="features", scope="session")
def features_fixture(event_data):
    """
    Fixture for feature
    """
    event_view = EventView.from_event_data(event_data)
    event_view["AMOUNT"].fillna(0)
    feature_group = event_view.groupby("USER ID").aggregate(
        "AMOUNT",
        method="sum",
        windows=["2h", "24h"],
        feature_names=["AMOUNT_SUM_2h", "AMOUNT_SUM_24h"],
    )
    features = [feature_group["AMOUNT_SUM_2h"], feature_group["AMOUNT_SUM_24h"]]
    for feature in features:
        feature.save()
    return features


async def update_online_store(session, feature_store, feature, feature_job_time_ts):
    """
    Trigger the SP_TILE_SCHEDULE_ONLINE_STORE with a fixed feature job time
    """
    # Manually update feature mapping table. Should only be done in test
    extended_feature_model = ExtendedFeatureModel(**feature.dict(), feature_store=feature_store)
    online_feature_spec = OnlineFeatureSpec(feature=extended_feature_model)
    feature_manager = FeatureManagerSnowflake(session)
    await feature_manager._update_tile_feature_mapping_table(online_feature_spec)

    # Trigger SP_TILE_SCHEDULE_ONLINE_STORE which will call the feature sql registered above
    tile_id = online_feature_spec.tile_ids[0]
    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{tile_id}', '{feature_job_time_ts}')"
    await session.execute_query(sql)


@pytest.mark.asyncio
async def test_online_serving_sql(features, snowflake_session, snowflake_feature_store):

    # Trigger tile compute. After get_historical_features, tiles should be already computed for the
    # following point in times.
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"] * 5),
            "user id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )
    feature_list = FeatureList(features)
    feature_list.get_historical_features(df_training_events)

    # Trigger SP_TILE_SCHEDULE_ONLINE_STORE to compute features and update online store
    feature_job_time = "2001-01-02 12:00:00"
    await update_online_store(
        snowflake_session, snowflake_feature_store, features[0], feature_job_time
    )
    await update_online_store(
        snowflake_session, snowflake_feature_store, features[1], feature_job_time
    )
