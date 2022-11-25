"""
Integration test for online store SQL generation
"""
import time

import pandas as pd
import pytest

from featurebyte import EventView, FeatureList
from featurebyte.enum import SourceType
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_feature import FeatureManagerSnowflake
from featurebyte.models.online_store import OnlineFeatureSpec
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.online_serving import get_online_store_retrieval_sql
from featurebyte.schema.feature_list import FeatureListGetOnlineFeatures


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


async def update_online_store(session, feature, feature_job_time_ts):
    """
    Trigger the SP_TILE_SCHEDULE_ONLINE_STORE with a fixed feature job time
    """
    # Manually update feature mapping table. Should only be done in test
    extended_feature_model = ExtendedFeatureModel(**feature.dict())
    online_feature_spec = OnlineFeatureSpec(feature=extended_feature_model)
    feature_manager = FeatureManagerSnowflake(session)
    await feature_manager._update_tile_feature_mapping_table(online_feature_spec)

    # Trigger SP_TILE_SCHEDULE_ONLINE_STORE which will call the feature sql registered above
    tile_id = online_feature_spec.tile_ids[0]
    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{tile_id}', '{feature_job_time_ts}')"
    await session.execute_query(sql)


@pytest.mark.asyncio
async def test_online_serving_sql(features, snowflake_session, config):
    """
    Test executing feature compute sql and feature retrieval SQL for online store
    """

    # Trigger tile compute. After get_historical_features, tiles should be already computed for the
    # provided point in time
    feature_job_time = "2001-01-02 12:00:00"
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime([feature_job_time] * 10),
            "user id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )
    feature_list = FeatureList(features, name="My Online Serving Featurelist")
    df_historical = feature_list.get_historical_features(df_training_events)

    # Trigger SP_TILE_SCHEDULE_ONLINE_STORE to compute features and update online store
    await update_online_store(snowflake_session, features[0], feature_job_time)
    await update_online_store(snowflake_session, features[1], feature_job_time)

    # Run online store retrieval sql
    df_entities = pd.DataFrame({"user id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    request_table_expr = construct_dataframe_sql_expr(df_entities, date_cols=[])
    feature_clusters = feature_list._get_feature_clusters()
    online_retrieval_sql = get_online_store_retrieval_sql(
        feature_clusters[0].graph,
        feature_clusters[0].nodes,
        source_type=SourceType.SNOWFLAKE,
        request_table_columns=["user id"],
        request_table_expr=request_table_expr,
    )
    online_features = await snowflake_session.execute_query(online_retrieval_sql)

    # Check result is expected
    columns = ["user id", "AMOUNT_SUM_2h", "AMOUNT_SUM_24h"]
    assert online_features.columns.tolist() == columns
    pd.testing.assert_frame_equal(df_historical[columns], online_features[columns])

    # Check online_features route
    feature_list.save()
    feature_list.deploy(make_production_ready=True, enable=True)
    client = config.get_client()
    payload = FeatureListGetOnlineFeatures(entity_serving_names=[{"user id": 5}])

    tic = time.time()
    res = client.post(
        f"/feature_list/{str(feature_list.id)}/online_features",
        data={"payload": payload.json()},
    )
    df = pd.DataFrame(res.json()["features"])
    elapsed = time.time() - tic
    print(f"online_features elapsed: {elapsed:.6f}s")
    df_expected = df_historical[df_historical["user id"] == 5][columns].reset_index(drop=True)
    pd.testing.assert_frame_equal(df_expected, df, check_dtype=False)
