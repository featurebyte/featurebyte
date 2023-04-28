"""
Integration test for online store SQL generation
"""
import time
from unittest.mock import patch

import pandas as pd
import pytest

from featurebyte import FeatureList, RecordRetrievalException
from featurebyte.common.date_util import get_next_job_datetime
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.online_serving import get_online_store_retrieval_expr
from featurebyte.schema.feature_list import FeatureListGetOnlineFeatures
from tests.util.helper import create_batch_request_table_from_dataframe, fb_assert_frame_equal


@pytest.fixture(name="features", scope="session")
def features_fixture(event_table, source_type):
    """
    Fixture for feature
    """
    event_view = event_table.get_view()
    event_view["ÀMOUNT"].fillna(0)  # pylint: disable=no-member
    feature_group = event_view.groupby("ÜSER ID").aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["2h", "24h"],
        feature_names=["AMOUNT_SUM_2h", "AMOUNT_SUM_24h"],
    )
    feature_group_dict = event_view.groupby("ÜSER ID", category="PRODUCT_ACTION").aggregate_over(
        method="count",
        windows=["24h"],
        feature_names=["EVENT_COUNT_BY_ACTION_24h"],
    )
    # Use this feature group to test handling of empty entity column names list
    feature_without_entity = event_view.groupby([], category="PRODUCT_ACTION").aggregate_over(
        method="count",
        windows=["24h", "7d"],
        feature_names=["TOTAL_EVENT_COUNT_BY_ACTION_24h", "TOTAL_EVENT_COUNT_BY_ACTION_7d"],
    )
    feature_complex_1 = (
        feature_group["AMOUNT_SUM_24h"]
        * feature_group_dict["EVENT_COUNT_BY_ACTION_24h"].cd.entropy()
    )
    feature_complex_1.name = "COMPLEX_FEATURE_1"
    feature_complex_2 = feature_without_entity[
        "TOTAL_EVENT_COUNT_BY_ACTION_24h"
    ].cd.cosine_similarity(feature_group_dict["EVENT_COUNT_BY_ACTION_24h"])
    feature_complex_2.name = "COMPLEX_FEATURE_2"
    feature_complex_3 = feature_without_entity[
        "TOTAL_EVENT_COUNT_BY_ACTION_7d"
    ].cd.cosine_similarity(feature_group_dict["EVENT_COUNT_BY_ACTION_24h"])
    feature_complex_3.name = "COMPLEX_FEATURE_3"

    if source_type == "spark":
        features = [
            feature_group["AMOUNT_SUM_2h"],
            feature_group_dict["EVENT_COUNT_BY_ACTION_24h"],
        ]
    else:
        features = [
            feature_group["AMOUNT_SUM_2h"],
            feature_group["AMOUNT_SUM_24h"],
            feature_group_dict["EVENT_COUNT_BY_ACTION_24h"],
            feature_complex_1,
            feature_complex_2,
            feature_complex_3,
        ]

    for feature in features:
        feature.save()
    return features


@pytest.mark.parametrize("source_type", ["snowflake", "spark", "databricks"], indirect=True)
@pytest.mark.asyncio
async def test_online_serving_sql(
    features,
    session,
    config,
    data_source,
):
    """
    Test executing feature compute sql and feature retrieval SQL for online store
    """
    # pylint: disable=too-many-locals

    point_in_time = "2001-01-02 12:00:00"
    frequency = pd.Timedelta("1h").total_seconds()
    time_modulo_frequency = pd.Timedelta("30m").total_seconds()
    next_job_datetime = get_next_job_datetime(
        pd.Timestamp(point_in_time).to_pydatetime(),
        int(frequency // 60),
        time_modulo_frequency_seconds=int(time_modulo_frequency),
    )

    feature_list = FeatureList(features, name="My Online Serving Featurelist")
    columns = ["üser id"] + [feature.name for feature in features]
    # Deploy as at point_in_time (will trigger online and offline tile jobs using previous job time)
    feature_list.save()
    with patch(
        "featurebyte.feature_manager.manager.get_next_job_datetime",
        return_value=next_job_datetime,
    ):
        deployment = feature_list.deploy(make_production_ready=True)
        deployment.enable()
        assert deployment.enabled is True

    user_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -999]
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime([point_in_time] * len(user_ids)),
            "üser id": user_ids,
        }
    )
    df_historical = feature_list.compute_historical_features(df_training_events)

    try:
        # Run online store retrieval sql
        df_entities = pd.DataFrame({"üser id": user_ids})
        request_table_expr = construct_dataframe_sql_expr(df_entities, date_cols=[])
        feature_clusters = feature_list._get_feature_clusters()
        online_retrieval_expr = get_online_store_retrieval_expr(
            feature_clusters[0].graph,
            feature_clusters[0].nodes,
            source_type=session.source_type,
            request_table_columns=["üser id"],
            request_table_expr=request_table_expr,
        )
        online_retrieval_sql = sql_to_string(online_retrieval_expr, session.source_type)
        online_features = await session.execute_query(online_retrieval_sql)

        # Check result is expected
        assert set(online_features.columns.tolist()) == set(columns)
        fb_assert_frame_equal(
            df_historical[columns],
            online_features[columns],
            dict_like_columns=["EVENT_COUNT_BY_ACTION_24h"],
        )

        # Check online_features route
        check_online_features_route(feature_list, config, df_historical, columns)

        # check get batch features
        batch_request_table = await create_batch_request_table_from_dataframe(
            session=session,
            df=df_entities,
            data_source=data_source,
        )
        check_get_batch_features(
            data_source,
            deployment,
            batch_request_table,
            df_historical,
            columns,
        )

        # clear batch request table
        batch_request_table.delete()
        assert batch_request_table.saved is False

    finally:
        deployment.disable()
        assert deployment.enabled is False


def check_online_features_route(feature_list, config, df_historical, columns):
    """
    Online enable a feature and call the online features endpoint
    """
    client = config.get_client()

    user_ids = [5, -999]
    entity_serving_names = [{"üser id": user_id} for user_id in user_ids]
    data = FeatureListGetOnlineFeatures(entity_serving_names=entity_serving_names)

    tic = time.time()
    res = client.post(
        f"/feature_list/{str(feature_list.id)}/online_features",
        json=data.json_dict(),
    )
    assert res.status_code == 200

    df = pd.DataFrame(res.json()["features"])
    elapsed = time.time() - tic
    print(f"online_features elapsed: {elapsed:.6f}s")

    assert df.columns.tolist() == columns
    df_expected = df_historical[df_historical["üser id"].isin(user_ids)][columns].reset_index(
        drop=True
    )
    fb_assert_frame_equal(df_expected, df, dict_like_columns=["EVENT_COUNT_BY_ACTION_24h"])


def check_get_batch_features(data_source, deployment, batch_request_table, df_historical, columns):
    """
    Check get_batch_features_async
    """
    batch_feature_table = deployment.compute_batch_feature_table(
        batch_request_table=batch_request_table,
        batch_feature_table_name="batch_feature_table",
    )
    source_table = data_source.get_source_table(
        table_name=batch_feature_table.location.table_details.table_name,
        schema_name=batch_feature_table.location.table_details.schema_name,
        database_name=batch_feature_table.location.table_details.database_name,
    )
    preview_df = source_table.preview(limit=df_historical.shape[0])
    fb_assert_frame_equal(
        df_historical[columns],
        preview_df[columns],
        dict_like_columns=["EVENT_COUNT_BY_ACTION_24h"],
    )

    # delete batch feature table and check the materialized table is deleted
    batch_feature_table.delete()
    assert batch_feature_table.saved is False  # check mongo record is deleted

    # check preview deleted materialized table should raise RecordRetrievalException
    with pytest.raises(RecordRetrievalException):
        source_table.preview()
