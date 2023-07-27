"""
Integration test for online store SQL generation
"""
import asyncio
import threading
import time
from collections import defaultdict
from queue import Queue
from unittest.mock import patch

import pandas as pd
import pytest

from featurebyte import FeatureList, RecordRetrievalException
from featurebyte.common.date_util import get_next_job_datetime
from featurebyte.enum import InternalName
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.online_store import OnlineFeatureSpec
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from featurebyte.sql.tile_schedule_online_store import TileScheduleOnlineStore
from tests.util.helper import create_batch_request_table_from_dataframe, fb_assert_frame_equal


@pytest.fixture(name="features", scope="module")
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
    feature_group_2 = event_view.groupby("ÜSER ID").aggregate_over(
        "ÀMOUNT",
        method="max",
        windows=["48h"],
        feature_names=["AMOUNT_MAX_48h"],
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
            feature_group_2["AMOUNT_MAX_48h"],
            feature_group_dict["EVENT_COUNT_BY_ACTION_24h"],
        ]
    else:
        features = [
            feature_group["AMOUNT_SUM_2h"],
            feature_group["AMOUNT_SUM_24h"],
            feature_group_2["AMOUNT_MAX_48h"],
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
    get_session_callback,
    online_store_table_version_service_factory,
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
        "featurebyte.service.feature_manager.get_next_job_datetime",
        return_value=next_job_datetime,
    ):
        deployment = feature_list.deploy(make_production_ready=True)
        deployment.enable()
        assert deployment.enabled is True

    await sanity_check_online_store_tables(session, feature_list)

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

        # Check online_features route
        check_online_features_route(deployment, config, df_historical, columns)

        # check get batch features
        batch_request_table = await create_batch_request_table_from_dataframe(
            session=session,
            df=df_entities,
            data_source=data_source,
        )
        check_get_batch_features(
            deployment,
            batch_request_table,
            df_historical,
            columns,
        )

        # clear batch request table
        batch_request_table.delete()
        assert batch_request_table.saved is False

        await check_concurrent_online_store_table_updates(
            get_session_callback,
            feature_list,
            next_job_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            online_store_table_version_service_factory,
        )
    finally:
        deployment.disable()
        assert deployment.enabled is False


def get_online_feature_spec(feature):
    """
    Helper function to create an online feature spec from a feature
    """
    return OnlineFeatureSpec(feature=ExtendedFeatureModel(**feature.dict(by_alias=True)))


def get_online_store_table_name_to_aggregation_id(feature_list):
    """
    Helper function to get a mapping from online store table name to aggregation ids
    """
    online_store_table_name_to_aggregation_id = defaultdict(set)
    for _, feature_object in feature_list.feature_objects.items():
        for query in get_online_feature_spec(feature_object).precompute_queries:
            online_store_table_name_to_aggregation_id[query.table_name].add(query.aggregation_id)
    return online_store_table_name_to_aggregation_id


async def sanity_check_online_store_tables(session, feature_list):
    """
    Verify that online enabling related features do not trigger redundant online store updates
    """
    table_names = list(get_online_store_table_name_to_aggregation_id(feature_list).keys())
    for table_name in table_names:
        df = await session.execute_query(
            f"SELECT MAX({InternalName.ONLINE_STORE_VERSION_COLUMN}) AS OUT FROM {table_name}"
        )
        max_version = df.iloc[0]["OUT"]
        # Verify that all results are at the very first version. If that is not the case, some
        # results were calculated more than once.
        assert max_version == 0


def check_online_features_route(deployment, config, df_historical, columns):
    """
    Online enable a feature and call the online features endpoint
    """
    client = config.get_client()

    user_ids = [-999, 5]
    entity_serving_names = [{"üser id": user_id} for user_id in user_ids]
    data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)

    tic = time.time()
    res = client.post(
        f"/deployment/{deployment.id}/online_features",
        json=data.json_dict(),
    )
    assert res.status_code == 200

    df = pd.DataFrame(res.json()["features"])
    elapsed = time.time() - tic
    print(f"online_features elapsed: {elapsed:.6f}s")

    assert df["üser id"].tolist() == user_ids
    assert df.columns.tolist() == columns
    df_expected = df_historical[df_historical["üser id"].isin(user_ids)][columns].reset_index(
        drop=True
    )
    fb_assert_frame_equal(
        df_expected,
        df,
        dict_like_columns=["EVENT_COUNT_BY_ACTION_24h"],
        sort_by_columns=["üser id"],
    )


def check_get_batch_features(deployment, batch_request_table, df_historical, columns):
    """
    Check get_batch_features_async
    """
    batch_feature_table = deployment.compute_batch_feature_table(
        batch_request_table=batch_request_table,
        batch_feature_table_name="batch_feature_table",
    )
    preview_df = batch_feature_table.preview(limit=df_historical.shape[0])
    fb_assert_frame_equal(
        df_historical[columns],
        preview_df[columns],
        dict_like_columns=["EVENT_COUNT_BY_ACTION_24h"],
        sort_by_columns=["üser id"],
    )

    # delete batch feature table and check the materialized table is deleted
    batch_feature_table.delete()
    assert batch_feature_table.saved is False  # check mongo record is deleted

    # check preview deleted materialized table should raise RecordRetrievalException
    with pytest.raises(RecordRetrievalException):
        batch_feature_table.preview()


async def check_concurrent_online_store_table_updates(
    get_session_callback,
    feature_list,
    job_schedule_ts_str,
    online_store_table_version_service_factory,
):
    """
    Test concurrent online store table updates
    """
    # Find concurrent online store table updates given a FeatureList. Concurrent online store table
    # updates occur when tile jobs associated with different aggregation ids write to the same
    # online store table. The logic below finds such a table and the associated aggregation ids.
    online_store_table_name_to_aggregation_id = get_online_store_table_name_to_aggregation_id(
        feature_list
    )

    table_with_concurrent_updates = None
    for table_name, agg_ids in online_store_table_name_to_aggregation_id.items():
        if len(agg_ids) > 1:
            table_with_concurrent_updates = table_name
            break
    if table_with_concurrent_updates is None:
        raise AssertionError("No table with concurrent updates found")

    aggregation_ids = list(online_store_table_name_to_aggregation_id[table_with_concurrent_updates])

    async def run(aggregation_id, out):
        """
        Trigger online store updates for the given aggregation_id
        """
        session = await get_session_callback()
        online_store_job = TileScheduleOnlineStore(
            session=session,
            aggregation_id=aggregation_id,
            job_schedule_ts_str=job_schedule_ts_str,
            retry_num=1,  # no issue even without retry
            online_store_table_version_service=online_store_table_version_service_factory(),
        )
        try:
            await online_store_job.execute()
        except Exception as e:  # pylint: disable=broad-exception-caught
            out.put(e)

    out = Queue()

    for _ in range(3):
        threads = []
        for aggregation_id in aggregation_ids:
            t = threading.Thread(target=asyncio.run, args=(run(aggregation_id, out),))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        if not out.empty():
            raise out.get()
