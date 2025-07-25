"""
Integration test for online store SQL generation
"""

import asyncio
import copy
import threading
import time
from collections import defaultdict
from datetime import datetime
from queue import Queue
from unittest.mock import patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte import FeatureList
from featurebyte.enum import InternalName
from featurebyte.exception import RecordRetrievalException
from featurebyte.query_graph.sql.online_store_compute_query import (
    get_online_store_precompute_queries,
)
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from featurebyte.sql.tile_schedule_online_store import TileScheduleOnlineStore
from tests.util.helper import create_batch_request_table_from_dataframe, fb_assert_frame_equal


@pytest.fixture(name="features", scope="module")
def features_fixture(event_table, source_type):
    """
    Fixture for feature
    """
    event_view = event_table.get_view()
    event_view["ÀMOUNT"].fillna(0)
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
        value_column=None,
        method="count",
        windows=["24h"],
        feature_names=["EVENT_COUNT_BY_ACTION_24h"],
    )
    # Use this feature group to test handling of empty entity column names list
    feature_without_entity = event_view.groupby([], category="PRODUCT_ACTION").aggregate_over(
        value_column=None,
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

    feature_offset = event_view.groupby("ÜSER ID").aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["24h"],
        feature_names=["AMOUNT_SUM_24h_OFFSET_12h"],
        offset="12h",
    )["AMOUNT_SUM_24h_OFFSET_12h"]

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
            feature_offset,
        ]

    for feature in features:
        feature.save()
    return features


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

    # Simulate enabling the deployment at 2001-01-02 13:15:00. Based on the feature job settings
    # (frequency 1h, time modulo frequency 30m), the backfill process would compute online features
    # as at the expected previous job time (2001-01-02 12:30:00)
    schedule_time = "2001-01-02 13:15:00"
    point_in_time = "2001-01-02 12:30:00"

    feature_list = FeatureList(features, name="My Online Serving Featurelist")
    columns = ["üser id"] + [feature.name for feature in features]
    feature_list.save()
    with patch("featurebyte.service.feature_manager.datetime") as patched_datetime:
        patched_datetime.utcnow.return_value = pd.Timestamp(schedule_time).to_pydatetime()
        with patch(
            "featurebyte.service.feature_materialize.datetime"
        ) as feature_materialize_patched_datetime:
            feature_materialize_patched_datetime.utcnow.return_value = pd.Timestamp(
                schedule_time
            ).to_pydatetime()
            deployment = feature_list.deploy(make_production_ready=True)
            deployment.enable()
            time.sleep(1)  # sleep 1s to invalidate cache
            assert deployment.enabled is True

    # await sanity_check_online_store_tables(session, feature_list)

    # We can compute the historical features using the expected previous job time as point in time.
    # The historical feature values should match with the online feature values.
    user_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -999]
    df_training_events = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime([point_in_time] * len(user_ids)),
        "üser id": user_ids,
    })
    df_historical = feature_list.compute_historical_features(df_training_events)

    try:
        # Run online store retrieval sql
        df_entities = pd.DataFrame({"üser id": user_ids})

        # Check online_features route
        check_online_features_route(deployment, config, df_historical, columns)
        with patch("featurebyte.session.session_helper.NUM_FEATURES_PER_QUERY", 4):
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
        check_get_batch_features(
            deployment,
            batch_request_table,
            df_historical,
            columns,
            use_deployed_tile_tables=False,
        )
        with patch("featurebyte.session.session_helper.NUM_FEATURES_PER_QUERY", 4):
            check_get_batch_features(
                deployment,
                batch_request_table,
                df_historical,
                columns,
            )
        check_get_batch_features_feature_table(
            data_source,
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
            pd.Timestamp(schedule_time).strftime("%Y-%m-%d %H:%M:%S"),
            online_store_table_version_service_factory,
        )
    finally:
        deployment.disable()
        time.sleep(1)  # sleep 1s to invalidate cache
        assert deployment.enabled is False


def get_online_store_table_name_to_aggregation_id(feature_list):
    """
    Helper function to get a mapping from online store table name to aggregation ids
    """
    online_store_table_name_to_aggregation_id = defaultdict(set)
    for _, feature_object in feature_list.feature_objects.items():
        feature_model = feature_object.cached_model
        precompute_queries = get_online_store_precompute_queries(
            graph=feature_model.graph,
            node=feature_model.node,
            source_info=feature_model.get_source_info(),
            agg_result_name_include_serving_names=feature_model.agg_result_name_include_serving_names,
        )
        for query in precompute_queries:
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
    with patch("featurebyte.service.online_serving.datetime", autospec=True) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 13, 15)
        res = client.post(
            f"/deployment/{deployment.id}/online_features",
            json=data.json_dict(),
        )
    assert res.status_code == 200, res.json()

    df = pd.DataFrame(res.json()["features"])
    elapsed = time.time() - tic
    print(f"online_features elapsed: {elapsed:.6f}s")

    assert df["üser id"].tolist() == user_ids
    assert set(df.columns.tolist()) == set(columns)
    df_expected = df_historical[df_historical["üser id"].isin(user_ids)][columns].reset_index(
        drop=True
    )
    fb_assert_frame_equal(
        df_expected,
        df[df_expected.columns],
        dict_like_columns=["EVENT_COUNT_BY_ACTION_24h"],
        sort_by_columns=["üser id"],
    )


def check_get_batch_features(
    deployment, batch_request_table, df_historical, columns, use_deployed_tile_tables=True
):
    """
    Check get_batch_features_async
    """
    batch_feature_table = deployment.compute_batch_feature_table(
        batch_request_table=batch_request_table,
        batch_feature_table_name=f"batch_feature_table_{ObjectId()}",
        point_in_time="2001-01-02 13:15:00",
        use_deployed_tile_tables=use_deployed_tile_tables,
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


def check_get_batch_features_feature_table(
    data_source, deployment, batch_request_table, df_historical, columns
):
    """
    Check get_batch_features_async that appends to an existing table
    """
    input_table_details = batch_request_table.location.table_details
    table_details = copy.deepcopy(input_table_details)
    table_details.table_name = "BATCH_FEATURE_TABLE_APPEND_TO"
    input_table_name = f'"{input_table_details.database_name}"."{input_table_details.schema_name}"."{input_table_details.table_name}"'
    output_table_name = f'"{table_details.database_name}"."{table_details.schema_name}"."{table_details.table_name}"'
    deployment.compute_batch_features(
        batch_request_table=batch_request_table,
        output_table_name=output_table_name,
        output_table_snapshot_date="2001-01-02",
        output_table_snapshot_date_name="snapshot_dt",
        point_in_time="2001-01-02 13:15:00",
    )
    output_table = data_source.get_source_table(
        table_name=table_details.table_name,
        database_name=table_details.database_name,
        schema_name=table_details.schema_name,
    )
    preview_df = output_table.preview(limit=df_historical.shape[0])
    fb_assert_frame_equal(
        df_historical[columns],
        preview_df[columns],
        dict_like_columns=["EVENT_COUNT_BY_ACTION_24h"],
        sort_by_columns=["üser id"],
    )

    # check that the POINT_IN_TIME and snapshot_date columns are present
    assert "POINT_IN_TIME" in preview_df.columns
    assert "snapshot_dt" in preview_df.columns
    assert preview_df["POINT_IN_TIME"].iloc[0] == pd.Timestamp("2001-01-02 13:15:00")
    assert preview_df["snapshot_dt"].iloc[0] == "2001-01-02"

    # append to the existing table using fully qualified table name
    deployment.compute_batch_features(
        batch_request_table=input_table_name,
        output_table_name=output_table_name,
        output_table_snapshot_date="2001-01-03",
        output_table_snapshot_date_name="snapshot_dt",
        point_in_time="2001-01-03 13:15:00",
        columns=["üser id"],
    )
    preview_df = output_table.preview(limit=50)
    assert preview_df.shape[0] == df_historical.shape[0] * 2


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
            online_store_table_version_service=online_store_table_version_service_factory(),
        )
        try:
            await online_store_job.execute()
        except Exception as e:
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
