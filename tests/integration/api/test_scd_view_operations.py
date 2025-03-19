"""
Integration tests for SCDView
"""

import json

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId
from sqlglot import expressions

from featurebyte import Entity, FeatureJobSetting, FeatureList, SCDTable
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema, TimeZoneColumn
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from tests.util.helper import (
    assert_preview_result_equal,
    fb_assert_frame_equal,
    make_online_request,
    tz_localize_if_needed,
)


def get_expected_scd_join_result(
    df_left,
    df_right,
    left_key,
    right_key,
    left_timestamp,
    right_timestamp,
):
    """
    Perform an SCD join using in memory DataFrames
    """
    df_left = df_left.copy()
    df_right = df_right.copy()
    df_left[left_timestamp] = pd.to_datetime(df_left[left_timestamp], utc=True).dt.tz_localize(None)
    df_right[right_timestamp] = pd.to_datetime(df_right[right_timestamp], utc=True).dt.tz_localize(
        None
    )
    df_left = df_left.set_index(left_timestamp).sort_index()
    df_right = df_right.set_index(right_timestamp).sort_index()
    df_result = pd.merge_asof(
        df_left,
        df_right,
        left_index=True,
        right_index=True,
        left_by=left_key,
        right_by=right_key,
        allow_exact_matches=True,
    )
    return df_result


@pytest.fixture
def expected_dataframe_scd_join(transaction_data_upper_case, scd_dataframe):
    """
    Fixture for expected result when joining the transaction table with scd table
    """
    df = get_expected_scd_join_result(
        df_left=transaction_data_upper_case.copy(),
        df_right=scd_dataframe,
        left_key="ÜSER ID",
        right_key="User ID",
        left_timestamp="ËVENT_TIMESTAMP",
        right_timestamp="Effective Timestamp",
    )
    df = df.reset_index()
    return df


@pytest.mark.asyncio
async def test_scd_join_small(session, data_source, source_type, config):
    """
    Self-contained test case to test SCD with small datasets
    """
    df_events = pd.DataFrame({
        "ts": pd.to_datetime([
            "2022-04-10 10:00:00",
            "2022-04-15 10:00:00",
            "2022-04-20 10:00:00",
            "1970-01-01 00:00:00",  # To be modified as None later
        ]),
        "cust_id": [1000, 1000, 1000, 1000],
        "event_id": [1, 2, 3, 4],
    })
    df_scd = pd.DataFrame({
        "effective_ts": pd.to_datetime([
            "2022-04-12 10:00:00",
            "2022-04-20 10:00:00",
            "1970-01-01 00:00:00",  # To be modified as None later
        ]),
        "scd_cust_id": [1000, 1000, 1000],
        "scd_value": [1, 2, 3],
    })
    # Insert duplicate rows to ensure it can be handled (only one row should be joined)
    df_scd = pd.concat([df_scd, df_scd], ignore_index=True)
    df_expected = pd.DataFrame({
        "ts": pd.to_datetime([
            pd.NaT,
            "2022-04-10 10:00:00",
            "2022-04-15 10:00:00",
            "2022-04-20 10:00:00",
        ]),
        "cust_id": [1000, 1000, 1000, 1000],
        "event_id": [4, 1, 2, 3],
        "scd_value_latest": [np.nan, np.nan, 1, 2],
        "scd_value_latest_v2": [np.nan, np.nan, 1, 2],
    })
    table_prefix = "TEST_SCD_JOIN_SMALL"

    def _quote(col_name) -> str:
        return sql_to_string(quoted_identifier(col_name), source_type=session.source_type)

    # Register event table
    table_name = f"{table_prefix}_EVENT"
    await session.register_table(table_name, df_events)
    await session.execute_query(
        f'UPDATE {table_name} SET {_quote("ts")} = NULL WHERE {_quote("event_id")} = 4'
    )

    # Register scd table
    table_name = f"{table_prefix}_SCD"
    await session.register_table(table_name, df_scd)
    await session.execute_query(
        f'UPDATE {table_name} SET {_quote("effective_ts")} = NULL WHERE {_quote("scd_value")} = 3'
    )

    event_source_table = data_source.get_source_table(
        table_name=f"{table_prefix}_EVENT",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    event_table = event_source_table.create_event_table(
        name=f"{source_type}_{table_prefix}_EVENT_DATA",
        event_id_column="event_id",
        event_timestamp_column="ts",
    )
    event_view = event_table.get_view()
    scd_source_table = data_source.get_source_table(
        table_name=f"{table_prefix}_SCD",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    scd_table = scd_source_table.create_scd_table(
        name=f"{source_type}_{table_prefix}_SCD_DATA",
        natural_key_column="scd_cust_id",
        effective_timestamp_column="effective_ts",
    )
    scd_view = scd_table.get_view()
    event_view = event_view.join(scd_view, on="cust_id", rsuffix="_latest")
    event_view = event_view.join(scd_view, on="cust_id", rsuffix="_latest_v2")
    df_actual = event_view.preview()
    pd.testing.assert_frame_equal(df_actual, df_expected, check_dtype=False)

    client = config.get_client()
    response = client.get(f"/scd_table/{scd_table.id}")
    assert response.status_code == 200
    response_dict = response.json()
    assert response_dict["validation"].pop("updated_at") is not None
    assert response_dict["validation"] == {
        "status": "FAILED",
        "validation_message": "Multiple records found for the same effective timestamp and natural key combination. Examples of invalid natural keys: [1000, 1000, 1000]",
        "task_id": None,
    }


@pytest.mark.asyncio
async def test_feature_derived_from_multiple_scd_joins(session, data_source, source_type):
    """
    Test case to test feature derived from multiple SCD joins
    """
    df_events = pd.DataFrame({
        "ts": pd.to_datetime([
            "2022-04-10 10:00:00",
            "2022-04-15 10:00:00",
            "2022-04-20 10:00:00",
        ]),
        "event_id": [1, 2, 3],
        "event_type": ["A", "B", "A"],
        "account_id": [1000, 1000, 1000],
    })
    df_scd = pd.DataFrame({
        "effective_ts": pd.to_datetime(["2022-04-12 10:00:00", "2022-04-20 10:00:00"]),
        "account_id": [1000, 1000],
        "customer_id": ["c1", "c1"],
    })
    df_scd_2 = pd.DataFrame({
        "effective_ts": pd.to_datetime(["2022-04-12 10:00:00", "2022-04-20 10:00:00"]),
        "customer_id": ["c1", "c1"],
        "state_code": ["CA", "MA"],
    })
    table_prefix = str(ObjectId())
    await session.register_table(f"{table_prefix}_EVENT", df_events)
    await session.register_table(f"{table_prefix}_SCD", df_scd)
    await session.register_table(f"{table_prefix}_SCD_2", df_scd_2)
    event_source_table = data_source.get_source_table(
        table_name=f"{table_prefix}_EVENT",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    event_table = event_source_table.create_event_table(
        name=f"{source_type}_{table_prefix}_EVENT_DATA",
        event_id_column="event_id",
        event_timestamp_column="ts",
    )
    event_view = event_table.get_view()
    scd_source_table = data_source.get_source_table(
        table_name=f"{table_prefix}_SCD",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    scd_table = scd_source_table.create_scd_table(
        name=f"{source_type}_{table_prefix}_SCD_DATA",
        natural_key_column="account_id",
        effective_timestamp_column="effective_ts",
    )
    scd_source_table_2 = data_source.get_source_table(
        table_name=f"{table_prefix}_SCD_2",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    scd_table_2 = scd_source_table_2.create_scd_table(
        name=f"{source_type}_{table_prefix}_SCD_DATA_2",
        natural_key_column="customer_id",
        effective_timestamp_column="effective_ts",
    )

    state_entity = Entity.create(f"{table_prefix}_state", ["state_code"])
    scd_table_2["state_code"].as_entity(state_entity.name)

    customer_entity = Entity.create(f"{table_prefix}_customer", ["customer_id"])
    scd_table["customer_id"].as_entity(customer_entity.name)
    scd_table_2["customer_id"].as_entity(customer_entity.name)

    scd_view = scd_table.get_view()
    scd_view_2 = scd_table_2.get_view()
    event_view = event_view.join(scd_view[["account_id", "customer_id"]], on="account_id")
    event_view = event_view.join(scd_view_2[["customer_id", "state_code"]], on="customer_id")
    features = event_view.groupby("state_code", category="event_type").aggregate_over(
        None,
        "count",
        windows=["30d"],
        feature_names=["state_code_counts_30d"],
        feature_job_setting=FeatureJobSetting(period="24h", offset="1h", blind_spot="2h"),
    )
    df_observations = pd.DataFrame({
        "POINT_IN_TIME": ["2022-04-25 10:00:00"],
        "customer_id": ["c1"],
    })
    df = features.preview(df_observations)
    expected = df_observations.copy()
    expected["POINT_IN_TIME"] = pd.to_datetime(expected["POINT_IN_TIME"])
    expected["state_code_counts_30d"] = '{\n  "A": 1\n}'
    fb_assert_frame_equal(df, expected, dict_like_columns=["state_code_counts_30d"])


@pytest.mark.asyncio
async def test_end_timestamp_column(
    session, data_source, source_type, config, timestamp_format_string_with_time
):
    """
    Self-contained test case to test handling of end timestamp column
    """
    df_events = pd.DataFrame({
        "ts": pd.to_datetime([
            "2022-03-20 10:00:00",
            "2022-04-20 10:00:00",
            "2022-05-20 10:00:00",
            "2022-06-20 10:00:00",
        ]),
        "cust_id": [1000, 1000, 1000, 1000],
        "event_id": [1, 2, 3, 4],
    })
    df_scd = pd.DataFrame({
        "effective_ts": pd.to_datetime([
            "2022-03-01 10:00:00",
            "2022-04-01 10:00:00",
            "2022-05-01 10:00:00",
            "2022-06-01 10:00:00",
        ]),
        "end_ts": [
            "2022|04|01|10:00:00",
            "2022|05|01|10:00:00",
            "2022|05|05|10:00:00",
            None,
        ],
        "scd_cust_id": [1000, 1000, 1000, 1000],
        "scd_value": [1, 2, 3, 4],
    })
    table_prefix = "TEST_SCD_END_TIMESTAMP"

    # Register event table
    table_name = f"{table_prefix}_EVENT"
    await session.register_table(table_name, df_events)

    # Register scd table
    table_name = f"{table_prefix}_SCD"
    await session.register_table(table_name, df_scd)

    event_source_table = data_source.get_source_table(
        table_name=f"{table_prefix}_EVENT",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    event_table = event_source_table.create_event_table(
        name=f"{source_type}_{table_prefix}_EVENT_DATA",
        event_id_column="event_id",
        event_timestamp_column="ts",
    )
    event_view = event_table.get_view()
    scd_source_table = data_source.get_source_table(
        table_name=f"{table_prefix}_SCD",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    scd_table = scd_source_table.create_scd_table(
        name=f"{source_type}_{table_prefix}_SCD_DATA",
        natural_key_column="scd_cust_id",
        effective_timestamp_column="effective_ts",
        end_timestamp_column="end_ts",
        end_timestamp_schema=TimestampSchema(
            format_string=timestamp_format_string_with_time,
            is_utc_time=True,
        ),
    )

    entity = Entity.create(
        "test_end_timestamp_column_entity", ["test_end_timestamp_column_cust_id"]
    )
    scd_table["scd_cust_id"].as_entity(entity.name)

    # Check SCD joins. Note the 3rd row in the expected result is NaN because of the end timestamp.
    scd_view = scd_table.get_view()
    event_view = event_view.join(scd_view, on="cust_id", rsuffix="_latest")
    df_actual = event_view.preview()
    df_expected = pd.DataFrame({
        "ts": pd.to_datetime([
            "2022-03-20 10:00:00",
            "2022-04-20 10:00:00",
            "2022-05-20 10:00:00",
            "2022-06-20 10:00:00",
        ]),
        "cust_id": [1000, 1000, 1000, 1000],
        "event_id": [1, 2, 3, 4],
        "scd_value_latest": [1, 2, np.nan, 4],
    })
    fb_assert_frame_equal(df_actual, df_expected)

    # Check SCD lookup feature. Note the 3rd row in the expected result is NaN because of the end
    # timestamp.
    feature = scd_view["scd_value"].as_feature("test_end_timestamp_feature")
    feature.save()
    feature_list = FeatureList([feature], "my_list")
    df_observation = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime([
            "2022-03-20 10:00:00",
            "2022-04-20 10:00:00",
            "2022-05-20 10:00:00",
            "2022-06-20 10:00:00",
        ]),
        "test_end_timestamp_column_cust_id": [1000, 1000, 1000, 1000],
    })
    df_features = feature_list.compute_historical_features(df_observation)
    df_expected = df_observation.copy()
    df_expected["test_end_timestamp_feature"] = [1, 2, np.nan, 4]
    fb_assert_frame_equal(df_features, df_expected)


def test_event_view_join_scd_view__preview_view(
    event_table, scd_table, expected_dataframe_scd_join
):
    """
    Test joining an EventView with and SCDView
    """
    event_view = event_table.get_view()
    scd_view = scd_table.get_view()
    event_view = event_view.join(scd_view, on="ÜSER ID")
    df = event_view.preview(1000)
    df_expected = expected_dataframe_scd_join

    # Check correctness of joined view
    df["ËVENT_TIMESTAMP"] = pd.to_datetime(df["ËVENT_TIMESTAMP"], utc=True).dt.tz_localize(None)
    df_compare = df[["ËVENT_TIMESTAMP", "ÜSER ID", "User Status"]].merge(
        df_expected[["ËVENT_TIMESTAMP", "ÜSER ID", "User Status"]],
        on=["ËVENT_TIMESTAMP", "ÜSER ID"],
        suffixes=("_actual", "_expected"),
    )
    pd.testing.assert_series_equal(
        df_compare["User Status_actual"],
        df_compare["User Status_expected"],
        check_names=False,
    )


def test_event_view_join_scd_view__preview_feature(event_table, scd_table):
    """
    Test joining an EventView with and SCDView
    """
    event_view = event_table.get_view()
    scd_table = scd_table.get_view()
    event_view = event_view.join(scd_table, on="ÜSER ID")

    # Create a feature and preview it
    feature = event_view.groupby("ÜSER ID", category="User Status").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_names=["count_7d"],
    )["count_7d"]

    observation_set = pd.DataFrame([{"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 1}])
    df = feature.preview(observation_set)
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
        "üser id": 1,
        "count_7d": '{\n  "STÀTUS_CODE_34": 3,\n  "STÀTUS_CODE_39": 15\n}',
    }
    assert_preview_result_equal(df, expected, dict_like_columns=["count_7d"])

    feature_list = FeatureList([feature], name="my_scd_feature_list")
    df = feature_list.compute_historical_features(observation_set)
    fb_assert_frame_equal(df, pd.DataFrame([expected]), dict_like_columns=["count_7d"])


def test_scd_lookup_feature(
    config, event_table, dimension_table, scd_table, item_table, scd_dataframe, source_type
):
    """
    Test creating lookup feature from a SCDView
    """
    # This fixture makes user id a parent of item id
    _ = item_table

    # SCD lookup feature
    scd_view = scd_table.get_view()
    scd_lookup_feature = scd_view["User Status"].as_feature("Current User Status")

    # Dimension lookup feature
    dimension_view = dimension_table.get_view()
    dimension_lookup_feature = dimension_view["item_name"].as_feature("Item Name Feature")

    # Window feature that depends on an SCD join
    event_view = event_table.get_view()
    event_view = event_view.join(scd_view, on="ÜSER ID")
    window_feature = event_view.groupby("ÜSER ID", "User Status").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_names=["count_7d"],
    )["count_7d"]

    # Preview a feature list with above features
    feature_list = FeatureList(
        [window_feature, scd_lookup_feature, dimension_lookup_feature],
        "feature_list__test_scd_lookup_feature",
    )
    point_in_time = "2001-11-15 10:00:00"
    item_id = "item_42"
    user_id = 1
    preview_params = {
        "POINT_IN_TIME": point_in_time,
        "üser id": user_id,
        "item_id": item_id,
    }
    preview_output = feature_list.preview(pd.DataFrame([preview_params])).iloc[0].to_dict()
    assert set(preview_output.keys()) == {
        "POINT_IN_TIME",
        "üser id",
        "item_id",
        "count_7d",
        "Current User Status",
        "Item Name Feature",
    }

    # Compare with expected result
    mask = (scd_dataframe["Effective Timestamp"] <= point_in_time) & (
        scd_dataframe["User ID"] == user_id
    )
    expected_row = scd_dataframe[mask].sort_values("Effective Timestamp").iloc[-1]
    assert preview_output["Current User Status"] == expected_row["User Status"]
    assert preview_output["Item Name Feature"] == "name_42"
    assert preview_output["count_7d"] == json.loads(
        '{\n  "STÀTUS_CODE_34": 3,\n  "STÀTUS_CODE_39": 15\n}'
    )

    # Check online serving.
    feature_list.save()
    deployment = None
    try:
        deployment = feature_list.deploy(make_production_ready=True)
        deployment.enable()
        params = preview_params.copy()
        params.pop("POINT_IN_TIME")
        online_params = [{"item_id": item_id}]
        online_result = make_online_request(config.get_client(), deployment, online_params)
        online_result_dict = online_result.json()
        if online_result.status_code != 200:
            raise AssertionError(f"Online request failed: {online_result_dict}")
        assert online_result_dict["features"] == [
            {
                "item_id": "item_42",
                "Current User Status": "STÀTUS_CODE_26",
                "Item Name Feature": "name_42",
                "count_7d": None,
            }
        ]
    finally:
        if deployment:
            deployment.disable()


def test_scd_lookup_feature_with_offset(config, scd_table, scd_dataframe, source_type):
    """
    Test creating lookup feature from a SCDView with offset
    """
    # SCD lookup feature
    offset_1 = "90d"
    offset_2 = "7d"
    scd_view = scd_table.get_view()
    scd_lookup_feature_1 = scd_view["User Status"].as_feature(
        "Current User Status Offset 90d", offset=offset_1
    )
    scd_lookup_feature_2 = scd_view["User Status"].as_feature(
        "Current User Status Offset 7d", offset=offset_2
    )
    feature_list = FeatureList(
        [scd_lookup_feature_1, scd_lookup_feature_2],
        "feature_list__test_scd_lookup_feature_with_offset",
    )

    # Preview
    point_in_time = "2001-11-15 10:00:00"
    user_id = 1
    preview_params = {
        "POINT_IN_TIME": point_in_time,
        "üser id": user_id,
    }
    preview_output = feature_list.preview(pd.DataFrame([preview_params])).iloc[0].to_dict()

    # Compare with expected result
    def _get_expected_row(offset):
        mask = (
            pd.to_datetime(scd_dataframe["Effective Timestamp"], utc=True).dt.tz_localize(None)
            <= (pd.to_datetime(point_in_time) - pd.Timedelta(offset))
        ) & (scd_dataframe["User ID"] == user_id)
        expected_row = scd_dataframe[mask].sort_values("Effective Timestamp").iloc[-1]
        return expected_row

    assert (
        preview_output["Current User Status Offset 90d"]
        == _get_expected_row(offset_1)["User Status"]
    )
    assert (
        preview_output["Current User Status Offset 7d"]
        == _get_expected_row(offset_2)["User Status"]
    )

    # Check online serving
    feature_list.save()
    deployment = None
    try:
        deployment = feature_list.deploy(make_production_ready=True)
        deployment.enable()
        params = preview_params.copy()
        params.pop("POINT_IN_TIME")
        online_result = make_online_request(config.get_client(), deployment, [params])
        assert online_result.json()["features"] == [
            {
                "üser id": 1,
                "Current User Status Offset 90d": "STÀTUS_CODE_39",
                "Current User Status Offset 7d": "STÀTUS_CODE_39",
            }
        ]
    finally:
        deployment.disable()


def test_aggregate_asat(scd_table, scd_dataframe, source_type):
    """
    Test aggregate_asat aggregation on SCDView
    """
    scd_view = scd_table.get_view()
    feature_1 = scd_view.groupby("User Status").aggregate_asat(
        value_column=None, method="count", feature_name="Current Number of Users With This Status"
    )
    feature_2 = scd_view.groupby("User Status").aggregate_asat(
        value_column=None,
        method="count",
        feature_name="Current Number of Users With This Status 1d",
        offset="1d",
    )

    # check preview but provides children id
    feature_list = FeatureList([feature_1, feature_2], name="mylist")
    df = feature_list.preview(
        pd.DataFrame([
            {
                "POINT_IN_TIME": "2001-10-25 10:00:00",
                "üser id": 1,
            }
        ])
    )
    # databricks return POINT_IN_TIME with "Etc/UTC" timezone
    tz_localize_if_needed(df, source_type)
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-10-25 10:00:00"),
        "üser id": 1,
        "Current Number of Users With This Status": 1,
        "Current Number of Users With This Status 1d": 1,
    }
    assert df.iloc[0].equals(pd.Series(expected))

    # check preview
    df = feature_list.preview(
        pd.DataFrame([
            {
                "POINT_IN_TIME": "2001-10-25 10:00:00",
                "user_status": "STÀTUS_CODE_42",
            }
        ])
    )
    # databricks return POINT_IN_TIME with "Etc/UTC" timezone
    tz_localize_if_needed(df, source_type)
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-10-25 10:00:00"),
        "user_status": "STÀTUS_CODE_42",
        "Current Number of Users With This Status": 1,
        "Current Number of Users With This Status 1d": np.nan,
    }
    assert df.iloc[0].equals(pd.Series(expected))

    # check historical features
    observations_set = pd.DataFrame({
        "POINT_IN_TIME": pd.date_range("2001-01-10 10:00:00", periods=10, freq="1d"),
        "user_status": ["STÀTUS_CODE_47"] * 10,
    })
    expected = observations_set.copy()
    expected["Current Number of Users With This Status"] = [
        np.nan,
        1,
        2,
        2,
        1,
        1,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
    ]
    expected["Current Number of Users With This Status 1d"] = [
        np.nan,
        np.nan,
        1,
        2,
        2,
        1,
        1,
        np.nan,
        np.nan,
        np.nan,
    ]
    df = feature_list.compute_historical_features(observations_set)
    # databricks return POINT_IN_TIME with "Etc/UTC" timezone
    tz_localize_if_needed(df, source_type)
    pd.testing.assert_frame_equal(df, expected, check_dtype=False)


def test_aggregate_asat__no_entity(
    scd_table,
    scd_dataframe,
    config,
    source_type,
):
    """
    Test aggregate_asat aggregation on SCDView without entity
    """
    scd_view = scd_table.get_view()
    feature = scd_view.groupby([]).aggregate_asat(
        value_column=None, method="count", feature_name="Current Number of Users"
    )
    feature_other = scd_view.groupby("User Status").aggregate_asat(
        value_column=None,
        method="count",
        feature_name="Current Number of Users With This Status V2",
    )

    # check preview
    df = feature.preview(
        pd.DataFrame([
            {
                "POINT_IN_TIME": "2001-10-25 10:00:00",
            }
        ])
    )
    # databricks return POINT_IN_TIME with "Etc/UTC" timezone
    tz_localize_if_needed(df, source_type)
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-10-25 10:00:00"),
        "Current Number of Users": 9,
    }
    assert df.iloc[0].to_dict() == expected

    # check historical features
    feature_list = FeatureList([feature], "feature_list")
    observations_set = pd.DataFrame({
        "POINT_IN_TIME": pd.date_range("2001-01-10 10:00:00", periods=10, freq="1d"),
    })
    expected = observations_set.copy()
    expected["Current Number of Users"] = [8, 8, 9, 9, 9, 9, 9, 9, 9, 9]
    df = feature_list.compute_historical_features(observations_set)
    df = df.sort_values("POINT_IN_TIME").reset_index(drop=True)
    # databricks return POINT_IN_TIME with "Etc/UTC" timezone
    tz_localize_if_needed(df, source_type)
    pd.testing.assert_frame_equal(df, expected, check_dtype=False)

    # check online serving
    feature_list = FeatureList([feature, feature_other], "feature_list")
    feature_list.save()
    deployment = feature_list.deploy(make_production_ready=True)
    deployment.enable()

    try:
        data = OnlineFeaturesRequestPayload(
            entity_serving_names=[{"user_status": "STÀTUS_CODE_47"}]
        )
        res = config.get_client().post(
            f"/deployment/{deployment.id}/online_features",
            json=data.json_dict(),
        )
        assert res.status_code == 200
        assert res.json() == {
            "features": [
                {
                    "user_status": "STÀTUS_CODE_47",
                    "Current Number of Users With This Status V2": 2,
                    "Current Number of Users": 9,
                }
            ]
        }
    finally:
        deployment.disable()


def test_columns_joined_from_scd_view_as_groupby_keys(event_table, scd_table, source_type):
    """
    Test aggregate_over using a key column joined from another view
    """
    event_view = event_table.get_view()
    scd_view = scd_table.get_view()

    event_view = event_view.join(scd_view, on="ÜSER ID")

    feature = event_view.groupby("User Status").aggregate_over(
        value_column=None,
        method="count",
        windows=["30d"],
        feature_names=["count_30d"],
    )["count_30d"]
    feature_list = FeatureList([feature], "feature_list")

    # check preview
    preview_param = {
        "POINT_IN_TIME": "2002-01-01 10:00:00",
        "user_status": "STÀTUS_CODE_47",
    }

    df = feature_list.preview(pd.DataFrame([preview_param]))
    # databricks return POINT_IN_TIME with "Etc/UTC" timezone
    tz_localize_if_needed(df, source_type)
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2002-01-01 10:00:00"),
        "user_status": "STÀTUS_CODE_47",
        "count_30d": 7,
    }
    assert df.iloc[0].to_dict() == expected

    # check historical features
    observations_set = pd.DataFrame([preview_param])
    df = feature_list.compute_historical_features(observations_set)
    df = df.sort_values("POINT_IN_TIME").reset_index(drop=True)
    # databricks return POINT_IN_TIME with "Etc/UTC" timezone
    tz_localize_if_needed(df, source_type)
    assert df.iloc[0].to_dict() == expected


def test_scd_view_custom_date_format(scd_table_custom_date_format, source_type, config):
    """
    Test SCDView with custom date format
    """
    scd_view = scd_table_custom_date_format.get_view()
    feature_lookup = scd_view["User Status"].as_feature(
        "Current User Status",
    )
    feature_asat = scd_view.groupby("User Status").aggregate_asat(
        value_column=None, method="count", feature_name="Current Number of Users With This Status"
    )
    feature_list = FeatureList(
        [feature_lookup, feature_asat], name="test_scd_view_custom_date_format"
    )
    observations_set = pd.DataFrame({
        "POINT_IN_TIME": pd.date_range("2001-01-10 10:00:00", periods=10, freq="1d"),
        "üser id": [1] * 10,
        "user_status_2": ["STÀTUS_CODE_37"] * 10,
    })
    expected = observations_set.copy()
    expected["Current User Status"] = ["STÀTUS_CODE_0"] * 10
    expected["Current Number of Users With This Status"] = [
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
    ]
    df = feature_list.compute_historical_features(observations_set)
    # databricks return POINT_IN_TIME with "Etc/UTC" timezone
    tz_localize_if_needed(df, source_type)
    pd.testing.assert_frame_equal(df, expected, check_dtype=False)

    feature_list.save()
    deployment = None
    try:
        deployment = feature_list.deploy(make_production_ready=True)
        deployment.enable()
        online_params = [{"üser id": 1, "user_status_2": "STÀTUS_CODE_37"}]
        online_result = make_online_request(config.get_client(), deployment, online_params)
        online_result_dict = online_result.json()
        if online_result.status_code != 200:
            raise AssertionError(f"Online request failed: {online_result_dict}")
        assert online_result_dict["features"] == []
    finally:
        if deployment:
            deployment.disable()


@pytest.mark.parametrize(
    "date_value, timestamp_schema, expected",
    [
        (
            "2022-01-15",
            TimestampSchema(timezone="Etc/UTC"),
            pd.Timestamp("2022-01-16"),
        ),
        (
            "2022-01-15",
            TimestampSchema(timezone="Asia/Singapore"),
            pd.Timestamp("2022-01-16 00:00:00"),
        ),
    ],
)
@pytest.mark.asyncio
async def test_scd_view_date_type(
    session_without_datasets,
    data_source,
    date_value,
    timestamp_schema,
    expected,
):
    """
    Test SCDView with date type as effective timestamp column
    """
    session = session_without_datasets
    create_table_query = expressions.select(
        expressions.alias_(
            expressions.Date(this=make_literal_value(date_value)),
            "effective_timestamp_column",
            quoted=True,
        ),
        expressions.alias_(make_literal_value("user_1"), "user_id", quoted=True),
        expressions.alias_(make_literal_value(123), "value", quoted=True),
    )
    table_name = "test_scd_view_date_type_{}".format(ObjectId()).upper()
    await session.create_table_as(
        table_details=TableDetails(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=table_name,
        ),
        select_expr=create_table_query,
    )
    source_table = data_source.get_source_table(
        database_name=session.database_name, schema_name=session.schema_name, table_name=table_name
    )
    scd_table = SCDTable.create(
        source_table=source_table,
        name=table_name,
        natural_key_column="user_id",
        effective_timestamp_column="effective_timestamp_column",
        effective_timestamp_schema=timestamp_schema,
    )
    view = scd_table.get_view()
    df_preview = view.preview()
    actual = df_preview["effective_timestamp_column"].iloc[0]
    assert actual == expected


def test_timestamp_schema_validation(
    mock_task_manager,
    scd_data_tabular_source_custom_date_with_tz_format,
    scd_table_name_custom_date_with_tz_format,
    scd_table_timestamp_with_tz_format_string,
    timestamp_format_string,
    config,
):
    """Test timestamp schema validation for SCD table"""
    _ = mock_task_manager
    client = config.get_client()

    # create SCD table with timestamp schema containing timezone information
    scd_table = scd_data_tabular_source_custom_date_with_tz_format.create_scd_table(
        name=scd_table_name_custom_date_with_tz_format,
        natural_key_column="User ID",
        effective_timestamp_column="Effective Timestamp",
        surrogate_key_column="ID",
        effective_timestamp_schema=TimestampSchema(
            format_string="kkk",
            timezone="Asia/Singapore",  # invalid format string
        ),
    )

    # check table validation (expecting failure)
    response = client.get(f"/scd_table/{scd_table.id}")
    assert response.status_code == 200
    response_dict = response.json()
    assert response_dict["validation"] == {
        "status": "FAILED",
        "validation_message": response_dict["validation"]["validation_message"],
        "task_id": None,
        "updated_at": response_dict["validation"]["updated_at"],
    }
    assert (
        "Timestamp column 'Effective Timestamp' has invalid format string (kkk)."
        in response_dict["validation"]["validation_message"]
    )

    # clean up SCD table
    scd_table.delete()

    # create SCD table with timestamp schema without timezone information
    scd_table = scd_data_tabular_source_custom_date_with_tz_format.create_scd_table(
        name=scd_table_name_custom_date_with_tz_format,
        natural_key_column="User ID",
        effective_timestamp_column="Effective Timestamp",
        surrogate_key_column="ID",
        effective_timestamp_schema=TimestampSchema(
            format_string=scd_table_timestamp_with_tz_format_string
        ),
    )

    # check table validation
    response = client.get(f"/scd_table/{scd_table.id}")
    assert response.status_code == 200
    response_dict = response.json()
    assert response_dict["validation"] == {
        "status": "PASSED",
        "validation_message": None,
        "task_id": None,
        "updated_at": response_dict["validation"]["updated_at"],
    }
    scd_table.delete()

    # create SCD table with offset timezone information
    scd_table = scd_data_tabular_source_custom_date_with_tz_format.create_scd_table(
        name=scd_table_name_custom_date_with_tz_format,
        natural_key_column="User ID",
        effective_timestamp_column="effective_timestamp",
        surrogate_key_column="ID",
        effective_timestamp_schema=TimestampSchema(
            format_string=timestamp_format_string,
            timezone="America/New_York",
        ),
    )

    # check table validation
    response = client.get(f"/scd_table/{scd_table.id}")
    assert response.status_code == 200
    response_dict = response.json()
    assert response_dict["validation"] == {
        "status": "PASSED",
        "validation_message": None,
        "task_id": None,
        "updated_at": response_dict["validation"]["updated_at"],
    }
    scd_table.delete()

    # create SCD table with offset timezone information
    scd_table = scd_data_tabular_source_custom_date_with_tz_format.create_scd_table(
        name=scd_table_name_custom_date_with_tz_format,
        natural_key_column="User ID",
        effective_timestamp_column="effective_timestamp",
        surrogate_key_column="ID",
        effective_timestamp_schema=TimestampSchema(
            format_string=timestamp_format_string,
            timezone=TimeZoneColumn(column_name="timezone_offset", type="offset"),
        ),
    )

    # check table validation
    response = client.get(f"/scd_table/{scd_table.id}")
    assert response.status_code == 200
    response_dict = response.json()
    assert response_dict["validation"] == {
        "status": "PASSED",
        "validation_message": None,
        "task_id": None,
        "updated_at": response_dict["validation"]["updated_at"],
    }
    scd_table.delete()

    # check invalid timezone column
    scd_table = scd_data_tabular_source_custom_date_with_tz_format.create_scd_table(
        name=scd_table_name_custom_date_with_tz_format,
        natural_key_column="User ID",
        effective_timestamp_column="effective_timestamp",
        surrogate_key_column="ID",
        effective_timestamp_schema=TimestampSchema(
            format_string=timestamp_format_string,
            timezone=TimeZoneColumn(column_name="invalid_timezone_offset", type="offset"),
        ),
    )

    # check table validation
    response = client.get(f"/scd_table/{scd_table.id}")
    assert response.status_code == 200
    response_dict = response.json()
    assert response_dict["validation"] == {
        "status": "FAILED",
        "validation_message": response_dict["validation"]["validation_message"],
        "task_id": None,
        "updated_at": response_dict["validation"]["updated_at"],
    }
    assert (
        "Timestamp column 'effective_timestamp' has invalid timezone (column_name='invalid_timezone_offset' type='offset')."
        in response_dict["validation"]["validation_message"]
    )
    scd_table.delete()
