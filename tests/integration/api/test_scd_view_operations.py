"""
Integration tests for SCDView
"""
import numpy as np
import pandas as pd
import pytest

from featurebyte import (
    DimensionView,
    EventData,
    EventView,
    FeatureList,
    SlowlyChangingData,
    SlowlyChangingView,
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
    Fixture for expected result when joining the transaction data with scd data
    """
    df = get_expected_scd_join_result(
        df_left=transaction_data_upper_case,
        df_right=scd_dataframe,
        left_key="USER ID",
        right_key="User ID",
        left_timestamp="EVENT_TIMESTAMP",
        right_timestamp="Effective Timestamp",
    )
    df = df.reset_index()
    return df


@pytest.mark.asyncio
async def test_scd_join_small(snowflake_session, snowflake_feature_store):
    """
    Self-contained test case to test SCD with small datasets
    """
    df_events = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2022-04-10 10:00:00",
                    "2022-04-15 10:00:00",
                    "2022-04-20 10:00:00",
                ]
            ),
            "cust_id": [1000, 1000, 1000],
            "event_id": [1, 2, 3],
        }
    )
    df_scd = pd.DataFrame(
        {
            "effective_ts": pd.to_datetime(["2022-04-12 10:00:00", "2022-04-20 10:00:00"]),
            "scd_cust_id": [1000, 1000],
            "scd_value": [1, 2],
        }
    )
    df_expected = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2022-04-10 10:00:00",
                    "2022-04-15 10:00:00",
                    "2022-04-20 10:00:00",
                ]
            ),
            "cust_id": [1000, 1000, 1000],
            "event_id": [1, 2, 3],
            "effective_ts_latest": pd.to_datetime(
                [
                    np.nan,
                    "2022-04-12 10:00:00",
                    "2022-04-20 10:00:00",
                ]
            ),
            "scd_value_latest": [np.nan, 1, 2],
            "effective_ts_latest_v2": pd.to_datetime(
                [
                    np.nan,
                    "2022-04-12 10:00:00",
                    "2022-04-20 10:00:00",
                ]
            ),
            "scd_value_latest_v2": [np.nan, 1, 2],
        }
    )
    table_prefix = "TEST_SCD_JOIN_SMALL"
    await snowflake_session.register_table(f"{table_prefix}_EVENT", df_events, temporary=False)
    await snowflake_session.register_table(f"{table_prefix}_SCD", df_scd, temporary=False)
    event_view = EventView.from_event_data(
        EventData.from_tabular_source(
            tabular_source=snowflake_feature_store.get_table(
                table_name=f"{table_prefix}_EVENT",
                database_name=snowflake_session.database,
                schema_name=snowflake_session.sf_schema,
            ),
            name="event_data",
            event_id_column="event_id",
            event_timestamp_column="ts",
        )
    )
    scd_view = SlowlyChangingView.from_slowly_changing_data(
        SlowlyChangingData.from_tabular_source(
            tabular_source=snowflake_feature_store.get_table(
                table_name=f"{table_prefix}_SCD",
                database_name=snowflake_session.database,
                schema_name=snowflake_session.sf_schema,
            ),
            name="scd_data",
            natural_key_column="scd_cust_id",
            effective_timestamp_column="effective_ts",
            surrogate_key_column="scd_cust_id",
        )
    )
    event_view.join(scd_view, on="cust_id", rsuffix="_latest")
    event_view.join(scd_view, on="cust_id", rsuffix="_latest_v2")
    df_actual = event_view.preview()
    pd.testing.assert_frame_equal(df_actual, df_expected, check_dtype=False)


def test_event_view_join_scd_view__preview_view(event_data, scd_data, expected_dataframe_scd_join):
    """
    Test joining an EventView with and SCDView
    """
    event_view = EventView.from_event_data(event_data)
    scd_data = SlowlyChangingView.from_slowly_changing_data(scd_data)
    event_view.join(scd_data, on="USER ID")
    df = event_view.preview(1000)
    df_expected = expected_dataframe_scd_join

    # Check correctness of joined view
    df["EVENT_TIMESTAMP"] = pd.to_datetime(df["EVENT_TIMESTAMP"], utc=True).dt.tz_localize(None)
    df_compare = df[["EVENT_TIMESTAMP", "USER ID", "User Status"]].merge(
        df_expected[["EVENT_TIMESTAMP", "USER ID", "User Status"]],
        on=["EVENT_TIMESTAMP", "USER ID"],
        suffixes=("_actual", "_expected"),
    )
    pd.testing.assert_series_equal(
        df_compare["User Status_actual"],
        df_compare["User Status_expected"],
        check_names=False,
    )


def test_event_view_join_scd_view__preview_feature(event_data, scd_data):
    """
    Test joining an EventView with and SCDView
    """
    event_view = EventView.from_event_data(event_data)
    scd_data = SlowlyChangingView.from_slowly_changing_data(scd_data)
    event_view.join(scd_data, on="USER ID")

    # Create a feature and preview it
    feature = event_view.groupby("USER ID", category="User Status").aggregate_over(
        method="count",
        windows=["7d"],
        feature_names=["count_7d"],
    )["count_7d"]

    df = feature.preview({"POINT_IN_TIME": "2001-11-15 10:00:00", "user id": 1})

    assert df.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
        "user id": 1,
        "count_7d": '{\n  "STATUS_CODE_12": 2,\n  "STATUS_CODE_3": 1,\n  "STATUS_CODE_46": 10,\n  "STATUS_CODE_48": 5\n}',
    }


def test_scd_lookup_feature(event_data, dimension_data, scd_data, scd_dataframe):
    """
    Test creating lookup feature from a SlowlyChangingView
    """
    # SCD lookup feature
    scd_view = SlowlyChangingView.from_slowly_changing_data(scd_data)
    scd_lookup_feature = scd_view["User Status"].as_feature("Current User Status")

    # Dimension lookup feature
    dimension_view = DimensionView.from_dimension_data(dimension_data)
    dimension_lookup_feature = dimension_view["item_name"].as_feature("Item Name Feature")

    # Window feature that depends on an SCD join
    event_view = EventView.from_event_data(event_data)
    event_view.join(scd_view, on="USER ID")
    window_feature = event_view.groupby("USER ID", "User Status").aggregate_over(
        method="count",
        windows=["7d"],
        feature_names=["count_7d"],
    )["count_7d"]

    # Preview a feature list with above features
    feature_list = FeatureList([window_feature, scd_lookup_feature, dimension_lookup_feature])
    point_in_time = "2001-11-15 10:00:00"
    item_id = "item_42"
    user_id = 1
    preview_params = {
        "POINT_IN_TIME": point_in_time,
        "user id": user_id,
        "item_id": item_id,
    }
    preview_output = feature_list.preview(preview_params).iloc[0].to_dict()
    assert set(preview_output.keys()) == {
        "POINT_IN_TIME",
        "user id",
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
    assert (
        preview_output["count_7d"]
        == '{\n  "STATUS_CODE_12": 2,\n  "STATUS_CODE_3": 1,\n  "STATUS_CODE_46": 10,\n  "STATUS_CODE_48": 5\n}'
    )


def test_scd_lookup_feature_with_offset(scd_data, scd_dataframe):
    """
    Test creating lookup feature from a SlowlyChangingView with offset
    """
    # SCD lookup feature
    offset = "90d"
    scd_view = SlowlyChangingView.from_slowly_changing_data(scd_data)
    scd_lookup_feature = scd_view["User Status"].as_feature("Current User Status", offset=offset)

    # Preview
    point_in_time = "2001-11-15 10:00:00"
    user_id = 1
    preview_params = {
        "POINT_IN_TIME": point_in_time,
        "user id": user_id,
    }
    preview_output = scd_lookup_feature.preview(preview_params).iloc[0].to_dict()

    # Compare with expected result
    mask = (
        scd_dataframe["Effective Timestamp"]
        <= (pd.to_datetime(point_in_time) - pd.Timedelta(offset))
    ) & (scd_dataframe["User ID"] == user_id)
    expected_row = scd_dataframe[mask].sort_values("Effective Timestamp").iloc[-1]
    assert preview_output["Current User Status"] == expected_row["User Status"]


def test_aggregate_asat(scd_data, scd_dataframe):
    """
    Test aggregate_asat aggregation on SlowlyChangingView
    """
    scd_view = SlowlyChangingView.from_slowly_changing_data(scd_data)
    feature = scd_view.groupby("User Status").aggregate_asat(
        method="count", feature_name="Current Number of Users With This Status"
    )

    # check preview
    df = feature.preview(
        {
            "POINT_IN_TIME": "2001-10-25 10:00:00",
            "user_status": "STATUS_CODE_42",
        }
    )
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-10-25 10:00:00"),
        "user_status": "STATUS_CODE_42",
        "Current Number of Users With This Status": 1,
    }
    assert df.iloc[0].to_dict() == expected

    # check historical features
    feature_list = FeatureList([feature])
    observations_set = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.date_range("2001-01-10 10:00:00", periods=10, freq="1d"),
            "user_status": ["STATUS_CODE_47"] * 10,
        }
    )
    expected = observations_set.copy()
    expected["Current Number of Users With This Status"] = [0, 1, 2, 2, 1, 1, 0, 0, 0, 0]
    df = feature_list.get_historical_features(observations_set)
    df = df.sort_values("POINT_IN_TIME").reset_index(drop=True)
    pd.testing.assert_frame_equal(df, expected)
