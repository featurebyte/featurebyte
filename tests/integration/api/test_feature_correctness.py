import json
import time
from collections import defaultdict

import numpy as np
import pandas as pd
import pytest

from featurebyte.api.event_view import EventView
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.scd_view import SlowlyChangingView
from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.logger import logger
from featurebyte.query_graph.sql.tile_compute import epoch_seconds_to_timestamp, get_epoch_seconds
from tests.util.helper import fb_assert_frame_equal, get_lagged_series_pandas


def calculate_aggregate_over_ground_truth(
    df,
    point_in_time,
    utc_event_timestamps,
    entity_column_name,
    entity_value,
    variable_column_name,
    agg_func,
    window_size,
    frequency,
    time_modulo_frequency,
    blind_spot,
    category=None,
):
    """
    Reference implementation for aggregate_over that is as simple as possible
    """
    if variable_column_name is None:
        # take any column because it doesn't matter
        variable_column_name = df.columns[0]

    last_job_index = (get_epoch_seconds(point_in_time) - time_modulo_frequency) // frequency
    last_job_epoch_seconds = last_job_index * frequency + time_modulo_frequency

    window_end_epoch_seconds = last_job_epoch_seconds - blind_spot
    window_end = epoch_seconds_to_timestamp(window_end_epoch_seconds)

    if window_size is not None:
        window_start_epoch_seconds = window_end_epoch_seconds - window_size
        window_start = epoch_seconds_to_timestamp(window_start_epoch_seconds)
    else:
        window_start = None

    mask = df[entity_column_name] == entity_value
    mask &= utc_event_timestamps < window_end
    if window_start is not None:
        mask &= utc_event_timestamps >= window_start
    df_filtered = df[mask]

    if category is None:
        out = agg_func(df_filtered[variable_column_name])
    else:
        out = {}
        category_vals = df_filtered[category].unique()
        for category_val in category_vals:
            if pd.isnull(category_val):
                category_val = "__MISSING__"
                category_mask = df_filtered[category].isnull()
            else:
                category_mask = df_filtered[category] == category_val
            feature_value = agg_func(df_filtered[category_mask][variable_column_name])
            out[category_val] = feature_value
        if not out:
            out = None
        else:
            out = json.dumps(pd.Series(out).to_dict())
    return out


def calculate_aggregate_asat_ground_truth(
    df,
    point_in_time,
    effective_timestamp_column,
    natural_key_column_name,
    entity_column_name,
    entity_value,
    variable_column_name,
    agg_func,
):
    """
    Reference implementation for aggregate_asat
    """
    if variable_column_name is None:
        # take any column because it doesn't matter
        variable_column_name = df.columns[0]

    df = df[df[effective_timestamp_column] <= point_in_time]

    def _extract_current_record(sub_df):
        latest_effective_timestamp = sub_df[effective_timestamp_column].max()
        latest_sub_df = sub_df[sub_df[effective_timestamp_column] == latest_effective_timestamp]
        if latest_sub_df.shape[0] == 0:
            return None
        assert latest_sub_df.shape[0] == 1
        return latest_sub_df.iloc[0]

    df_current = df.groupby(natural_key_column_name).apply(_extract_current_record)

    df_filtered = df_current[df_current[entity_column_name] == entity_value]
    out = agg_func(df_filtered[variable_column_name])

    return out


@pytest.fixture(scope="session")
def observation_set(transaction_data_upper_case):

    # Sample training time points from historical data
    df = transaction_data_upper_case
    cols = ["ËVENT_TIMESTAMP", "ÜSER ID"]
    df = df[cols].drop_duplicates(cols)
    df = df.sample(1000, replace=False, random_state=0).reset_index(drop=True)
    df.rename({"ËVENT_TIMESTAMP": "POINT_IN_TIME"}, axis=1, inplace=True)

    # Add random spikes to point in time of some rows
    rng = np.random.RandomState(0)
    spike_mask = rng.randint(0, 2, len(df)).astype(bool)
    spike_shift = pd.to_timedelta(rng.randint(0, 3601, len(df)), unit="s")
    df.loc[spike_mask, "POINT_IN_TIME"] = (
        df.loc[spike_mask, "POINT_IN_TIME"] + spike_shift[spike_mask]
    )
    df = df.reset_index(drop=True)

    return df


@pytest.fixture(scope="session")
def scd_observation_set(scd_dataframe):

    num_rows = 1000
    point_in_time_values = pd.date_range(
        scd_dataframe["Effective Timestamp"].min(),
        scd_dataframe["Effective Timestamp"].max(),
        periods=num_rows,
    ).floor("h")

    rng = np.random.RandomState(0)
    df = pd.DataFrame(
        {
            "POINT_IN_TIME": point_in_time_values,
            "User Status": rng.choice(scd_dataframe["User Status"].unique(), num_rows),
        }
    )
    # only TZ-naive timestamps in UTC supported for point-in-time
    df["POINT_IN_TIME"] = pd.to_datetime(df["POINT_IN_TIME"], utc=True).dt.tz_localize(None)
    return df


def get_expected_feature_values(kind, observation_set, feature_name, **kwargs):
    """
    Calculate the expected feature values given observation_set and feature parameters
    """
    assert kind in {"aggregate_over", "aggregate_asat"}

    expected_output = defaultdict(list)

    for _, row in observation_set.iterrows():
        entity_value = row[kwargs["entity_column_name"]]
        point_in_time = row["POINT_IN_TIME"]
        if kind == "aggregate_over":
            val = calculate_aggregate_over_ground_truth(
                point_in_time=point_in_time,
                entity_value=entity_value,
                **kwargs,
            )
        else:
            val = calculate_aggregate_asat_ground_truth(
                point_in_time=point_in_time,
                entity_value=entity_value,
                **kwargs,
            )
        expected_output["POINT_IN_TIME"].append(point_in_time)
        expected_output[kwargs["entity_column_name"]].append(entity_value)
        expected_output[feature_name].append(val)

    df_expected = pd.DataFrame(expected_output, index=observation_set.index)
    return df_expected


def sum_func(values):
    """
    Sum function that returns nan when there is no valid (non-null) values

    pandas.Series sum method returns 0 in that case.
    """
    if len(values) == 0:
        return np.nan
    if values.isnull().all():
        return np.nan
    return values.sum()


def add_inter_events_derived_columns(df, event_view):
    """
    Add inter-events columns such as lags
    """
    df = df.copy()
    by_column = "CUST_ID"

    # Previous amount
    col = f"PREV_AMOUNT_BY_{by_column}"
    df[col] = get_lagged_series_pandas(df, "ÀMOUNT", "ËVENT_TIMESTAMP", by_column)
    event_view[col] = event_view["ÀMOUNT"].lag(by_column)

    # Time since previous event
    col = f"TIME_SINCE_PREVIOUS_EVENT_BY_{by_column}"
    df[col] = (
        df["ËVENT_TIMESTAMP"]
        - get_lagged_series_pandas(df, "ËVENT_TIMESTAMP", "ËVENT_TIMESTAMP", by_column)
    ).dt.total_seconds()
    event_view[col] = event_view["ËVENT_TIMESTAMP"] - event_view["ËVENT_TIMESTAMP"].lag(by_column)

    return df


def check_feature_preview(feature_list, df_expected, dict_like_columns, n_points=10):
    """
    Check correctness of feature preview result
    """
    # expect point-in-time to be converted to UTC without timezone
    df_expected = df_expected.copy()
    df_expected["POINT_IN_TIME"] = pd.to_datetime(
        df_expected["POINT_IN_TIME"], utc=True
    ).dt.tz_localize(None)

    tic = time.time()
    sampled_points = df_expected.sample(n=n_points, random_state=0)
    for _, preview_time_point in sampled_points.iterrows():
        preview_param = {
            "POINT_IN_TIME": preview_time_point["POINT_IN_TIME"],
            "üser id": preview_time_point["ÜSER ID"],
        }
        output = feature_list[feature_list.feature_names].preview(preview_param)
        output.rename({"üser id": "ÜSER ID"}, axis=1, inplace=True)
        df_expected = pd.DataFrame([preview_time_point], index=output.index)
        fb_assert_frame_equal(output, df_expected, dict_like_columns)
    elapsed = time.time() - tic
    print(f"elapsed check_feature_preview: {elapsed:.2f}s")


@pytest.fixture(name="feature_parameters")
def feature_parameters_fixture(source_type):
    """
    Parameters for feature tests using aggregate_over
    """
    parameters = [
        ("ÀMOUNT", "avg", "2h", "avg_2h", lambda x: x.mean(), None),
        ("ÀMOUNT", "avg", "24h", "avg_24h", lambda x: x.mean(), None),
        ("ÀMOUNT", "min", "24h", "min_24h", lambda x: x.min(), None),
        ("ÀMOUNT", "max", "24h", "max_24h", lambda x: x.max(), None),
        ("ÀMOUNT", "sum", "24h", "sum_24h", sum_func, None),
        (None, "count", "24h", "count_24h", lambda x: len(x), None),
        ("ÀMOUNT", "na_count", "24h", "na_count_24h", lambda x: x.isnull().sum(), None),
        (None, "count", "24h", "count_by_action_24h", lambda x: len(x), "PRODUCT_ACTION"),
        ("PREV_AMOUNT_BY_CUST_ID", "avg", "24h", "prev_amount_avg_24h", lambda x: x.mean(), None),
        (
            "TIME_SINCE_PREVIOUS_EVENT_BY_CUST_ID",
            "avg",
            "24h",
            "event_interval_avg_24h",
            lambda x: x.mean(),
            None,
        ),
        ("ÀMOUNT", "std", "24h", "std_24h", lambda x: x.std(ddof=0), None),
        (
            "ÀMOUNT",
            "latest",
            "24h",
            "latest_24h",
            lambda x: x.values[-1] if len(x) > 0 else None,
            None,
        ),
        (
            "ÀMOUNT",
            "latest",
            None,
            "latest_ever",
            lambda x: x.values[-1] if len(x) > 0 else None,
            None,
        ),
    ]
    if source_type == "spark":
        parameters = [param for param in parameters if param[1] in ["max", "std", "latest"]]
    return parameters


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_aggregate_over(
    transaction_data_upper_case,
    observation_set,
    event_data,
    config,
    feature_parameters,
):
    """
    Test that aggregate_over produces correct feature values
    """
    event_view = EventView.from_event_data(event_data)
    feature_job_setting = event_data.default_feature_job_setting
    frequency, time_modulo_frequency, blind_spot = validate_job_setting_parameters(
        frequency=feature_job_setting.frequency,
        time_modulo_frequency=feature_job_setting.time_modulo_frequency,
        blind_spot=feature_job_setting.blind_spot,
    )

    # Some fixed parameters
    entity_column_name = "ÜSER ID"
    event_timestamp_column_name = "ËVENT_TIMESTAMP"

    # Apply a filter condition
    def _get_filtered_data(event_view_or_dataframe):
        cond1 = event_view_or_dataframe["ÀMOUNT"] > 20
        cond2 = event_view_or_dataframe["ÀMOUNT"].isnull()
        mask = cond1 | cond2
        return event_view_or_dataframe[mask]

    event_view = _get_filtered_data(event_view)
    transaction_data_upper_case = _get_filtered_data(transaction_data_upper_case)

    # Add inter-event derived columns
    transaction_data_upper_case = add_inter_events_derived_columns(
        transaction_data_upper_case, event_view
    )

    features = []
    df_expected_all = [observation_set]
    df = transaction_data_upper_case.sort_values(event_timestamp_column_name)
    utc_event_timestamps = pd.to_datetime(df[event_timestamp_column_name], utc=True).dt.tz_localize(
        None
    )

    elapsed_time_ref = 0
    for (
        variable_column_name,
        agg_name,
        window,
        feature_name,
        agg_func_callable,
        category,
    ) in feature_parameters:

        feature_group = event_view.groupby(entity_column_name, category=category).aggregate_over(
            method=agg_name,
            value_column=variable_column_name,
            windows=[window],
            feature_names=[feature_name],
        )
        features.append(feature_group[feature_name])

        tic = time.time()
        if window is not None:
            window_size = pd.Timedelta(window).total_seconds()
        else:
            window_size = None
        df_expected = get_expected_feature_values(
            "aggregate_over",
            observation_set,
            feature_name,
            df=df,
            entity_column_name=entity_column_name,
            utc_event_timestamps=utc_event_timestamps,
            variable_column_name=variable_column_name,
            agg_func=agg_func_callable,
            window_size=window_size,
            frequency=frequency,
            time_modulo_frequency=time_modulo_frequency,
            blind_spot=blind_spot,
            category=category,
        )[[feature_name]]
        elapsed_time_ref += time.time() - tic
        df_expected_all.append(df_expected)
    logger.debug(f"elapsed reference implementation: {elapsed_time_ref}")

    df_expected = pd.concat(df_expected_all, axis=1)
    feature_list = FeatureList(features, name="feature_list")

    dict_like_columns = ["count_by_action_24h"]
    dict_like_columns = [col for col in dict_like_columns if col in df_expected.columns]

    check_feature_preview(feature_list, df_expected, dict_like_columns)

    tic = time.time()
    df_historical_features = feature_list.get_historical_features(
        observation_set,
        serving_names_mapping={"üser id": "ÜSER ID"},
    )
    elapsed_historical = time.time() - tic
    logger.debug(f"elapsed historical: {elapsed_historical}")

    # Note: The row output order can be different, so sort before comparing
    df_expected = df_expected.sort_values(["POINT_IN_TIME", entity_column_name]).reset_index(
        drop=True
    )
    df_historical_features = df_historical_features.sort_values(
        ["POINT_IN_TIME", entity_column_name]
    ).reset_index(drop=True)

    # expect point-in-time to be converted to UTC without timezone
    df_expected["POINT_IN_TIME"] = pd.to_datetime(
        df_expected["POINT_IN_TIME"], utc=True
    ).dt.tz_localize(None)

    fb_assert_frame_equal(df_historical_features, df_expected, dict_like_columns)


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_aggregate_asat(
    scd_dataframe,
    scd_observation_set,
    scd_data,
    config,
):
    """
    Test that aggregate_asat produces correct feature values
    """
    feature_parameters = [
        (None, "count", "asat_count", lambda x: len(x)),
    ]

    scd_view = SlowlyChangingView.from_slowly_changing_data(scd_data)
    entity_column_name = "User Status"
    effective_timestamp_column = "Effective Timestamp"

    features = []
    df_expected_all = [scd_observation_set]

    scd_dataframe = scd_dataframe.copy()
    scd_dataframe[effective_timestamp_column] = pd.to_datetime(
        scd_dataframe[effective_timestamp_column], utc=True
    ).dt.tz_localize(None)

    for (
        variable_column_name,
        agg_name,
        feature_name,
        agg_func_callable,
    ) in feature_parameters:

        feature = scd_view.groupby(entity_column_name).aggregate_asat(
            method=agg_name,
            value_column=variable_column_name,
            feature_name=feature_name,
        )
        features.append(feature)

        df_expected = get_expected_feature_values(
            "aggregate_asat",
            scd_observation_set,
            feature_name,
            df=scd_dataframe,
            effective_timestamp_column=effective_timestamp_column,
            natural_key_column_name="User ID",
            entity_column_name=entity_column_name,
            variable_column_name=variable_column_name,
            agg_func=agg_func_callable,
        )[[feature_name]]

        df_expected_all.append(df_expected)

    df_expected = pd.concat(df_expected_all, axis=1)
    feature_list = FeatureList(features, name="feature_list")

    # Check historical features
    df_historical_features = feature_list.get_historical_features(
        scd_observation_set,
        serving_names_mapping={"user_status": "User Status"},
    )

    df_expected = df_expected.sort_values(["POINT_IN_TIME", entity_column_name]).reset_index(
        drop=True
    )
    df_historical_features = df_historical_features.sort_values(
        ["POINT_IN_TIME", entity_column_name]
    ).reset_index(drop=True)

    fb_assert_frame_equal(df_historical_features, df_expected, [])
