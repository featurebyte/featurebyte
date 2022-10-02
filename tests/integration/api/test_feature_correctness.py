import json
import time
from collections import defaultdict
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from featurebyte.api.event_view import EventView
from featurebyte.api.feature_list import FeatureList
from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.logger import logger
from featurebyte.query_graph.sql.tile_compute import epoch_seconds_to_timestamp, get_epoch_seconds
from tests.util.helper import get_lagged_series_pandas


def calculate_feature_ground_truth(
    df,
    point_in_time,
    event_timestamp_column_name,
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
    Reference implementation for feature calculation that is as simple as possible
    """
    if variable_column_name is None:
        # take any column because it doesn't matter
        variable_column_name = df.columns[0]

    last_job_index = (get_epoch_seconds(point_in_time) - time_modulo_frequency) // frequency
    last_job_epoch_seconds = last_job_index * frequency + time_modulo_frequency

    window_end_epoch_seconds = last_job_epoch_seconds - blind_spot
    window_start_epoch_seconds = window_end_epoch_seconds - window_size

    window_end = epoch_seconds_to_timestamp(window_end_epoch_seconds)
    window_start = epoch_seconds_to_timestamp(window_start_epoch_seconds)

    mask = df[entity_column_name] == entity_value
    mask &= (df[event_timestamp_column_name] >= window_start) & (
        df[event_timestamp_column_name] < window_end
    )
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


@pytest.fixture(scope="session")
def training_events(transaction_data_upper_case):

    # Sample training time points from historical data
    df = transaction_data_upper_case
    cols = ["EVENT_TIMESTAMP", "USER ID"]
    df = df[cols].drop_duplicates(cols)
    df = df.sample(1000, replace=False, random_state=0).reset_index(drop=True)
    df.rename({"EVENT_TIMESTAMP": "POINT_IN_TIME"}, axis=1, inplace=True)

    # Add random spikes to point in time of some rows
    rng = np.random.RandomState(0)
    spike_mask = rng.randint(0, 2, len(df)).astype(bool)
    spike_shift = pd.to_timedelta(rng.randint(0, 3601, len(df)), unit="s")
    df.loc[spike_mask, "POINT_IN_TIME"] = (
        df.loc[spike_mask, "POINT_IN_TIME"] + spike_shift[spike_mask]
    )
    df = df.reset_index(drop=True)

    return df


def get_expected_feature_values(training_events, feature_name, **kwargs):
    """
    Calculate the expected feature values given training_events and feature parameters
    """

    expected_output = defaultdict(list)

    for _, row in training_events.iterrows():
        entity_value = row[kwargs["entity_column_name"]]
        point_in_time = row["POINT_IN_TIME"]
        val = calculate_feature_ground_truth(
            point_in_time=point_in_time,
            entity_value=entity_value,
            **kwargs,
        )
        expected_output["POINT_IN_TIME"].append(point_in_time)
        expected_output[kwargs["entity_column_name"]].append(entity_value)
        expected_output[feature_name].append(val)

    df_expected = pd.DataFrame(expected_output, index=training_events.index)
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


def assert_dict_equal(s1, s2):
    """
    Check two dict like columns are equal

    Parameters
    ----------
    s1 : Series
        First series
    s2 : Series
        Second series
    """

    def _json_normalize(x):
        # json conversion during preview changed None to nan
        if x is None or np.nan:
            return None
        return json.loads(x)

    s1 = s1.apply(_json_normalize)
    s2 = s2.apply(_json_normalize)
    pd.testing.assert_series_equal(s1, s2)


def fb_assert_frame_equal(df, df_expected, dict_like_columns=None):
    """
    Check that two DataFrames are equal

    Parameters
    ----------
    df : DataFrame
        DataFrame to check
    df_expected : DataFrame
        Reference DataFrame
    dict_like_columns : list | None
        List of dict like columns which will be compared accordingly, not just exact match
    """

    assert df.columns.tolist() == df_expected.columns.tolist()

    regular_columns = df.columns.tolist()
    if dict_like_columns is not None:
        assert isinstance(dict_like_columns, list)
        regular_columns = [col for col in regular_columns if col not in dict_like_columns]

    if regular_columns:
        for col in regular_columns:
            if isinstance(df[col].iloc[0], Decimal):
                df[col] = df[col].astype(int)
        pd.testing.assert_frame_equal(
            df[regular_columns], df_expected[regular_columns], check_dtype=False
        )

    if dict_like_columns:
        for col in dict_like_columns:
            assert_dict_equal(df[col], df_expected[col])


def add_inter_events_derived_columns(df, event_view):
    """
    Add inter-events columns such as lags
    """
    df = df.copy()
    by_column = "CUST_ID"

    # Previous amount
    col = f"PREV_AMOUNT_BY_{by_column}"
    df[col] = get_lagged_series_pandas(df, "AMOUNT", "EVENT_TIMESTAMP", by_column)
    event_view[col] = event_view["AMOUNT"].lag(by_column)

    # Time since previous event
    col = f"TIME_SINCE_PREVIOUS_EVENT_BY_{by_column}"
    df[col] = (
        df["EVENT_TIMESTAMP"]
        - get_lagged_series_pandas(df, "EVENT_TIMESTAMP", "EVENT_TIMESTAMP", by_column)
    ).dt.total_seconds()
    event_view[col] = event_view["EVENT_TIMESTAMP"] - event_view["EVENT_TIMESTAMP"].lag(by_column)

    return df


def check_feature_preview(feature_list, df_expected, dict_like_columns, n_points=10):
    """
    Check correctness of feature preview result
    """
    tic = time.time()
    sampled_points = df_expected.sample(n=n_points, random_state=0)
    for _, preview_time_point in sampled_points.iterrows():
        preview_param = {
            "POINT_IN_TIME": preview_time_point["POINT_IN_TIME"],
            "user id": preview_time_point["USER ID"],
        }
        output = feature_list[feature_list.feature_names].preview(preview_param)
        output.rename({"user id": "USER ID"}, axis=1, inplace=True)
        df_expected = pd.DataFrame([preview_time_point], index=output.index)
        fb_assert_frame_equal(output, df_expected, dict_like_columns)
    elapsed = time.time() - tic
    print(f"elapsed check_feature_preview: {elapsed:.2f}s")


def test_aggregation(
    transaction_data_upper_case,
    training_events,
    event_data,
    config,
):
    """
    Test that aggregation produces correct feature values
    """

    # Test cases listed here. This is written this way instead of parametrized test is so that all
    # features can be retrieved in one historical request
    feature_parameters = [
        ("AMOUNT", "avg", "2h", "avg_2h", lambda x: x.mean(), None),
        ("AMOUNT", "avg", "24h", "avg_24h", lambda x: x.mean(), None),
        ("AMOUNT", "min", "24h", "min_24h", lambda x: x.min(), None),
        ("AMOUNT", "max", "24h", "max_24h", lambda x: x.max(), None),
        ("AMOUNT", "sum", "24h", "sum_24h", sum_func, None),
        (None, "count", "24h", "count_24h", lambda x: len(x), None),
        ("AMOUNT", "na_count", "24h", "na_count_24h", lambda x: x.isnull().sum(), None),
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
        ("AMOUNT", "std", "24h", "std_24h", lambda x: x.std(ddof=0), None),
    ]

    event_view = EventView.from_event_data(event_data)
    feature_job_setting = event_data.default_feature_job_setting
    frequency, time_modulo_frequency, blind_spot = validate_job_setting_parameters(
        frequency=feature_job_setting.frequency,
        time_modulo_frequency=feature_job_setting.time_modulo_frequency,
        blind_spot=feature_job_setting.blind_spot,
    )

    # Some fixed parameters
    entity_column_name = "USER ID"
    event_timestamp_column_name = "EVENT_TIMESTAMP"

    # Apply a filter condition
    def _get_filtered_data(event_view_or_dataframe):
        cond1 = event_view_or_dataframe["AMOUNT"] > 20
        cond2 = event_view_or_dataframe["AMOUNT"].isnull()
        mask = cond1 | cond2
        return event_view_or_dataframe[mask]

    event_view = _get_filtered_data(event_view)
    transaction_data_upper_case = _get_filtered_data(transaction_data_upper_case)

    # Add inter-event derived columns
    transaction_data_upper_case = add_inter_events_derived_columns(
        transaction_data_upper_case, event_view
    )

    features = []
    df_expected_all = [training_events]

    elapsed_time_ref = 0
    for (
        variable_column_name,
        agg_name,
        window,
        feature_name,
        agg_func_callable,
        category,
    ) in feature_parameters:

        feature_group = event_view.groupby(entity_column_name, category=category).aggregate(
            method=agg_name,
            value_column=variable_column_name,
            windows=[window],
            feature_names=[feature_name],
        )
        features.append(feature_group[feature_name])

        tic = time.time()
        window_size = pd.Timedelta(window).total_seconds()
        df_expected = get_expected_feature_values(
            training_events,
            feature_name,
            df=transaction_data_upper_case,
            entity_column_name=entity_column_name,
            event_timestamp_column_name=event_timestamp_column_name,
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
    feature_list = FeatureList(features)

    dict_like_columns = ["count_by_action_24h"]
    check_feature_preview(feature_list, df_expected, dict_like_columns)

    tic = time.time()
    df_historical_features = feature_list.get_historical_features(
        training_events,
        serving_names_mapping={"user id": "USER ID"},
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

    fb_assert_frame_equal(df_historical_features, df_expected, dict_like_columns)
