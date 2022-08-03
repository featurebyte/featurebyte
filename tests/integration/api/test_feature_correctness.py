from collections import defaultdict

import numpy as np
import pandas as pd
import pytest

from featurebyte.api.event_view import EventView
from featurebyte.api.feature_list import FeatureList
from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.query_graph.tile_compute import epoch_seconds_to_timestamp, get_epoch_seconds


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
):
    """
    Reference implementation for feature calculation that is as simple as possible
    """
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

    out = agg_func(df_filtered[variable_column_name])
    return out


@pytest.fixture(scope="session")
def training_events(transaction_data_upper_case):

    # Sample training time points from historical data
    df = transaction_data_upper_case
    cols = ["EVENT_TIMESTAMP", "USER_ID"]
    df = df[cols].drop_duplicates(cols)
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
        ("avg", "avg_24h", lambda x: x.mean()),
        ("min", "min_24h", lambda x: x.min()),
        ("max", "max_24h", lambda x: x.max()),
        ("sum", "sum_24h", sum_func),
        ("count", "count_24h", lambda x: len(x)),
    ]

    event_view = EventView.from_event_data(event_data)
    feature_job_setting = event_data.default_feature_job_setting
    frequency, time_modulo_frequency, blind_spot = validate_job_setting_parameters(
        frequency=feature_job_setting.frequency,
        time_modulo_frequency=feature_job_setting.time_modulo_frequency,
        blind_spot=feature_job_setting.blind_spot,
    )

    # Some fixed parameters
    variable_column_name = "AMOUNT"
    entity_column_name = "USER_ID"
    window_size = 3600 * 24
    event_timestamp_column_name = "EVENT_TIMESTAMP"

    features = []
    df_expected_all = [training_events]

    for agg_name, feature_name, agg_func_callable in feature_parameters:

        feature_group = event_view.groupby(entity_column_name).aggregate(
            variable_column_name,
            agg_name,
            windows=["24h"],
            feature_names=[feature_name],
        )
        features.append(feature_group[feature_name])

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
        )[[feature_name]]
        df_expected_all.append(df_expected)

    df_expected = pd.concat(df_expected_all, axis=1)
    feature_list = FeatureList(features)
    df_historical_features = feature_list.get_historical_features(
        training_events, credentials=config.credentials, serving_names_mapping={"uid": "USER_ID"}
    )

    # Note: The row output order can be different, so sort before comparing
    df_expected = df_expected.sort_values(["POINT_IN_TIME", entity_column_name]).reset_index(
        drop=True
    )
    df_historical_features = df_historical_features.sort_values(
        ["POINT_IN_TIME", entity_column_name]
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(df_historical_features, df_expected, check_dtype=False)
