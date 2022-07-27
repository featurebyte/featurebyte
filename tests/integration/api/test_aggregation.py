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

    return df


def test_aggregation(transaction_data_upper_case, training_events, event_data, config):

    event_view = EventView.from_event_data(event_data)
    feature_job_setting = event_data.default_feature_job_setting
    frequency, time_modulo_frequency, blind_spot = validate_job_setting_parameters(
        frequency=feature_job_setting.frequency,
        time_modulo_frequency=feature_job_setting.time_modulo_frequency,
        blind_spot=feature_job_setting.blind_spot,
    )
    feature_group = event_view.groupby("USER_ID").aggregate(
        "AMOUNT",
        "avg",
        windows=["24h"],
        feature_names=["AVG_24h"],
    )
    feature_list = FeatureList([feature_group])

    expected_output = defaultdict(list)
    event_timestamp_column_name = "EVENT_TIMESTAMP"
    variable_column_name = "AMOUNT"
    entity_column_name = "USER_ID"
    agg_func = lambda x: x.mean()
    window_size = 3600 * 24
    feature_name = feature_group.feature_names[0]

    for _, row in training_events.iterrows():
        entity_value = row[entity_column_name]
        point_in_time = row["POINT_IN_TIME"]
        val = calculate_feature_ground_truth(
            df=transaction_data_upper_case,
            point_in_time=point_in_time,
            event_timestamp_column_name=event_timestamp_column_name,
            entity_column_name=entity_column_name,
            entity_value=entity_value,
            variable_column_name=variable_column_name,
            agg_func=agg_func,
            window_size=window_size,
            frequency=frequency,
            time_modulo_frequency=time_modulo_frequency,
            blind_spot=blind_spot,
        )
        expected_output["POINT_IN_TIME"].append(point_in_time)
        expected_output[entity_column_name].append(entity_value)
        expected_output[feature_name].append(val)

    df_expected = pd.DataFrame(expected_output)
    df_historical_features = feature_list.get_historical_features(
        training_events, credentials=config.credentials, serving_names_mapping={"UID": "USER_ID"}
    )

    df_expected = df_expected.sort_values(["POINT_IN_TIME", entity_column_name]).reset_index(
        drop=True
    )
    df_historical_features = df_historical_features.sort_values(
        ["POINT_IN_TIME", entity_column_name]
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(df_historical_features, df_expected)
