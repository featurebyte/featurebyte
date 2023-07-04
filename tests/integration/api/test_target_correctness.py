"""
Target correctness tests.
"""
from typing import Optional

import json
from collections import defaultdict
from dataclasses import dataclass

import pandas as pd
import pytest

from featurebyte.query_graph.sql.tile_compute import epoch_seconds_to_timestamp, get_epoch_seconds
from tests.integration.api.test_feature_correctness import sum_func
from tests.util.helper import fb_assert_frame_equal


@dataclass
class TargetParameter:
    """
    Target parameter for testing
    """

    variable_column_name: str
    agg_name: str
    horizon: str
    target_name: str
    agg_func: callable
    skip: bool = True


@pytest.fixture(name="target_parameters")
def target_parameters_fixture(source_type):
    """
    Parameters for feature tests using aggregate_over
    """
    parameters = [
        TargetParameter("ÀMOUNT", "avg", "2h", "avg_2h", lambda x: x.mean(), skip=False),
        TargetParameter("ÀMOUNT", "avg", "24h", "avg_24h", lambda x: x.mean()),
        TargetParameter("ÀMOUNT", "min", "24h", "min_24h", lambda x: x.min()),
        TargetParameter("ÀMOUNT", "max", "24h", "max_24h", lambda x: x.max()),
        TargetParameter("ÀMOUNT", "sum", "24h", "sum_24h", sum_func),
    ]
    spark_only_agg_types = ["max", "std", "latest"]
    if source_type == "spark":
        parameters = [param for param in parameters if param.agg_name in spark_only_agg_types]
    return parameters


def transform_and_sample_observation_set(observation_set: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and sample observation set.

    - Transform POINT_IN_TIME to datetime
    - Sample 50 points
    - Rename ÜSER ID to üser id

    Parameters
    ----------
    observation_set: pd.DataFrame
        Observation set

    Returns
    -------
    pd.DataFrame
    """
    observation_set_copied = observation_set.copy()
    observation_set_copied["POINT_IN_TIME"] = pd.to_datetime(
        observation_set_copied["POINT_IN_TIME"], utc=True
    ).dt.tz_localize(None)

    sampled_points = observation_set_copied.sample(n=50, random_state=0).reset_index(drop=True)
    sampled_points.rename({"ÜSER ID": "üser id"}, axis=1, inplace=True)
    return sampled_points


def apply_agg_func_on_filtered_dataframe(agg_func, category, df_filtered, variable_column_name):
    """
    Helper function to apply an aggregation function on a dataframe filtered to contain only the
    data within the feature window for a specific entity

    # TODO: copied from test_feature_correctness
    """
    # Handle no category
    if category is None:
        return agg_func(df_filtered[variable_column_name])

    # Handle category
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
        return None
    return json.dumps(pd.Series(out).to_dict())


def convert_duration_str_to_seconds(duration_str: str) -> Optional[float]:
    if duration_str is not None:
        return pd.Timedelta(duration_str).total_seconds()
    return None


def calculate_forward_aggregate_ground_truth(
    df: pd.DataFrame,
    point_in_time,
    utc_event_timestamps,
    entity_column_name,
    entity_value,
    variable_column_name: str,
    agg_func,
    horizon: Optional[float],
    category=None,
):
    """
    Reference implementation for forward_aggregate that is as simple as possible
    """
    # Find the window of datapoints that are relevant
    window_start_epoch_seconds = get_epoch_seconds(point_in_time)
    window_start = epoch_seconds_to_timestamp(window_start_epoch_seconds)

    if horizon is not None:
        window_end_epoch_seconds = window_start_epoch_seconds + horizon
        window_end = epoch_seconds_to_timestamp(int(window_end_epoch_seconds))
    else:
        window_end = None

    mask = df[entity_column_name] == entity_value
    mask &= utc_event_timestamps > window_start
    if window_end is not None:
        mask &= utc_event_timestamps <= window_end
    df_filtered = df[mask]

    # Apply the aggregation function
    return apply_agg_func_on_filtered_dataframe(
        agg_func, category, df_filtered, variable_column_name
    )


def get_expected_target_values(
    observation_set: pd.DataFrame,
    entity_column_name: str,
    target_name: str,
    df: pd.DataFrame,
    utc_event_timestamps,
    variable_column_name: str,
    agg_func,
    horizon: Optional[float],
    category=None,
) -> pd.DataFrame:
    expected_output = defaultdict(list)

    for _, row in observation_set.iterrows():
        entity_value = row[entity_column_name]
        point_in_time = row["POINT_IN_TIME"]
        val = calculate_forward_aggregate_ground_truth(
            point_in_time=point_in_time,
            entity_value=entity_value,
            df=df,
            utc_event_timestamps=utc_event_timestamps,
            entity_column_name=entity_column_name,
            variable_column_name=variable_column_name,
            agg_func=agg_func,
            horizon=horizon,
            category=category,
        )
        expected_output["POINT_IN_TIME"].append(point_in_time)
        expected_output[entity_column_name].append(entity_value)
        expected_output[target_name].append(val)
    df_expected = pd.DataFrame(expected_output, index=observation_set.index)
    return df_expected


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_forward_aggregate(
    event_table, target_parameters, transaction_data_upper_case, observation_set
):
    """
    Test forward aggregate.
    """
    event_view = event_table.get_view()
    entity_column_name = "ÜSER ID"

    # Build dataframe for testing
    # Apply a filter condition
    # def _get_filtered_data(event_view_or_dataframe):
    #     cond1 = event_view_or_dataframe["ÀMOUNT"] > 20
    #     cond2 = event_view_or_dataframe["ÀMOUNT"].isnull()
    #     mask = cond1 | cond2
    #     return event_view_or_dataframe[mask]

    # event_view = _get_filtered_data(event_view)
    # transaction_data_upper_case = _get_filtered_data(transaction_data_upper_case)
    event_timestamp_column_name = "ËVENT_TIMESTAMP"
    df = transaction_data_upper_case.sort_values(event_timestamp_column_name)

    for target_parameter in target_parameters:
        if target_parameter.skip:
            continue

        # Get expected target values
        utc_event_timestamps = pd.to_datetime(
            df[event_timestamp_column_name], utc=True
        ).dt.tz_localize(None)
        expected_values = get_expected_target_values(
            observation_set,
            entity_column_name,
            target_name=target_parameter.target_name,
            df=df,
            utc_event_timestamps=utc_event_timestamps,
            variable_column_name=target_parameter.variable_column_name,
            agg_func=target_parameter.agg_func,
            horizon=convert_duration_str_to_seconds(target_parameter.horizon),
        )

        # Transform and sample to get a smaller sample dataframe just for preview
        expected_values = transform_and_sample_observation_set(expected_values)
        expected_values.to_csv("expected_values.csv", index=False)

        # Build actual Target preview results
        target = event_view.groupby(entity_column_name).forward_aggregate(
            method=target_parameter.agg_name,
            value_column=target_parameter.variable_column_name,
            horizon=target_parameter.horizon,
            target_name=target_parameter.target_name,
        )
        results = target.preview(expected_values[["POINT_IN_TIME", "üser id"]])
        results.to_csv("results.csv", index=False)
        # Compare actual preview, against sampled results
        fb_assert_frame_equal(results, expected_values)
