from typing import Any, Optional

import time
from collections import defaultdict
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.api.feature_list import FeatureList
from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.enum import StrEnum
from featurebyte.logging import get_logger
from featurebyte.query_graph.sql.tile_compute import epoch_seconds_to_timestamp, get_epoch_seconds
from tests.integration.api.dataframe_helper import apply_agg_func_on_filtered_dataframe
from tests.util.helper import fb_assert_frame_equal, get_lagged_series_pandas

logger = get_logger(__name__)


@dataclass
class ExpectedFeatureValueParams:
    """
    Base parameters for calculating the expected feature value.
    """

    df: pd.DataFrame
    entity_column_name: str
    variable_column_name: str
    agg_func: callable
    category: Optional[Any]


@dataclass
class ExpectedEventFeatureValueParams(ExpectedFeatureValueParams):
    """
    Parameters for calculating the expected feature value for event features.
    """

    utc_event_timestamps: Optional[Any]
    window_size_secs: Optional[int]
    frequency: Optional[int]
    time_modulo_frequency: Optional[int]
    blind_spot: Optional[int]


@dataclass
class ExpectedSCDFeatureValueParams(ExpectedFeatureValueParams):
    """
    Parameters for calculating the expected feature value for scd features.
    """

    effective_timestamp_column: Optional[Any]
    natural_key_column_name: str


def calculate_aggregate_over_ground_truth(
    point_in_time,
    entity_value,
    expected_feature_value_params: ExpectedEventFeatureValueParams,
):
    """
    Reference implementation for aggregate_over that is as simple as possible
    """
    df = expected_feature_value_params.df
    variable_column_name = expected_feature_value_params.variable_column_name
    if expected_feature_value_params.variable_column_name is None:
        # take any column because it doesn't matter
        variable_column_name = df.columns[0]

    last_job_index = (
        get_epoch_seconds(point_in_time) - expected_feature_value_params.time_modulo_frequency
    ) // expected_feature_value_params.frequency
    last_job_epoch_seconds = (
        last_job_index * expected_feature_value_params.frequency
        + expected_feature_value_params.time_modulo_frequency
    )

    window_end_epoch_seconds = last_job_epoch_seconds - expected_feature_value_params.blind_spot
    window_end = epoch_seconds_to_timestamp(window_end_epoch_seconds)

    if expected_feature_value_params.window_size_secs is not None:
        window_start_epoch_seconds = (
            window_end_epoch_seconds - expected_feature_value_params.window_size_secs
        )
        window_start = epoch_seconds_to_timestamp(window_start_epoch_seconds)
    else:
        window_start = None

    mask = df[expected_feature_value_params.entity_column_name] == entity_value
    mask &= expected_feature_value_params.utc_event_timestamps < window_end
    if window_start is not None:
        mask &= expected_feature_value_params.utc_event_timestamps >= window_start
    df_filtered = df[mask]

    return apply_agg_func_on_filtered_dataframe(
        expected_feature_value_params.agg_func,
        expected_feature_value_params.category,
        df_filtered,
        variable_column_name,
    )


def calculate_aggregate_asat_ground_truth(
    point_in_time,
    entity_value,
    expected_feature_value_params: ExpectedSCDFeatureValueParams,
):
    """
    Reference implementation for aggregate_asat
    """
    df = expected_feature_value_params.df
    variable_column_name = expected_feature_value_params.variable_column_name
    if expected_feature_value_params.variable_column_name is None:
        # take any column because it doesn't matter
        variable_column_name = df.columns[0]

    df = df[df[expected_feature_value_params.effective_timestamp_column] <= point_in_time]

    def _extract_current_record(sub_df):
        latest_effective_timestamp = sub_df[
            expected_feature_value_params.effective_timestamp_column
        ].max()
        latest_sub_df = sub_df[
            sub_df[expected_feature_value_params.effective_timestamp_column]
            == latest_effective_timestamp
        ]
        if latest_sub_df.shape[0] == 0:
            return None
        assert latest_sub_df.shape[0] == 1
        return latest_sub_df.iloc[0]

    df_current = df.groupby(expected_feature_value_params.natural_key_column_name).apply(
        _extract_current_record
    )

    df_filtered = df_current[
        df_current[expected_feature_value_params.entity_column_name] == entity_value
    ]

    return apply_agg_func_on_filtered_dataframe(
        expected_feature_value_params.agg_func,
        expected_feature_value_params.category,
        df_filtered,
        variable_column_name,
    )


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


class FeatureKind(StrEnum):
    """
    Feature kind enum
    """

    AGGREGATE_OVER = "aggregate_over"
    AGGREGATE_AS_AT = "aggregate_as_at"


def get_expected_feature_values(
    kind: FeatureKind,
    observation_set: pd.DataFrame,
    feature_name: str,
    expected_feature_value_params: ExpectedFeatureValueParams,
):
    """
    Calculate the expected feature values given observation_set and feature parameters
    """
    expected_output = defaultdict(list)

    for _, row in observation_set.iterrows():
        entity_value = row[expected_feature_value_params.entity_column_name]
        point_in_time = row["POINT_IN_TIME"]
        if kind == FeatureKind.AGGREGATE_OVER:
            assert isinstance(expected_feature_value_params, ExpectedEventFeatureValueParams)
            val = calculate_aggregate_over_ground_truth(
                point_in_time=point_in_time,
                entity_value=entity_value,
                expected_feature_value_params=expected_feature_value_params,
            )
        elif kind == FeatureKind.AGGREGATE_AS_AT:
            assert isinstance(expected_feature_value_params, ExpectedSCDFeatureValueParams)
            val = calculate_aggregate_asat_ground_truth(
                point_in_time=point_in_time,
                entity_value=entity_value,
                expected_feature_value_params=expected_feature_value_params,
            )
        else:
            raise ValueError(f"Unknown kind {kind}")
        expected_output["POINT_IN_TIME"].append(point_in_time)
        expected_output[expected_feature_value_params.entity_column_name].append(entity_value)
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
    sampled_points = df_expected.sample(n=n_points, random_state=0).reset_index(drop=True)
    sampled_points.rename({"ÜSER ID": "üser id"}, axis=1, inplace=True)
    output = feature_list[feature_list.feature_names].preview(
        sampled_points[["POINT_IN_TIME", "üser id"]]
    )
    fb_assert_frame_equal(output, sampled_points, dict_like_columns)
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


@pytest.fixture(name="transaction_event_dataframe")
def transaction_event_dataframe_fixture(transaction_data_upper_case, event_table):
    """
    Fixture for transaction event dataframe
    """
    event_view = event_table.get_view()
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

    return transaction_data_upper_case.sort_values(event_timestamp_column_name)


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_feature_with_target(event_table, observation_set, transaction_data_upper_case):
    """
    Test that feature with target works
    """
    # Common values
    entity_column_name = "ÜSER ID"
    value_column = "ÀMOUNT"

    # Generate feature
    event_view = event_table.get_view()
    feature_job_setting = event_table.default_feature_job_setting
    frequency, time_modulo_frequency, blind_spot = validate_job_setting_parameters(
        frequency=feature_job_setting.frequency,
        time_modulo_frequency=feature_job_setting.time_modulo_frequency,
        blind_spot=feature_job_setting.blind_spot,
    )
    feature_group = event_view.groupby(entity_column_name).aggregate_over(
        method="sum",
        value_column=value_column,
        windows=["2h"],
        feature_names=["amount_2h"],
    )
    feature_amount_2h = feature_group["amount_2h"]
    feature_list = FeatureList([feature_amount_2h], name="feature_list")

    # Generate target table
    target = event_view.groupby(entity_column_name).forward_aggregate(
        method="sum",
        value_column=value_column,
        window="2h",
        target_name="target_next_amount_2h",
    )
    observation_table_name = f"target_observation_table_name_{ObjectId()}"
    target_observation_table = target.compute_target_table(
        observation_set, observation_table_name, serving_names_mapping={"üser id": "ÜSER ID"}
    )

    # Compute historical feature table
    historical_features_table = feature_list.compute_historical_feature_table(
        target_observation_table,
        historical_feature_table_name=f"historical_feature_table_name_{ObjectId()}",
        serving_names_mapping={"üser id": "ÜSER ID"},
    )
    df_historical_features = historical_features_table.to_pandas()
    # Verify that the target and amount columns are present
    assert "target_next_amount_2h" in df_historical_features
    assert "amount_2h" in df_historical_features

    # Calculate expected feature values
    event_timestamp_column_name = "ËVENT_TIMESTAMP"
    transaction_event_dataframe = transaction_data_upper_case.copy()
    utc_event_timestamps = pd.to_datetime(
        transaction_event_dataframe[event_timestamp_column_name], utc=True
    ).dt.tz_localize(None)
    df_expected_feature_values = get_expected_feature_values(
        FeatureKind.AGGREGATE_OVER,
        observation_set,
        "amount_2h",
        ExpectedEventFeatureValueParams(
            df=transaction_event_dataframe,
            entity_column_name=entity_column_name,
            variable_column_name=value_column,
            agg_func=sum_func,
            category=None,
            utc_event_timestamps=utc_event_timestamps,
            window_size_secs=int(pd.Timedelta("2h").total_seconds()),
            frequency=frequency,
            time_modulo_frequency=time_modulo_frequency,
            blind_spot=blind_spot,
        ),
    )
    df_expected_feature_values["POINT_IN_TIME"] = pd.to_datetime(
        df_expected_feature_values["POINT_IN_TIME"], utc=True
    ).dt.tz_localize(None)

    actual_feature_values_df = df_historical_features[["POINT_IN_TIME", "ÜSER ID", "amount_2h"]]
    fb_assert_frame_equal(
        actual_feature_values_df,
        df_expected_feature_values,
        sort_by_columns=["POINT_IN_TIME"],
    )


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_aggregate_over(
    transaction_event_dataframe,
    observation_set,
    event_table,
    config,
    feature_parameters,
):
    """
    Test that aggregate_over produces correct feature values
    """
    event_view = event_table.get_view()
    feature_job_setting = event_table.default_feature_job_setting
    frequency, time_modulo_frequency, blind_spot = validate_job_setting_parameters(
        frequency=feature_job_setting.frequency,
        time_modulo_frequency=feature_job_setting.time_modulo_frequency,
        blind_spot=feature_job_setting.blind_spot,
    )

    # Some fixed parameters
    entity_column_name = "ÜSER ID"
    event_timestamp_column_name = "ËVENT_TIMESTAMP"

    df = transaction_event_dataframe

    common_args = (
        df,
        entity_column_name,
        event_timestamp_column_name,
        event_view,
        feature_parameters,
        frequency,
        time_modulo_frequency,
        blind_spot,
    )
    check_aggregate_over(observation_set, *common_args)

    # Test with a subset of data but with POINT_IN_TIME shifted forward to trigger tile updates
    observation_set_new = observation_set.sample(n=100, random_state=0).reset_index(drop=True)
    observation_set_new["POINT_IN_TIME"] = observation_set_new["POINT_IN_TIME"] + pd.Timedelta("1d")
    check_aggregate_over(observation_set_new, *common_args)


def check_aggregate_over(
    observation_set,
    df,
    entity_column_name,
    event_timestamp_column_name,
    event_view,
    feature_parameters,
    frequency,
    time_modulo_frequency,
    blind_spot,
):
    df_expected_all = [observation_set]
    utc_event_timestamps = pd.to_datetime(df[event_timestamp_column_name], utc=True).dt.tz_localize(
        None
    )

    elapsed_time_ref = 0
    features = []
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
            FeatureKind.AGGREGATE_OVER,
            observation_set,
            feature_name,
            ExpectedEventFeatureValueParams(
                df=df,
                entity_column_name=entity_column_name,
                utc_event_timestamps=utc_event_timestamps,
                variable_column_name=variable_column_name,
                agg_func=agg_func_callable,
                window_size_secs=window_size,
                frequency=frequency,
                time_modulo_frequency=time_modulo_frequency,
                blind_spot=blind_spot,
                category=category,
            ),
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
    df_historical_features = feature_list.compute_historical_features(
        observation_set,
        serving_names_mapping={"üser id": "ÜSER ID"},
    )
    elapsed_historical = time.time() - tic
    logger.debug(f"elapsed historical: {elapsed_historical}")

    # expect point-in-time to be converted to UTC without timezone
    df_expected["POINT_IN_TIME"] = pd.to_datetime(
        df_expected["POINT_IN_TIME"], utc=True
    ).dt.tz_localize(None)

    fb_assert_frame_equal(df_historical_features, df_expected, dict_like_columns)


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_aggregate_asat(
    scd_dataframe,
    scd_observation_set,
    scd_table,
    config,
):
    """
    Test that aggregate_asat produces correct feature values
    """
    feature_parameters = [
        (None, "count", "asat_count", lambda x: len(x), None),
        (None, "count", "asat_count_by_day_of_month", lambda x: len(x), "day"),
    ]

    scd_view = scd_table.get_view()
    entity_column_name = "User Status"
    effective_timestamp_column = "Effective Timestamp"

    features = []
    df_expected_all = [scd_observation_set]

    scd_dataframe = scd_dataframe.copy()
    scd_dataframe[effective_timestamp_column] = pd.to_datetime(
        scd_dataframe[effective_timestamp_column], utc=True
    ).dt.tz_localize(None)

    scd_view["day"] = scd_view["Effective Timestamp"].dt.day
    scd_dataframe["day"] = scd_dataframe["Effective Timestamp"].dt.day

    for (
        variable_column_name,
        agg_name,
        feature_name,
        agg_func_callable,
        category,
    ) in feature_parameters:
        feature = scd_view.groupby(entity_column_name, category=category).aggregate_asat(
            method=agg_name,
            value_column=variable_column_name,
            feature_name=feature_name,
        )
        features.append(feature)

        df_expected = get_expected_feature_values(
            FeatureKind.AGGREGATE_AS_AT,
            scd_observation_set,
            feature_name,
            ExpectedSCDFeatureValueParams(
                df=scd_dataframe,
                effective_timestamp_column=effective_timestamp_column,
                natural_key_column_name="User ID",
                entity_column_name=entity_column_name,
                variable_column_name=variable_column_name,
                agg_func=agg_func_callable,
                category=category,
            ),
        )[[feature_name]]

        df_expected_all.append(df_expected)

    df_expected = pd.concat(df_expected_all, axis=1)
    feature_list = FeatureList(features, name="feature_list")

    # Check historical features
    df_historical_features = feature_list.compute_historical_features(
        scd_observation_set,
        serving_names_mapping={"user_status": "User Status"},
    )
    fb_assert_frame_equal(
        df_historical_features,
        df_expected,
        ["asat_count_by_day_of_month"],
        sort_by_columns=["POINT_IN_TIME", entity_column_name],
    )
