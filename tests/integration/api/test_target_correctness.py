"""
Target correctness tests.
"""

from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte import AggFunc
from featurebyte.common.model_util import parse_duration_string
from tests.integration.api.dataframe_helper import apply_agg_func_on_filtered_dataframe
from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
)
from tests.integration.api.test_feature_correctness import sum_func
from tests.util.helper import (
    create_observation_table_from_dataframe,
    fb_assert_frame_equal,
    tz_localize_if_needed,
)


@dataclass
class TargetParameter:
    """
    Target parameter for testing
    """

    variable_column_name: str
    agg_name: str
    window: str
    target_name: str
    agg_func: callable


@pytest.fixture(name="target_parameters")
def target_parameters_fixture():
    """
    Parameters for feature tests using aggregate_over
    """
    parameters = [
        TargetParameter("ÀMOUNT", "avg", "2h", "avg_2h", lambda x: x.mean()),
        TargetParameter("ÀMOUNT", "avg", "24h", "avg_24h", lambda x: x.mean()),
        TargetParameter("ÀMOUNT", "min", "24h", "min_24h", lambda x: x.min()),
        TargetParameter(
            "ÀMOUNT",
            "max",
            "24h",
            "max_24h",
            lambda x: x.max(),
        ),
        TargetParameter("ÀMOUNT", "sum", "24h", "sum_24h", sum_func),
        TargetParameter("ÀMOUNT", "sum", "24h-12h", "sum_24h_offset_12h", sum_func),
    ]
    return parameters


def transform_and_sample_observation_set(observation_set: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and sample observation set.

    - Transform POINT_IN_TIME to datetime
    - Sample 25 points
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

    sampled_points = observation_set_copied.sample(n=25, random_state=0).reset_index(drop=True)
    sampled_points.rename({"ÜSER ID": "üser id"}, axis=1, inplace=True)
    return sampled_points


def calculate_forward_aggregate_ground_truth(
    df: pd.DataFrame,
    point_in_time,
    utc_event_timestamps,
    entity_column_name: str,
    entity_value,
    variable_column_name: str,
    agg_func: callable,
    window: Optional[int],
    offset: Optional[int],
    category=None,
):
    """
    Reference implementation for forward_aggregate that is as simple as possible
    """
    # Find the window of datapoints that are relevant
    # convert point_in_time to utc
    point_in_time = pd.Timestamp(point_in_time)
    pit_utc = point_in_time.tz_convert("UTC").tz_localize(None)
    window_start = pit_utc

    if offset is not None:
        window_start += pd.Timedelta(offset, unit="s")

    if window is not None:
        window_end = window_start + pd.Timedelta(window, unit="s")
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
    agg_func: callable,
    window: Optional[int],
    offset: Optional[int],
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
            window=window,
            offset=offset,
            category=category,
        )
        expected_output["POINT_IN_TIME"].append(point_in_time)
        expected_output[entity_column_name].append(entity_value)
        expected_output[target_name].append(val)
    df_expected = pd.DataFrame(expected_output, index=observation_set.index)
    return df_expected


def test_forward_aggregate(
    event_table, target_parameters, transaction_data_upper_case, observation_set, user_entity
):
    """
    Test forward aggregate.
    """
    event_view = event_table.get_view()
    entity_column_name = "ÜSER ID"
    event_timestamp_column_name = "ËVENT_TIMESTAMP"
    df = transaction_data_upper_case.sort_values(event_timestamp_column_name)

    for target_parameter in target_parameters:
        if "-" in target_parameter.window:
            window, offset = target_parameter.window.split("-", 1)
        else:
            window, offset = target_parameter.window, None

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
            window=parse_duration_string(window),
            offset=parse_duration_string(offset) if offset is not None else None,
        )

        # Transform and sample to get a smaller sample dataframe just for preview
        preview_expected_values = transform_and_sample_observation_set(expected_values)

        # Build actual Target preview results
        target = event_view.groupby(entity_column_name).forward_aggregate(
            method=target_parameter.agg_name,
            value_column=target_parameter.variable_column_name,
            window=window,
            offset=offset,
            target_name=target_parameter.target_name,
            fill_value=None,
        )
        results = target.preview(preview_expected_values[["POINT_IN_TIME", "üser id"]])

        # Compare actual preview, against sampled results
        fb_assert_frame_equal(results, preview_expected_values)

        # Build full materialized Target
        df_targets = target.compute_targets(
            observation_set, serving_names_mapping={"üser id": "ÜSER ID"}
        )
        expected_values["POINT_IN_TIME"] = pd.to_datetime(
            expected_values["POINT_IN_TIME"], utc=True
        ).dt.tz_localize(None)
        fb_assert_frame_equal(df_targets, expected_values)

        # Build full materialized Target observation table
        observation_table_name = f"target_observation_table_name_{ObjectId()}"
        target_observation_table = target.compute_target_table(
            observation_set, observation_table_name, serving_names_mapping={"üser id": "ÜSER ID"}
        )
        assert target_observation_table.entity_ids == [user_entity.id]
        assert target_observation_table.primary_entity_ids == [user_entity.id]
        dataframe = target_observation_table.to_pandas()
        fb_assert_frame_equal(dataframe, expected_values, sort_by_columns=["POINT_IN_TIME"])


def test_forward_aggregate_with_count_and_value_column_none(event_table, source_type):
    """
    Test forward aggregate with count and value column None.
    """
    event_view = event_table.get_view()
    count_target = event_view.groupby("ÜSER ID").forward_aggregate(
        method=AggFunc.COUNT,
        value_column=None,
        window="7d",
        target_name="count_target",
        fill_value=None,
    )
    preview_params = {"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 1}
    target_preview = count_target.preview(pd.DataFrame([preview_params]))
    tz_localize_if_needed(target_preview, source_type)
    assert target_preview.iloc[0].to_dict() == {
        "count_target": 12,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.asyncio
async def test_bool_target(event_table, source_type, session, data_source):
    """
    Test boolean type target
    """
    event_view = event_table.get_view()
    count_target = event_view.groupby("ÜSER ID").forward_aggregate(
        method=AggFunc.COUNT,
        value_column=None,
        window="7d",
        target_name="count_target",
        fill_value=None,
    )
    new_target = count_target > 3
    new_target.name = "count_target_bool"
    new_target.save()
    df_preview = pd.DataFrame([
        {"POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"), "üser id": 1}
    ])
    observation_table = await create_observation_table_from_dataframe(
        session,
        df_preview,
        data_source,
    )
    target_table = new_target.compute_target_table(
        observation_table, "target table (test_bool_target)"
    )
    df_target_table = target_table.to_pandas()
    df_expected = df_preview.copy()
    df_expected["count_target_bool"] = True
    fb_assert_frame_equal(df_target_table, df_expected)
    assert df_target_table["count_target_bool"].dtype == "bool"
