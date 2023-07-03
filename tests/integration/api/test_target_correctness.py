"""
Target correctness tests.
"""
import tempfile
import time

import numpy as np
import pandas as pd
import pytest

from tests.integration.api.test_feature_correctness import sum_func


@pytest.fixture(name="target_parameters")
def target_parameters_fixture(source_type):
    """
    Parameters for feature tests using aggregate_over
    """
    parameters = [
        ("ÀMOUNT", "avg", "2h", "avg_2h", lambda x: x.mean()),
        # ("ÀMOUNT", "avg", "24h", "avg_24h", lambda x: x.mean()),
        # ("ÀMOUNT", "min", "24h", "min_24h", lambda x: x.min()),
        # ("ÀMOUNT", "max", "24h", "max_24h", lambda x: x.max()),
        # ("ÀMOUNT", "sum", "24h", "sum_24h", sum_func),
    ]
    if source_type == "spark":
        parameters = [param for param in parameters if param[1] in ["max", "std", "latest"]]
    return parameters


@pytest.fixture(scope="session")
def observation_set(transaction_data_upper_case):
    # TODO: copied from test_feature_correctness
    # Sample training time points from historical table
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


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_forward_aggregate(event_table, target_parameters, observation_set):
    """
    Test forward aggregate.
    """
    event_view = event_table.get_view()
    entity_column_name = "ÜSER ID"
    sampled_points = observation_set.sample(n=50, random_state=0).reset_index(drop=True)
    sampled_points.rename({"ÜSER ID": "üser id"}, axis=1, inplace=True)
    for target_parameter in target_parameters:
        variable_column_name, agg_name, horizon, target_name, agg_func = target_parameter
        target = event_view.groupby(entity_column_name).forward_aggregate(
            method=agg_name,
            value_column=variable_column_name,
            horizon=horizon,
            target_name=target_name,
        )

        results = target.preview(sampled_points)
