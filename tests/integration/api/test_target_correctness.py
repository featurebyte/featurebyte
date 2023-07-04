"""
Target correctness tests.
"""
import tempfile
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest

from tests.integration.api.test_feature_correctness import sum_func


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
        if target_parameter.skip:
            continue
        target = event_view.groupby(entity_column_name).forward_aggregate(
            method=target_parameter.agg_name,
            value_column=target_parameter.variable_column_name,
            horizon=target_parameter.horizon,
            target_name=target_parameter.target_name,
        )

        results = target.preview(sampled_points)
