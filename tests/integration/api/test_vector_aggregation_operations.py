"""
Test vector aggregation operations module
"""
from typing import List

import json
import os

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio

from featurebyte.api.aggregator.vector_validator import VECTOR_AGGREGATE_SUPPORTED_FUNCTIONS
from featurebyte.enum import AggFunc
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
)


@pytest.fixture(name="data_with_array", scope="session")
def data_with_array_fixture():
    """
    Simulated data with an array column
    """
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "fixtures", "vector_data.csv"))
    # manually add the vectors on since it's easier than shoving it into a csv
    df["VECTOR_VALUE"] = [[3, 1, 3], [1, 3, 1], [1, 5, 9], [4, 8, 6], [7, 2, 3]]
    df["EVENT_TIMESTAMP"] = pd.to_datetime(df["EVENT_TIMESTAMP"].astype(str))
    yield df


def get_expected_dataframe(
    input_vector_data: pd.DataFrame,
    groupby_columns: List[str],
    vector_column: str,
    agg_func: AggFunc,
) -> pd.DataFrame:
    """
    Helper function to aggregate the vector column in a dataframe.
    """
    assert agg_func in VECTOR_AGGREGATE_SUPPORTED_FUNCTIONS
    g = input_vector_data.groupby(groupby_columns, as_index=False)

    def apply_function(xs):
        if agg_func == AggFunc.MAX:
            # cast to string so that the apply function works
            return str(list(np.max(np.array([x for x in xs]), axis=0)))
        if agg_func == AggFunc.AVG:
            return str(list(np.mean(np.array([x for x in xs]), axis=0)))
        raise NotImplementedError(f"AggFunc {agg_func} not implemented")

    out = g[vector_column].apply(apply_function)
    # convert string list to actual list
    out[vector_column] = out[vector_column].apply(lambda x: json.loads(x))
    return out


def test_random(data_with_array):
    """
    Test random
    """
    assert data_with_array.shape[0] == 5
    expected_dataframe = get_expected_dataframe(
        data_with_array, ["CUST_ID", "USER_ID"], "VECTOR_VALUE", AggFunc.AVG
    )
    assert expected_dataframe.shape[0] == 3


@pytest_asyncio.fixture(name="event_table_with_array_column", scope="session")
async def register_table_with_array_column(
    data_with_array, session, data_source, catalog, user_entity, customer_entity
):
    """
    Register a table with an array column
    """
    _ = catalog, user_entity, customer_entity
    table_name = "event_table_with_vector"
    await session.register_table(table_name, data_with_array, temporary=False)

    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=table_name,
    )
    event_table = database_table.create_event_table(
        name=table_name,
        event_id_column="EVENT_ID",
        event_timestamp_column="EVENT_TIMESTAMP",
    )
    event_table.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="30m", frequency="1h", time_modulo_frequency="30m"
        )
    )
    event_table["USER_ID"].as_entity("User")
    event_table["CUST_ID"].as_entity("Customer")
    return event_table


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
def test_vector_aggregation_operations(event_table_with_array_column):
    """
    Test vector aggregation operations
    """
    event_view = event_table_with_array_column.get_view()
    feature = event_view.groupby("CUST_ID").aggregate_over(
        value_column="VECTOR_VALUE",
        method=AggFunc.MAX,
        windows=["1d"],
        feature_names=["vector_max"],
        skip_fill_na=True,
    )["vector_max"]

    preview_params = {"POINT_IN_TIME": "2022-06-06 00:58:00", "cust_id": "1"}
    feature_preview = feature.preview(pd.DataFrame([preview_params]))
    assert feature_preview.shape[0] == 1
    assert feature_preview.iloc[0].to_dict() == {
        "vector_max": "[3,3,3]",
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }
