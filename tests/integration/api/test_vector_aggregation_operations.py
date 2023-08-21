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
from featurebyte.exception import RecordRetrievalException
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
)

VECTOR_VALUE_FLOAT_COL = "VECTOR_VALUE_FLOAT"
VECTOR_VALUE_INT_COL = "VECTOR_VALUE_INT"
VECTOR_VALUE_MIXED_COL = "VECTOR_VALUE_MIXED"
VECTOR_VALUE_DIFF_LENGTH_COL = "VECTOR_VALUE_DIFFERENT_LENGTH"


def _update_df(df: pd.DataFrame, event_timestamp_col: str) -> pd.DataFrame:
    """
    Helper method to update df
    """
    # Manually add the vectors on since it's easier than shoving it into a csv
    # The first two rows will belong to CUST_ID 1, and the last three rows will belong to CUST_ID 3
    df[VECTOR_VALUE_FLOAT_COL] = [
        [3.0, 1.0, 3.0],
        [1.0, 3.0, 1.0],
        [1.0, 5.0, 9.0],
        [4.0, 8.0, 6.0],
        [7.0, 2.0, 3.0],
    ]
    df[VECTOR_VALUE_INT_COL] = [
        [3, 1, 3],
        [1, 3, 1],
        [1, 5, 9],
        [4, 8, 6],
        [7, 2, 3],
    ]
    df[VECTOR_VALUE_MIXED_COL] = [
        [3, 1.0, 3],
        [1.0, 3, 1],
        [1, 5, 9.0],
        [4.0, 8.0, 6],
        [7, 2, 3],
    ]
    df[VECTOR_VALUE_DIFF_LENGTH_COL] = [
        [3, 1, 3],
        [1],
        None,
        [4, 8],
        [],
    ]
    df[event_timestamp_col] = pd.to_datetime(df[event_timestamp_col].astype(str))
    return df


@pytest.fixture(name="item_data_with_array", scope="session")
def item_data_with_array_fixture():
    """
    Simulated data with an array column
    """
    df = pd.read_csv(
        os.path.join(os.path.dirname(__file__), "fixtures", "vector_data_item_data.csv")
    )
    yield _update_df(df, "EVENT_TIMESTAMP_ITEM")


@pytest.fixture(name="event_data_with_array", scope="session")
def event_data_with_array_fixture():
    """
    Simulated data with an array column
    """
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "fixtures", "vector_data.csv"))
    yield _update_df(df, "EVENT_TIMESTAMP")


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


@pytest_asyncio.fixture(name="event_table_with_array_column", scope="session")
async def register_table_with_array_column(
    event_data_with_array, session, data_source, catalog, user_entity, customer_entity, order_entity
):
    """
    Register a table with an array column
    """
    _ = catalog, user_entity, customer_entity, order_entity
    table_name = "event_table_with_vector"
    await session.register_table(table_name, event_data_with_array, temporary=False)

    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=table_name,
    )
    event_table = database_table.create_event_table(
        name=table_name,
        event_id_column="ORDER_ID",
        event_timestamp_column="EVENT_TIMESTAMP",
    )
    event_table.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="30m", frequency="1h", time_modulo_frequency="30m"
        )
    )
    event_table["USER_ID"].as_entity("User")
    event_table["CUST_ID"].as_entity("Customer")
    event_table["ORDER_ID"].as_entity("Order")
    return event_table


@pytest_asyncio.fixture(name="item_table_with_array_column", scope="session")
async def register_item_table_with_array_column(
    event_table_with_array_column, item_data_with_array, session, data_source, catalog, item_entity
):
    """
    Register a table with an array column
    """
    _ = item_entity
    table_name = "item_table_with_vector"
    await session.register_table(table_name, item_data_with_array, temporary=False)

    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=table_name,
    )
    item_table = database_table.create_item_table(
        name=table_name,
        event_id_column="ORDER_ID",
        item_id_column="ITEM_ID",
        event_table_name=event_table_with_array_column.name,
    )
    item_table["USER_ID_ITEM"].as_entity("User")
    item_table["CUST_ID_ITEM"].as_entity("Customer")
    item_table["ITEM_ID"].as_entity("Item")
    item_table["ORDER_ID"].as_entity("Order")
    return item_table


TEST_CASES = [
    (AggFunc.MAX, "[3.0,3.0,3.0]", VECTOR_VALUE_FLOAT_COL),
    (AggFunc.AVG, "[2.0,2.0,2.0]", VECTOR_VALUE_FLOAT_COL),
    (AggFunc.SUM, "[4.0,4.0,4.0]", VECTOR_VALUE_FLOAT_COL),
    (AggFunc.MAX, "[3.0,3.0,3.0]", VECTOR_VALUE_MIXED_COL),
    (AggFunc.AVG, "[2.0,2.0,2.0]", VECTOR_VALUE_MIXED_COL),
    (AggFunc.SUM, "[4.0,4.0,4.0]", VECTOR_VALUE_MIXED_COL),
    (AggFunc.MAX, "[3.0,3.0,3.0]", VECTOR_VALUE_INT_COL),
    (AggFunc.AVG, "[2.0,2.0,2.0]", VECTOR_VALUE_INT_COL),
    (AggFunc.SUM, "[4.0,4.0,4.0]", VECTOR_VALUE_INT_COL),
]


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.parametrize(
    "agg_func,expected_results,vector_value_column",
    TEST_CASES,
)
def test_vector_aggregation_operations__aggregate_over(
    event_table_with_array_column, agg_func, expected_results, vector_value_column
):
    """
    Test vector aggregation operations
    """
    event_view = event_table_with_array_column.get_view()
    feature_name = "vector_agg"
    feature = event_view.groupby("CUST_ID").aggregate_over(
        value_column=vector_value_column,
        method=agg_func,
        windows=["1d"],
        feature_names=[feature_name],
        skip_fill_na=True,
    )[feature_name]

    preview_params = {"POINT_IN_TIME": "2022-06-06 00:58:00", "cust_id": "1"}
    feature_preview = feature.preview(pd.DataFrame([preview_params]))
    assert feature_preview.shape[0] == 1
    assert feature_preview.iloc[0].to_dict() == {
        feature_name: expected_results,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.parametrize(
    "agg_func,expected_results,vector_value_column",
    TEST_CASES,
)
def test_vector_aggregation_operations__aggregate(
    item_table_with_array_column, agg_func, expected_results, vector_value_column
):
    """
    Test vector aggregation operations
    """
    item_view = item_table_with_array_column.get_view()
    feature_name = "vector_agg"
    feature = item_view.groupby("ORDER_ID").aggregate(
        value_column=vector_value_column,
        method=agg_func,
        feature_name=feature_name,
        skip_fill_na=True,
    )

    preview_params = {"POINT_IN_TIME": "2022-06-06 00:58:00", "order_id": "1000"}
    feature_preview = feature.preview(pd.DataFrame([preview_params]))
    assert feature_preview.shape[0] == 1
    assert feature_preview.iloc[0].to_dict() == {
        feature_name: expected_results,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.parametrize("cust_id", [1, 3])
def test_vector_aggregation_operations_fails_for_vectors_of_different_lengths(
    event_table_with_array_column, cust_id
):
    """
    Test vector aggregation operations fails for vectors of different lengths
    """
    event_view = event_table_with_array_column.get_view()
    feature_name = "vector_agg"
    feature = event_view.groupby("CUST_ID").aggregate_over(
        value_column=VECTOR_VALUE_DIFF_LENGTH_COL,
        method=AggFunc.MAX,
        windows=["1d"],
        feature_names=[feature_name],
        skip_fill_na=True,
    )[feature_name]

    preview_params = {"POINT_IN_TIME": "2022-06-06 00:58:00", "cust_id": f"{cust_id}"}
    with pytest.raises(RecordRetrievalException):
        feature.preview(pd.DataFrame([preview_params]))
