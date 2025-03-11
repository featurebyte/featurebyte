"""
Test vector aggregation operations module
"""

import json
import os
from typing import List
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio

from featurebyte import FeatureGroup
from featurebyte.api.aggregator.vector_validator import VECTOR_AGGREGATE_SUPPORTED_FUNCTIONS
from featurebyte.api.entity import Entity
from featurebyte.api.feature_list import FeatureList
from featurebyte.enum import AggFunc
from featurebyte.exception import RecordRetrievalException
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
)
from tests.source_types import SNOWFLAKE_AND_SPARK

VECTOR_VALUE_FLOAT_COL = "VECTOR_VALUE_FLOAT"
VECTOR_VALUE_INT_COL = "VECTOR_VALUE_INT"
VECTOR_VALUE_MIXED_COL = "VECTOR_VALUE_MIXED"
VECTOR_VALUE_DIFF_LENGTH_COL = "VECTOR_VALUE_DIFFERENT_LENGTH"


@pytest.fixture(autouse=True, scope="module")
def always_patch_initialize_entity_dtype():
    """
    Patch initialize_entity_dtype
    """
    module_base_path = (
        "featurebyte.service.table_columns_info.EntityDtypeInitializationAndValidationService"
    )
    patched = {}
    patch_targets = [
        "maybe_initialize_entity_dtype",
        "validate_entity_dtype",
        "update_entity_dtype",
    ]
    started_patchers = []
    for patch_target in patch_targets:
        patcher = patch(f"{module_base_path}.{patch_target}")
        patched[patch_target] = patcher.start()
        started_patchers.append(patcher)
    yield started_patchers
    for patcher in started_patchers:
        patcher.stop()


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


@pytest.fixture(name="item_data_with_array", scope="module")
def item_data_with_array_fixture():
    """
    Simulated data with an array column
    """
    df = pd.read_csv(
        os.path.join(os.path.dirname(__file__), "fixtures", "vector_data_item_data.csv")
    )
    for col in ["ORDER_ID", "CUST_ID_ITEM", "USER_ID_ITEM", "ITEM_ID"]:
        df[col] = df[col].astype(str)
    yield _update_df(df, "EVENT_TIMESTAMP_ITEM")


@pytest.fixture(name="event_data_with_array", scope="module")
def event_data_with_array_fixture():
    """
    Simulated data with an array column
    """
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "fixtures", "vector_data.csv"))
    for col in ["ORDER_ID", "CUST_ID", "USER_ID"]:
        df[col] = df[col].astype(str)
    yield _update_df(df, "EVENT_TIMESTAMP")


@pytest.fixture(name="scd_data_with_array", scope="module")
def scd_data_with_array_fixture():
    """
    Simulated data with an array column
    """
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "fixtures", "vector_data_scd.csv"))
    for col in ["ITEM_ID", "CUST_ID", "USER_ID"]:
        df[col] = df[col].astype(str)
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


@pytest.fixture(name="vector_user_entity", scope="module")
def vector_user_entity_fixture(catalog):
    """
    Fixture for an Entity "User"
    """
    _ = catalog
    entity = Entity(name="Vector User", serving_names=["vector_user_id"])
    entity.save()
    return entity


@pytest.fixture(name="vector_order_entity", scope="module")
def vector_order_entity_fixture(catalog):
    """
    Fixture for an Entity "User"
    """
    _ = catalog
    entity = Entity(name="Vector Order", serving_names=["vector_order_id"])
    entity.save()
    return entity


@pytest_asyncio.fixture(name="event_table_with_array_column", scope="module")
async def register_table_with_array_column(
    event_data_with_array,
    session_without_datasets,
    data_source,
    catalog,
    vector_order_entity,
    vector_user_entity,
):
    """
    Register a table with an array column
    """
    _ = catalog
    table_name = "event_table_with_vector"
    session = session_without_datasets
    await session.register_table(table_name, event_data_with_array)

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
        feature_job_setting=FeatureJobSetting(blind_spot="30m", period="1h", offset="30m")
    )
    event_table["USER_ID"].as_entity(vector_user_entity.name)
    event_table["ORDER_ID"].as_entity(vector_order_entity.name)
    return event_table


@pytest_asyncio.fixture(name="item_table_with_array_column", scope="module")
async def register_item_table_with_array_column(
    event_table_with_array_column,
    item_data_with_array,
    session_without_datasets,
    data_source,
    item_entity,
    vector_order_entity,
):
    """
    Register a table with an array column
    """
    table_name = "item_table_with_vector"
    session = session_without_datasets
    await session.register_table(table_name, item_data_with_array)

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
    item_table["ITEM_ID"].as_entity(item_entity.name)
    item_table["ORDER_ID"].as_entity(vector_order_entity.name)
    return item_table


@pytest_asyncio.fixture(name="scd_table_with_array_column", scope="module")
async def register_scd_table_with_array_column(
    scd_data_with_array, session_without_datasets, data_source, catalog, vector_user_entity
):
    """
    Register a table with an array column
    """
    table_name = "scd_table_with_vector"
    session = session_without_datasets
    await session.register_table(table_name, scd_data_with_array)

    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=table_name,
    )
    scd_table = database_table.create_scd_table(
        name=table_name,
        natural_key_column="ITEM_ID",
        effective_timestamp_column="EVENT_TIMESTAMP",
    )
    scd_table["USER_ID"].as_entity(vector_user_entity.name)
    return scd_table


TEST_CASES = [
    (AggFunc.MAX, [3.0, 3.0, 3.0], VECTOR_VALUE_FLOAT_COL),
    (AggFunc.AVG, [2.0, 2.0, 2.0], VECTOR_VALUE_FLOAT_COL),
    (AggFunc.SUM, [4.0, 4.0, 4.0], VECTOR_VALUE_FLOAT_COL),
    (AggFunc.MAX, [3.0, 3.0, 3.0], VECTOR_VALUE_MIXED_COL),
    (AggFunc.AVG, [2.0, 2.0, 2.0], VECTOR_VALUE_MIXED_COL),
    (AggFunc.SUM, [4.0, 4.0, 4.0], VECTOR_VALUE_MIXED_COL),
    (AggFunc.MAX, [3.0, 3.0, 3.0], VECTOR_VALUE_INT_COL),
    (AggFunc.AVG, [2.0, 2.0, 2.0], VECTOR_VALUE_INT_COL),
    (AggFunc.SUM, [4.0, 4.0, 4.0], VECTOR_VALUE_INT_COL),
]


@pytest.fixture(params=TEST_CASES)
def test_case(request, source_type):
    """
    Test case
    """
    _, _, vector_value_column = request.param
    if source_type == "bigquery" and vector_value_column == VECTOR_VALUE_INT_COL:
        pytest.skip("Aggregating ARRAY<INT64> is currently not supported in BigQuery")
    return request.param


def test_vector_aggregation_operations__aggregate_over(
    event_table_with_array_column,
    test_case,
):
    """
    Test vector aggregation operations
    """
    agg_func, expected_results, vector_value_column = test_case
    event_view = event_table_with_array_column.get_view()
    feature_name = "vector_agg"
    feature = event_view.groupby("USER_ID").aggregate_over(
        value_column=vector_value_column,
        method=agg_func,
        windows=["1d"],
        feature_names=[feature_name],
        skip_fill_na=True,
    )[feature_name]

    preview_params = {"POINT_IN_TIME": "2022-06-06 00:58:00", "vector_user_id": "2"}
    feature_preview = feature.preview(pd.DataFrame([preview_params]))
    assert feature_preview.shape[0] == 1
    assert feature_preview.iloc[0].to_dict() == {
        feature_name: expected_results,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


def test_vector_aggregation_operations__aggregate_over_compute_historical_features(
    event_table_with_array_column,
):
    """
    Test vector aggregation operations
    """
    event_view = event_table_with_array_column.get_view()
    feature_name = "vector_agg"
    feature = event_view.groupby("USER_ID").aggregate_over(
        value_column=VECTOR_VALUE_FLOAT_COL,
        method=AggFunc.MAX,
        windows=["1d"],
        feature_names=[feature_name],
        skip_fill_na=True,
    )[feature_name]

    preview_params = {"POINT_IN_TIME": "2022-06-06 00:58:00", "vector_user_id": "2"}
    feature_list = FeatureList([feature], name="vector_agg_list")
    observation_set_df = pd.DataFrame([preview_params])
    historical_features = feature_list.compute_historical_features(observation_set_df)
    assert historical_features.shape[0] == 1
    assert list(historical_features.iloc[0][feature_name]) == [3.0, 3.0, 3.0]


def test_vector_aggregation_operations__aggregate(item_table_with_array_column, test_case):
    """
    Test vector aggregation operations
    """
    agg_func, expected_results, vector_value_column = test_case
    item_view = item_table_with_array_column.get_view()
    feature_name = "vector_agg"
    feature = item_view.groupby("ORDER_ID").aggregate(
        value_column=vector_value_column,
        method=agg_func,
        feature_name=feature_name,
        skip_fill_na=True,
    )

    preview_params = {"POINT_IN_TIME": "2022-06-06 00:58:00", "vector_order_id": "1000"}
    feature_preview = feature.preview(pd.DataFrame([preview_params]))
    assert feature_preview.shape[0] == 1
    assert feature_preview.iloc[0].to_dict() == {
        feature_name: expected_results,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


def test_vector_aggregation_operations__aggregate_asat(scd_table_with_array_column, test_case):
    """
    Test vector aggregation operations
    """
    agg_func, expected_results, vector_value_column = test_case
    item_view = scd_table_with_array_column.get_view()
    feature_name = "vector_agg"
    feature = item_view.groupby("USER_ID").aggregate_asat(
        value_column=vector_value_column,
        method=agg_func,
        feature_name=feature_name,
        skip_fill_na=True,
    )

    preview_params = {"POINT_IN_TIME": "2022-06-06 00:58:00", "vector_user_id": "2"}
    feature_preview = feature.preview(pd.DataFrame([preview_params]))
    assert feature_preview.shape[0] == 1
    assert feature_preview.iloc[0].to_dict() == {
        feature_name: expected_results,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", SNOWFLAKE_AND_SPARK, indirect=True)
@pytest.mark.parametrize("user_id", [2, 4])
def test_vector_aggregation_operations_fails_for_vectors_of_different_lengths(
    event_table_with_array_column, user_id
):
    """
    Test vector aggregation operations fails for vectors of different lengths
    """
    event_view = event_table_with_array_column.get_view()
    feature_name = "vector_agg"
    feature = event_view.groupby("USER_ID").aggregate_over(
        value_column=VECTOR_VALUE_DIFF_LENGTH_COL,
        method=AggFunc.MAX,
        windows=["1d"],
        feature_names=[feature_name],
        skip_fill_na=True,
    )[feature_name]

    preview_params = {"POINT_IN_TIME": "2022-06-06 00:58:00", "vector_user_id": f"{user_id}"}
    with pytest.raises(RecordRetrievalException):
        feature.preview(pd.DataFrame([preview_params]))


def test_vector_cosine_similarity(item_table_with_array_column):
    """
    Test vector cosine similarity
    """
    item_view = item_table_with_array_column.get_view()
    feature_name = "vector_agg"
    feature = item_view.groupby("ORDER_ID").aggregate(
        value_column=VECTOR_VALUE_FLOAT_COL,
        method=AggFunc.MAX,
        feature_name=feature_name,
        skip_fill_na=True,
    )
    feature_group = FeatureGroup([feature])
    cosine_similarity_feature_name = "vector_cosine_similarity"
    feature_group[cosine_similarity_feature_name] = feature.vec.cosine_similarity(feature)

    preview_params = {"POINT_IN_TIME": "2022-06-06 00:58:00", "vector_order_id": "1000"}
    feature_preview = feature_group[cosine_similarity_feature_name].preview(
        pd.DataFrame([preview_params])
    )
    assert feature_preview.shape[0] == 1
    assert (
        feature_preview.iloc[0].to_dict()
        == {
            cosine_similarity_feature_name: 1.0,  # we expect cosine_similarity of 1 since the array is compared w/ itself
            **convert_preview_param_dict_to_feature_preview_resp(preview_params),
        }
    )


def test_vector_value_column_latest_aggregation(event_table_with_array_column):
    """
    Test latest aggregation on vector column
    """
    event_view = event_table_with_array_column.get_view()
    feature_name = "vector_agg"
    feature = event_view.groupby("USER_ID").aggregate_over(
        value_column=VECTOR_VALUE_FLOAT_COL,
        method=AggFunc.LATEST,
        windows=["1d"],
        feature_names=[feature_name],
        skip_fill_na=True,
    )[feature_name]

    preview_params = pd.DataFrame({
        "POINT_IN_TIME": ["2022-06-06 00:58:00"] * 2,
        "vector_user_id": ["2", "4"],
    })
    feature_preview = feature.preview(preview_params)
    expected_results = [[1.0, 3.0, 1.0], [7.0, 2.0, 3.0]]
    assert feature_preview[feature_name].tolist() == expected_results

    feature_list = FeatureList([feature], name="vector_agg_list")
    historical_features = feature_list.compute_historical_features(preview_params)
    assert [list(x) for x in historical_features[feature_name]] == expected_results
