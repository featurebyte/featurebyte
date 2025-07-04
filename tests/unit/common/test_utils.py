"""
Test helper functions in featurebyte.common.utils
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import toml
from pandas.testing import assert_frame_equal

from featurebyte.common.utils import (
    ARROW_METADATA_DB_VAR_TYPE,
    dataframe_from_arrow_stream,
    dataframe_from_arrow_table,
    dataframe_from_json,
    dataframe_to_arrow_bytes,
    dataframe_to_json,
    get_version,
)
from featurebyte.enum import DBVarType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.feature import BatchFeatureCreate, BatchFeatureItem
from featurebyte.schema.feature_list import FeatureListCreateWithBatchFeatureCreation


@pytest.fixture(name="data_to_convert")
def data_to_convert_fixture():
    """
    Dataframe fixture for conversion test
    """
    dataframe = pd.DataFrame({
        "a": range(10),
        "b": [f"2020-01-03 12:00:00+{i:02d}:00" for i in range(10)],
        "c": [f"2020-01-03 12:00:{i:02d}" for i in range(10)],
        "d": [f"2020-01-{i:02d}" for i in range(1, 11)],
    })
    type_conversions = {"b": DBVarType.TIMESTAMP_TZ, "c": DBVarType.TIMESTAMP, "d": DBVarType.DATE}
    return dataframe, type_conversions


def test_dataframe_to_arrow_bytes(data_to_convert):
    """
    Test dataframe_to_arrow_bytes
    """
    original_df, _ = data_to_convert
    data = dataframe_to_arrow_bytes(original_df)
    output_df = dataframe_from_arrow_stream(data)
    assert_frame_equal(output_df, original_df)


def test_dataframe_to_json(data_to_convert):
    """
    Test test_dataframe_to_json
    """
    original_df, type_conversions = data_to_convert
    data = dataframe_to_json(original_df, type_conversions)
    output_df = dataframe_from_json(data)
    # timestamp column should be casted to datetime with tz offsets
    original_df["b"] = pd.to_datetime(original_df["b"])
    original_df["c"] = pd.to_datetime(original_df["c"])
    original_df["d"] = pd.to_datetime(original_df["d"]).apply(lambda x: x.date())
    assert_frame_equal(output_df, original_df)


def test_dataframe_to_json_no_column_name(data_to_convert):
    """
    Test test_dataframe_to_json for single column without name in conversion
    """
    original_df, _ = data_to_convert
    original_df = original_df[["b"]].copy()
    type_conversions = {None: DBVarType.TIMESTAMP_TZ}
    data = dataframe_to_json(original_df, type_conversions)
    output_df = dataframe_from_json(data)
    # timestamp column should be casted to datetime with tz offsets
    original_df["b"] = pd.to_datetime(original_df["b"])
    assert_frame_equal(output_df, original_df)


def test_dataframe_to_json_infinite_values():
    """
    Test test_dataframe_to_json for single column without name in conversion
    """
    original_df = pd.DataFrame({"a": [1, 2, np.inf, -np.inf, np.nan]})
    expected_df = pd.DataFrame({"a": [1, 2, "inf", "-inf", np.nan]})
    data = dataframe_to_json(original_df, {})
    output_df = dataframe_from_json(data)
    assert_frame_equal(output_df, expected_df)


def test_get_version():
    """
    Test get_version
    """
    data = toml.load("pyproject.toml")
    assert get_version() == data["tool"]["poetry"]["version"]


def create_batch_feature_items_and_graph(features):
    """create batch feature items & graph"""
    query_graph = QueryGraph()
    feature_items = []
    for feature in features:
        query_graph, node_name_map = query_graph.load(feature.graph)
        feature_items.append(
            BatchFeatureItem(
                id=feature.id,
                name=feature.name,
                node_name=node_name_map[feature.node_name],
                tabular_source=feature.tabular_source,
            )
        )

    return feature_items, query_graph


def create_batch_feature_create(features):
    """Create batch feature create object"""
    feature_items, query_graph = create_batch_feature_items_and_graph(features)
    return BatchFeatureCreate(graph=query_graph, features=feature_items)


def create_feature_list_batch_feature_create(
    features, feature_list_name, feature_list_id, conflict_resolution
):
    """Create feature list batch feature create object"""
    feature_items, query_graph = create_batch_feature_items_and_graph(features)
    return FeatureListCreateWithBatchFeatureCreation(
        _id=feature_list_id,
        name=feature_list_name,
        conflict_resolution=conflict_resolution,
        graph=query_graph,
        features=feature_items,
    )


def test_dataframe_from_arrow_table():
    """
    Test dataframe_from_arrow_table for single column without name in conversion
    """
    schema = pa.schema([
        pa.field("value", pa.string(), metadata={ARROW_METADATA_DB_VAR_TYPE: "DICT"}),
    ])
    table = pa.Table.from_pandas(
        pd.DataFrame({
            "value": [
                '{"a": "\n", "b": null}',  # control character
                '{"a": nan, "b": 1}',  # invalid json
                None,
            ]
        }),
        schema=schema,
    )
    assert dataframe_from_arrow_table(table).value.tolist() == [{"a": "\n", "b": None}, None, None]
