"""
Test helper functions in featurebyte.common.utils
"""
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from featurebyte.common.utils import (
    dataframe_from_arrow_stream,
    dataframe_from_json,
    dataframe_to_arrow_bytes,
    dataframe_to_json,
)
from featurebyte.enum import DBVarType


@pytest.fixture(name="data_to_convert")
def data_to_convert_fixture():
    """
    Dataframe fixture for conversion test
    """
    dataframe = pd.DataFrame(
        {
            "a": range(10),
            "b": [f"2020-01-03 12:00:00+{i:02d}:00" for i in range(10)],
        }
    )
    type_conversions = {"b": DBVarType.TIMESTAMP_TZ}
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
    assert_frame_equal(output_df, original_df)


def test_dataframe_to_json_no_column_name(data_to_convert):
    """
    Test test_dataframe_to_json for single column without name in conversion
    """
    original_df, _ = data_to_convert
    original_df = original_df[["b"]]
    type_conversions = {None: DBVarType.TIMESTAMP_TZ}
    data = dataframe_to_json(original_df, type_conversions)
    output_df = dataframe_from_json(data)
    # timestamp column should be casted to datetime with tz offsets
    original_df["b"] = pd.to_datetime(original_df["b"])
    assert_frame_equal(output_df, original_df)
