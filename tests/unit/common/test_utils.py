"""
Test helper functions in featurebyte.common.utils
"""
import numpy as np
import pandas as pd
import pytest
import toml
from pandas.testing import assert_frame_equal

from featurebyte.common.utils import (
    CodeStr,
    InfoDict,
    dataframe_from_arrow_stream,
    dataframe_from_json,
    dataframe_to_arrow_bytes,
    dataframe_to_json,
    get_version,
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


def test_codestr_formatting():
    """
    Test CodeStr formatting
    """
    code = CodeStr("import featurebyte")
    assert str(code) == "import featurebyte"
    assert code._repr_html_() == (
        '<div style="margin:30px; padding: 20px; border:1px solid #aaa">'
        '<div class="highlight"><pre style="line-height: 125%;"><span></span>'
        '<span style="color: #008000; font-weight: bold">import</span> '
        '<span style="color: #0000FF; font-weight: bold">featurebyte</span>\n'
        "</pre></div>\n</div>"
    )


def test_info_dict_formatting(update_fixtures):
    """
    Test InfoDict formatting
    """
    feature_list_info = {
        "name": "Small List",
        "created_at": "2023-05-03T14:30:54.217000",
        "updated_at": "2023-05-13T06:55:12.110000",
        "entities": [
            {
                "name": "grocerycustomer",
                "serving_names": ["GROCERYCUSTOMERGUID"],
                "catalog_name": "Grocery - playground (spark)",
            }
        ],
        "primary_entity": [
            {
                "name": "grocerycustomer",
                "serving_names": ["GROCERYCUSTOMERGUID"],
                "catalog_name": "Grocery - playground (spark)",
            }
        ],
        "tables": [
            {
                "name": "GROCERYPRODUCT",
                "status": "PUBLIC_DRAFT",
                "catalog_name": "Grocery - playground (spark)",
            },
            {
                "name": "INVOICEITEMS",
                "status": "PUBLIC_DRAFT",
                "catalog_name": "Grocery - playground (spark)",
            },
            {
                "name": "GROCERYINVOICE",
                "status": "PUBLIC_DRAFT",
                "catalog_name": "Grocery - playground (spark)",
            },
        ],
        "default_version_mode": "AUTO",
        "version_count": 1,
        "catalog_name": "Grocery - playground (spark)",
        "dtype_distribution": [
            {"dtype": "OBJECT", "count": 1},
            {"dtype": "FLOAT", "count": 8},
            {"dtype": "VARCHAR", "count": 1},
        ],
        "status": "PUBLIC_DRAFT",
        "feature_count": 10,
        "version": {"this": "V230503", "default": "V230503"},
        "production_ready_fraction": {"this": 1.0, "default": 1.0},
        "versions_info": None,
        "deployed": False,
        "class_name": "FeatureList",
    }

    feature_list_info_html = InfoDict(feature_list_info)._repr_html_()
    feature_list_info_fixture_path = "tests/fixtures/feature_list_info.html"
    if update_fixtures:
        with open(feature_list_info_fixture_path, "w") as file_obj:
            file_obj.write(feature_list_info_html)
    else:
        # check report
        with open(feature_list_info_fixture_path, "r") as file_obj:
            expected_html = file_obj.read()
        assert feature_list_info_html == expected_html
