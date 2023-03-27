"""
Integration tests related to DimensionView
"""

import pandas as pd
import pytest

from featurebyte import Feature
from featurebyte.common.typing import is_scalar_nan
from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
)


@pytest.fixture(name="item_type_dimension_lookup_feature")
def item_type_dimension_lookup_feature_fixture(dimension_view):
    """
    Get item type dimension lookup feature
    """
    return dimension_view["item_type"].as_feature("ItemTypeFeature")


@pytest.fixture(name="count_item_type_dictionary_feature")
def count_item_type_dictionary_feature_fixture(item_table):
    """
    Get count item type dictionary feature.

    Feature is grouped by ORDER_ID, with ITEM_TYPE as their category.
    """
    item_table = item_table.get_view()
    return item_table.groupby("order_id", category="item_type").aggregate(
        method="count",
        feature_name="COUNT_ITEM_TYPE",
    )


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_dimension_lookup_features(dimension_view):
    """
    Test lookup features from DimensionView
    """
    feature = dimension_view["item_type"].as_feature("ItemTypeFeature")

    # Test single lookup feature
    preview_params = {"item_id": "item_42"}
    df = feature.preview(pd.DataFrame([preview_params]))
    assert df.iloc[0].to_dict() == {
        "ItemTypeFeature": "type_42",
        **preview_params,
    }

    # Test multiple lookup features
    feature_group = dimension_view.as_features(
        column_names=["item_name", "item_type"],
        feature_names=["ItemNameFeature", "ItemTypeFeature"],
    )
    df = feature_group.preview(pd.DataFrame([preview_params]))
    assert df.iloc[0].to_dict() == {
        "ItemNameFeature": "name_42",
        "ItemTypeFeature": "type_42",
        **preview_params,
    }


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_is_in_view_column__target_is_array(event_table):
    """
    Test view column's isin() method with scalar sequence as target
    """
    event_view = event_table.get_view()

    # Use isin() to construct a boolean column used for filtering
    fixed_sequence = ["àdd", "rëmove"]
    condition = event_view["PRODUCT_ACTION"].isin(fixed_sequence)
    filtered_view = event_view[condition]

    # Check output is expected
    df = filtered_view.preview(100)
    assert df["PRODUCT_ACTION"].isin(fixed_sequence).all()


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_is_in_dictionary__target_is_dictionary_feature(
    item_type_dimension_lookup_feature, event_table
):
    """
    Test is in dictionary
    """
    # get dictionary feature
    event_view = event_table.get_view()
    feature_group = event_view.groupby("CUST_ID", category="ÜSER ID").aggregate_over(
        value_column="PRODUCT_ACTION",
        method="latest",
        windows=["30d"],
        feature_names=["LATEST_ACTION_DICT_30d"],
    )
    dictionary_feature = feature_group["LATEST_ACTION_DICT_30d"]

    # perform is in
    isin_feature = item_type_dimension_lookup_feature.isin(dictionary_feature)
    isin_feature.name = "lookup_is_in_dictionary"

    # assert
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "cust_id": "1", "item_id": "item_0"}
    isin_feature_preview = isin_feature.preview(pd.DataFrame([preview_params]))
    assert isin_feature_preview.shape[0] == 1
    assert isin_feature_preview.iloc[0].to_dict() == {
        "lookup_is_in_dictionary": False,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_is_in_dictionary__target_is_array(item_type_dimension_lookup_feature):
    """
    Test is in array
    """
    # perform is in check
    isin_feature = item_type_dimension_lookup_feature.isin(["type_0", "type_1"])
    isin_feature.name = "lookup_is_in_dictionary"

    # try to get preview and assert
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "item_id": "item_0"}
    isin_feature_preview = isin_feature.preview(pd.DataFrame([preview_params]))
    assert isin_feature_preview.shape[0] == 1
    assert isin_feature_preview.iloc[0].to_dict() == {
        "lookup_is_in_dictionary": True,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_get_value_from_dictionary__target_is_lookup_feature(
    item_type_dimension_lookup_feature, count_item_type_dictionary_feature
):
    """
    Test get value from dictionary.
    """
    # perform get_value
    get_value_feature = count_item_type_dictionary_feature.cd.get_value(
        item_type_dimension_lookup_feature
    )
    assert isinstance(get_value_feature, Feature)
    feature_name = "get_count_value_from_dictionary"
    get_value_feature.name = feature_name

    # assert
    preview_params = {
        "POINT_IN_TIME": "2001-01-13 12:00:00",
        "item_id": "item_55",
        "order_id": "T2",
    }
    get_value_feature_preview = get_value_feature.preview(pd.DataFrame([preview_params]))

    # Note: Snowflake returns the count value 1 as a string because the value type has to be VARIANT
    # when constructing the dictionary. Spark returns the count value 1 as an int which is more
    # correct. Adding this cast here so that the test works for both backends, but ideally we should
    # fix Snowflake to return the correct type when looking up values from a dictionary.
    get_value_feature_preview[feature_name] = get_value_feature_preview[feature_name].astype(int)

    assert get_value_feature_preview.shape[0] == 1
    assert get_value_feature_preview.iloc[0].to_dict() == {
        feature_name: 1,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_get_value_in_dictionary__target_is_scalar(event_table):
    """
    Test is in dictionary
    """
    # get dictionary feature
    event_view = event_table.get_view()
    feature_name = "SUM_AMOUNT_DICT_30d"
    feature_group = event_view.groupby("CUST_ID", category="PRODUCT_ACTION").aggregate_over(
        value_column="ÀMOUNT",
        method="sum",
        windows=["30d"],
        feature_names=[feature_name],
    )
    dictionary_feature = feature_group[feature_name]

    # perform get_value
    get_value_feature = dictionary_feature.cd.get_value("detail")
    assert isinstance(get_value_feature, Feature)
    feature_name = "get_value_in_dictionary"
    get_value_feature.name = feature_name

    # assert
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "cust_id": "350"}
    get_value_feature_preview = get_value_feature.preview(pd.DataFrame([preview_params]))

    # Note: See notes above in test_get_value_from_dictionary__target_is_lookup_feature for why the
    # casting is needed.
    get_value_feature_preview[feature_name] = get_value_feature_preview[feature_name].astype(float)

    assert get_value_feature_preview.shape[0] == 1
    assert get_value_feature_preview.iloc[0].to_dict() == {
        feature_name: 44.21,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_get_relative_frequency_from_dictionary__target_is_lookup_feature(
    item_type_dimension_lookup_feature, count_item_type_dictionary_feature
):
    """
    Test get relative frequency from dictionary.
    """
    # perform get_value
    get_value_feature = count_item_type_dictionary_feature.cd.get_relative_frequency(
        item_type_dimension_lookup_feature
    )
    assert isinstance(get_value_feature, Feature)
    get_value_feature.name = "get_relative_frequency_from_dictionary"

    # assert
    preview_params = {
        "POINT_IN_TIME": "2001-01-13 12:00:00",
        "item_id": "item_13",
        "order_id": "T2",
    }
    get_value_feature_preview = get_value_feature.preview(pd.DataFrame([preview_params]))
    assert get_value_feature_preview.shape[0] == 1
    assert get_value_feature_preview.iloc[0].to_dict() == {
        get_value_feature.name: 0.11111111111111101,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_get_relative_frequency_in_dictionary__target_is_scalar(count_item_type_dictionary_feature):
    """
    Test get relative frequency
    """
    # perform get_value
    get_value_feature = count_item_type_dictionary_feature.cd.get_relative_frequency("type_13")
    assert isinstance(get_value_feature, Feature)
    get_value_feature.name = "get_relative_frequency_in_dictionary"

    # assert
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "order_id": "T2"}
    get_value_feature_preview = get_value_feature.preview(pd.DataFrame([preview_params]))
    assert get_value_feature_preview.shape[0] == 1
    assert get_value_feature_preview.iloc[0].to_dict() == {
        get_value_feature.name: 0.11111111111111101,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_get_rank_from_dictionary__target_is_lookup_feature(
    item_type_dimension_lookup_feature, count_item_type_dictionary_feature
):
    """
    Test get rank from dictionary.
    """
    # perform get_rank
    get_value_feature = count_item_type_dictionary_feature.cd.get_rank(
        item_type_dimension_lookup_feature
    )
    assert isinstance(get_value_feature, Feature)
    get_value_feature.name = "get_rank_value_from_dictionary"

    # assert
    preview_params = {
        "POINT_IN_TIME": "2001-01-13 12:00:00",
        "item_id": "item_13",
        "order_id": "T2",
    }
    get_value_feature_preview = get_value_feature.preview(pd.DataFrame([preview_params]))
    assert get_value_feature_preview.shape[0] == 1
    assert get_value_feature_preview.iloc[0].to_dict() == {
        get_value_feature.name: 1.0,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_get_rank_in_dictionary__target_is_scalar(count_item_type_dictionary_feature):
    """
    Test get rank in dictionary
    """
    # perform get_rank
    get_value_feature = count_item_type_dictionary_feature.cd.get_rank("type_13", descending=True)
    assert isinstance(get_value_feature, Feature)
    get_value_feature.name = "get_rank_in_dictionary"

    # assert
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "order_id": "T2"}
    get_value_feature_preview = get_value_feature.preview(pd.DataFrame([preview_params]))
    assert get_value_feature_preview.shape[0] == 1
    assert get_value_feature_preview.iloc[0].to_dict() == {
        get_value_feature.name: 1.0,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_get_rank_in_dictionary__target_is_not_found(count_item_type_dictionary_feature):
    """
    Test get rank in dictionary, key is not found
    """
    # perform get_rank
    get_value_feature = count_item_type_dictionary_feature.cd.get_rank(
        "unknown_key", descending=True
    )
    assert isinstance(get_value_feature, Feature)
    get_value_feature.name = "get_rank_in_dictionary"

    # assert
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "order_id": "T2"}
    get_value_feature_preview = get_value_feature.preview(pd.DataFrame([preview_params]))
    assert get_value_feature_preview.shape[0] == 1
    preview_dict = get_value_feature_preview.iloc[0].to_dict()
    rank = preview_dict[get_value_feature.name]
    assert is_scalar_nan(rank)
