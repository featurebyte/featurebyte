"""
Integration tests related to DimensionView
"""

import pandas as pd
import pytest
import pytest_asyncio

from featurebyte import Feature, RequestColumn
from featurebyte.typing import is_scalar_nan
from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
)
from tests.util.helper import (
    assert_preview_result_equal,
    fb_assert_frame_equal,
    tz_localize_if_needed,
)


@pytest_asyncio.fixture(name="bad_dimension_table")
async def bad_dimension_table_fixture(session, data_source, transaction_data_upper_case):
    """
    Fixture for a bad dimension table (primary key is not unique)
    """
    unique_product_action = list(transaction_data_upper_case["PRODUCT_ACTION"].unique())
    df_bad_dimension = pd.DataFrame({
        "PRODUCT_ACTION": unique_product_action + unique_product_action
    })
    df_bad_dimension["DIMENSION_VALUE"] = range(len(df_bad_dimension))
    await session.register_table("BAD_DIMENSION_TABLE", df_bad_dimension)
    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="BAD_DIMENSION_TABLE",
    )
    dimension_table = database_table.create_dimension_table(
        name="BAD_DIMENSION_TABLE",
        dimension_id_column="PRODUCT_ACTION",
    )
    return dimension_table


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
    item_view = item_table.get_view()
    return item_view.groupby("order_id", category="item_type").aggregate(
        value_column=None,
        method="count",
        feature_name="COUNT_ITEM_TYPE",
    )


def test_dimension_lookup_features(dimension_view):
    """
    Test lookup features from DimensionView
    """
    feature = dimension_view["item_type"].as_feature("ItemTypeFeature")

    # Test single lookup feature
    preview_params = {"item_id": "item_42"}
    df = feature.preview(pd.DataFrame([preview_params]))
    assert df.shape[0] == 1
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
    assert df.shape[0] == 1
    assert df.iloc[0].to_dict() == {
        "ItemNameFeature": "name_42",
        "ItemTypeFeature": "type_42",
        **preview_params,
    }


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


def test_is_in_dictionary__target_is_dictionary_feature(
    item_type_dimension_lookup_feature, event_table, source_type
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
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "cust_id": 1, "item_id": "item_0"}
    isin_feature_preview = isin_feature.preview(pd.DataFrame([preview_params]))
    tz_localize_if_needed(isin_feature_preview, source_type)
    assert isin_feature_preview.shape[0] == 1
    assert isin_feature_preview.iloc[0].to_dict() == {
        "lookup_is_in_dictionary": False,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


def test_is_in_dictionary__target_is_array(item_type_dimension_lookup_feature, source_type):
    """
    Test is in array
    """
    # perform is in check
    isin_feature = item_type_dimension_lookup_feature.isin(["type_0", "type_1"])
    isin_feature.name = "lookup_is_in_dictionary"

    # try to get preview and assert
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "item_id": "item_0"}
    isin_feature_preview = isin_feature.preview(pd.DataFrame([preview_params]))
    tz_localize_if_needed(isin_feature_preview, source_type)
    assert isin_feature_preview.shape[0] == 1
    assert isin_feature_preview.iloc[0].to_dict() == {
        "lookup_is_in_dictionary": True,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


def test_get_value_from_dictionary__target_is_lookup_feature(
    item_type_dimension_lookup_feature, count_item_type_dictionary_feature, source_type
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

    tz_localize_if_needed(get_value_feature_preview, source_type)
    assert get_value_feature_preview.shape[0] == 1
    assert get_value_feature_preview.iloc[0].to_dict() == {
        feature_name: 1,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


def test_get_value_in_dictionary__target_is_scalar(event_table, source_type):
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
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "cust_id": 350}
    get_value_feature_preview = get_value_feature.preview(pd.DataFrame([preview_params]))

    # Note: See notes above in test_get_value_from_dictionary__target_is_lookup_feature for why the
    # casting is needed.
    get_value_feature_preview[feature_name] = get_value_feature_preview[feature_name].astype(float)

    tz_localize_if_needed(get_value_feature_preview, source_type)
    assert get_value_feature_preview.shape[0] == 1
    assert get_value_feature_preview.iloc[0].to_dict() == {
        feature_name: 44.21,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


def test_get_value_in_dictionary__target_is_non_lookup(event_table, source_type):
    """
    Test is in dictionary when the key is a feature but not a lookup feature
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

    # get another feature to be used as key (not a lookup feature)
    key_feature = event_view.groupby("CUST_ID").aggregate_over(
        value_column="PRODUCT_ACTION",
        method="latest",
        windows=["30d"],
        feature_names=["latest_action"],
    )["latest_action"]

    # perform get_value
    get_value_feature = dictionary_feature.cd.get_value(key_feature)
    assert isinstance(get_value_feature, Feature)
    feature_name = "get_value_in_dictionary"
    get_value_feature.name = feature_name

    # assert
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "cust_id": 350}
    get_value_feature_preview = get_value_feature.preview(pd.DataFrame([preview_params]))

    # Note: See notes above in test_get_value_from_dictionary__target_is_lookup_feature for why the
    # casting is needed.
    get_value_feature_preview[feature_name] = get_value_feature_preview[feature_name].astype(float)

    tz_localize_if_needed(get_value_feature_preview, source_type)
    assert get_value_feature_preview.shape[0] == 1
    assert get_value_feature_preview.iloc[0].to_dict() == {
        feature_name: 22.05,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


def test_get_value_in_dictionary__target_derived_from_request_column(event_table, source_type):
    """
    Test is in dictionary when the key is derived from request column
    """
    # Dictionary feature with day of week as category
    event_view = event_table.get_view()
    feature_name = "SUM_AMOUNT_DICT_30d"
    event_view["day_of_week"] = event_view["ËVENT_TIMESTAMP"].dt.day_of_week
    feature_group = event_view.groupby("CUST_ID", category="day_of_week").aggregate_over(
        value_column="ÀMOUNT",
        method="sum",
        windows=["30d"],
        feature_names=[feature_name],
    )
    dictionary_feature = feature_group[feature_name]

    # Use a key derived from request point in time to access the dictionary
    key_1 = RequestColumn.point_in_time().dt.day_of_week.astype(str)
    key_2 = (RequestColumn.point_in_time().dt.day_of_week + 1).astype(str)
    get_value_feature_1 = dictionary_feature.cd.get_value(key_1)
    get_value_feature_2 = dictionary_feature.cd.get_value(key_2)
    final_feature = get_value_feature_2 / get_value_feature_1
    feature_name = "get_value_in_dictionary"
    final_feature.name = feature_name

    # Check feature can be saved
    final_feature.save()

    # Check output
    preview_params = [{"POINT_IN_TIME": "2001-01-09 12:00:00", "cust_id": 350}]
    get_value_feature_preview = final_feature.preview(pd.DataFrame(preview_params))
    tz_localize_if_needed(get_value_feature_preview, source_type)
    assert_preview_result_equal(
        get_value_feature_preview,
        {
            feature_name: 0.423660,
            **convert_preview_param_dict_to_feature_preview_resp(preview_params[0]),
        },
    )


def test_get_relative_frequency_from_dictionary__target_is_lookup_feature(
    item_type_dimension_lookup_feature, count_item_type_dictionary_feature, source_type
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
    tz_localize_if_needed(get_value_feature_preview, source_type)
    expected = pd.DataFrame([
        {
            get_value_feature.name: 0.11111111111111101,
            **convert_preview_param_dict_to_feature_preview_resp(preview_params),
        }
    ])[get_value_feature_preview.columns]
    fb_assert_frame_equal(get_value_feature_preview, expected)


def test_get_relative_frequency_in_dictionary__target_is_scalar(
    count_item_type_dictionary_feature, source_type
):
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
    tz_localize_if_needed(get_value_feature_preview, source_type)
    expected = pd.DataFrame([
        {
            get_value_feature.name: 0.11111111111111101,
            **convert_preview_param_dict_to_feature_preview_resp(preview_params),
        }
    ])[get_value_feature_preview.columns]
    fb_assert_frame_equal(get_value_feature_preview, expected)


def test_get_rank_from_dictionary__target_is_lookup_feature(
    item_type_dimension_lookup_feature, count_item_type_dictionary_feature, source_type
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
    tz_localize_if_needed(get_value_feature_preview, source_type)
    assert get_value_feature_preview.shape[0] == 1
    assert get_value_feature_preview.iloc[0].to_dict() == {
        get_value_feature.name: 1.0,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


def test_get_rank_in_dictionary__target_is_scalar(count_item_type_dictionary_feature, source_type):
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
    tz_localize_if_needed(get_value_feature_preview, source_type)
    assert get_value_feature_preview.shape[0] == 1
    assert get_value_feature_preview.iloc[0].to_dict() == {
        get_value_feature.name: 1.0,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


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


def test_bad_dimension_view_join(event_table, bad_dimension_table):
    """
    Test joining event view with a bad dimension view
    """
    event_view = event_table.get_view()
    dimension_view = bad_dimension_table.get_view()
    event_view = event_view.join(dimension_view, on="PRODUCT_ACTION")

    # Check that the join does not multiply rows
    df = event_view[event_view["TRANSACTION_ID"] == "T0"].preview()
    assert df.shape[0] == 1
