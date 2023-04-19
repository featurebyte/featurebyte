"""
Test change view operations
"""
import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from featurebyte import AggFunc, FeatureList


@pytest.fixture
def freeze_time_for_change_view():
    """
    Fix ChangeView creation time since that affects its default feature job setting
    """
    fixed_change_view_creation_time = datetime.datetime(2022, 5, 1)
    with patch("featurebyte.api.change_view.datetime") as mocked_datetime:
        mocked_datetime.now.return_value = fixed_change_view_creation_time
        yield


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
@pytest.mark.usefixtures("freeze_time_for_change_view")
def test_change_view(scd_table):
    """
    Test change view operations
    """
    change_view = scd_table.get_change_view("User Status")

    # assert initialization
    assert len(change_view.columns_info) == 5
    assert change_view.timestamp_column == "new_Effective Timestamp"
    assert change_view.natural_key_column == scd_table.natural_key_column
    assert change_view.columns == [
        "User ID",
        "new_Effective Timestamp",
        "past_Effective Timestamp",
        "new_User Status",
        "past_User Status",
    ]

    # check creating additional lag works
    column_name = "lagged_status_offset2"
    change_view[column_name] = change_view["new_User Status"].lag("User ID", 2)
    df = change_view.preview(10)
    assert df[column_name].notnull().sum() > 0

    # assert that we can get features
    count_1w_feature = change_view.groupby("User ID").aggregate_over(
        method=AggFunc.COUNT,
        windows=["1w"],
        feature_names=["count_1w"],
    )["count_1w"]
    df = count_1w_feature.preview(
        pd.DataFrame([{"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 1}])
    )
    assert df.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
        "üser id": 1,
        "count_1w": 1,
    }


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
@pytest.mark.usefixtures("freeze_time_for_change_view")
def test_change_view__feature_no_entity(scd_table):
    """
    Test change view operations
    """
    change_view = scd_table.get_change_view("User Status")

    # assert that we can get features
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
        "count_1w": 16,
    }
    count_1w_feature = change_view.groupby([]).aggregate_over(
        method=AggFunc.COUNT,
        windows=["1w"],
        feature_names=["count_1w"],
    )["count_1w"]
    df = count_1w_feature.preview(pd.DataFrame([{"POINT_IN_TIME": "2001-11-15 10:00:00"}]))
    assert df.iloc[0].to_dict() == expected

    # check historical features
    observations_set = pd.DataFrame([{"POINT_IN_TIME": "2001-11-15 10:00:00"}])
    df = FeatureList([count_1w_feature], name="mylist").get_historical_features(observations_set)
    assert df.iloc[0].to_dict() == expected

    # run again with different time to trigger entity tracker update, and it should work
    expected = {"POINT_IN_TIME": pd.Timestamp("2001-12-15 10:00:00"), "count_1w": 24}
    observations_set = pd.DataFrame([{"POINT_IN_TIME": "2001-12-15 10:00:00"}])
    df = FeatureList([count_1w_feature], name="mylist").get_historical_features(observations_set)
    assert df.iloc[0].to_dict() == expected
