"""
Test operations on dict / cross aggregation features
"""

import pandas as pd
import pytest

from featurebyte import FeatureGroup, FeatureList
from tests.util.helper import fb_assert_frame_equal


@pytest.fixture(name="cross_aggregate_feature")
def cross_aggregate_feature_fixture(event_table):
    """
    Fixture for a cross aggregate feature
    """
    view = event_table.get_view()
    feature = view.groupby("ÜSER ID", category="PRODUCT_ACTION").aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["7d"],
        feature_names=["amount_sum_across_action_7d"],
    )["amount_sum_across_action_7d"]
    return feature


def test_key_with_highest_and_lowest_value(cross_aggregate_feature):
    """
    Test key_with_highest_value and key_with_lowest_value
    """
    action_with_lowest_amount = cross_aggregate_feature.cd.key_with_lowest_value()
    action_with_lowest_amount.name = "action_with_lowest_amount"
    action_with_highest_amount = cross_aggregate_feature.cd.key_with_highest_value()
    action_with_highest_amount.name = "action_with_highest_amount"
    feature_group = FeatureGroup([
        cross_aggregate_feature,
        action_with_lowest_amount,
        action_with_highest_amount,
    ])
    preview_param = pd.DataFrame([
        {
            "POINT_IN_TIME": "2001-01-02 10:00:00",
            "üser id": 1,
        }
    ])
    df = feature_group.preview(preview_param)
    expected = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "amount_sum_across_action_7d": '{\n  "__MISSING__": 2.347700000000000e+02,\n  "detail": 1.941600000000000e+02,\n  "purchase": 2.257800000000000e+02,\n  "rëmove": 1.139000000000000e+01,\n  "àdd": 3.385100000000000e+02\n}',
            "action_with_lowest_amount": "rëmove",
            "action_with_highest_amount": "àdd",
        }
    ])
    fb_assert_frame_equal(df, expected, dict_like_columns=["amount_sum_across_action_7d"])


def test_flat_dict_feature(event_table):
    """
    Test feature with a FLAT_DICT dtype
    """
    view = event_table.get_view()
    feature = view.groupby("ÜSER ID").aggregate_over(
        "FLAT_DICT",
        method="latest",
        windows=[None],
        feature_names=["flat_dict_latest_feature"],
    )["flat_dict_latest_feature"]
    feature_list = FeatureList([feature], name="flat_dict_feature")
    df_obs = pd.DataFrame([
        {
            "POINT_IN_TIME": "2001-01-02 10:00:00",
            "üser id": 1,
        }
    ])
    df = feature_list.compute_historical_features(df_obs)
    expected = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "flat_dict_latest_feature": "{'a': 1, 'b': 9}",
        }
    ])
    fb_assert_frame_equal(df, expected, dict_like_columns=["flat_dict_latest_feature"])
