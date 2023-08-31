"""
Tests for more features
"""
import pandas as pd
import pytest

from featurebyte import FeatureList


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_features_without_entity(event_table):
    """
    Test working with purely time based features without any entity
    """
    event_view = event_table.get_view()
    feature_group = event_view.groupby([]).aggregate_over(
        method="count",
        windows=["2h", "24h"],
        feature_names=["ALL_COUNT_2h", "ALL_COUNT_24h"],
    )
    df = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.date_range("2001-01-01", periods=5, freq="d"),
        }
    )

    feature_list_1 = FeatureList([feature_group["ALL_COUNT_2h"]], name="all_count_2h_list")
    feature_list_2 = FeatureList([feature_group["ALL_COUNT_24h"]], name="all_count_24h_list")

    # Test historical features with on demand tile generation
    df_features_1 = (
        feature_list_1.compute_historical_features(df)
        .sort_values("POINT_IN_TIME")
        .reset_index(drop=True)
    )
    df_features_2 = (
        feature_list_2.compute_historical_features(df)
        .sort_values("POINT_IN_TIME")
        .reset_index(drop=True)
    )

    # Test saving and deploying
    deployment_1, deployment_2 = None, None
    try:
        feature_list_1.save()
        deployment_1 = feature_list_1.deploy(make_production_ready=True)
        deployment_1.enable()

        feature_list_2.save()
        deployment_2 = feature_list_2.deploy(make_production_ready=True)
        deployment_2.enable()

        # Test getting historical requests
        df_features_deployed_1 = (
            feature_list_1.compute_historical_features(df)
            .sort_values("POINT_IN_TIME")
            .reset_index(drop=True)
        )
        df_features_deployed_2 = (
            feature_list_2.compute_historical_features(df)
            .sort_values("POINT_IN_TIME")
            .reset_index(drop=True)
        )
    finally:
        for deployment in [deployment_1, deployment_2]:
            if deployment:
                deployment.disable()

    pd.testing.assert_frame_equal(df_features_1, df_features_deployed_1)
    pd.testing.assert_frame_equal(df_features_2, df_features_deployed_2)


def test_combined_simple_aggregate_and_window_aggregate(event_table, item_table):
    """
    Test combining simple aggregate and window aggregate
    """
    event_view = event_table.get_view()
    item_view = item_table.get_view()
    item_feature = item_view.groupby("order_id").aggregate(
        method="count", feature_name="my_item_feature"
    )
    event_view = event_view.add_feature("added_feature", item_feature, "TRANSACTION_ID")

    window_feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="added_feature",
        method="sum",
        windows=["7d"],
        feature_names=["added_feature_sum_7d"],
    )["added_feature_sum_7d"]

    feature = window_feature + item_feature
    feature.name = "combined_feature"
    df_observation = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-11-15 10:00:00"]),
            "order_id": ["T1"],
        }
    )
    df_preview = feature.preview(df_observation)
    expected = [
        {
            "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
            "order_id": "T1",
            "combined_feature": 69,
        }
    ]
    assert df_preview.to_dict("records") == expected


def test_relative_frequency_with_filter(event_table, scd_table):
    """
    Test complex feature involving an aggregation feature and a lookup feature, one with filter and
    another without
    """
    event_view = event_table.get_view()
    scd_view = scd_table.get_view()

    event_view = event_view.join(scd_view, on="ÜSER ID")
    lookup_feature = event_view["User Status"].as_feature("User Status Feature")

    filtered_view = event_view[event_view["ÀMOUNT"] > 2]
    dict_feature = filtered_view.groupby("ÜSER ID", category="User Status").aggregate_over(
        method="count",
        windows=["7d"],
        feature_names=["my_feature"],
    )["my_feature"]

    feature = dict_feature.cd.get_relative_frequency(lookup_feature)
    feature.name = "complex_feature"
    feature_list = FeatureList([feature], name="my_feature_list")
    preview_param = {
        "POINT_IN_TIME": "2002-01-02 10:00:00",
        "order_id": "T4390",
        "üser id": 1,
    }
    observations_set = pd.DataFrame([preview_param])
    df = feature_list.compute_historical_features(observations_set)
    expected = [
        {
            "POINT_IN_TIME": pd.Timestamp("2002-01-02 10:00:00"),
            "order_id": "T4390",
            "üser id": 1,
            "complex_feature": 0.125,
        }
    ]
    assert df.to_dict("records") == expected
