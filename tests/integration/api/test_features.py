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
    event_view = event_view.add_feature("added_feature", item_feature)

    window_feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="added_feature",
        method="sum",
        windows=["24h"],
        feature_names=["added_feature_sum_24h"],
    )["added_feature_sum_24h"]

    feature = window_feature + item_feature
    feature.name = "combined_feature"
    df_observation = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-11-15 10:00:00"]),
            "order_id": ["T1"],
        }
    )
    feature.preview(df_observation)
    raise
