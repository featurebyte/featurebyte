"""
Tests for more features
"""
import pandas as pd
import pytest

from featurebyte import EventView, FeatureList


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_features_without_entity(event_data):
    """
    Test working with purely time based features without any entity
    """
    event_view = EventView.from_event_data(event_data)
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
        feature_list_1.get_historical_features(df)
        .sort_values("POINT_IN_TIME")
        .reset_index(drop=True)
    )
    df_features_2 = (
        feature_list_2.get_historical_features(df)
        .sort_values("POINT_IN_TIME")
        .reset_index(drop=True)
    )

    # Test saving and deploying
    try:
        feature_list_1.save()
        feature_list_1.deploy(enable=True, make_production_ready=True)
        feature_list_2.save()
        feature_list_2.deploy(enable=True, make_production_ready=True)

        # Test getting historical requests
        df_features_deployed_1 = (
            feature_list_1.get_historical_features(df)
            .sort_values("POINT_IN_TIME")
            .reset_index(drop=True)
        )
        df_features_deployed_2 = (
            feature_list_2.get_historical_features(df)
            .sort_values("POINT_IN_TIME")
            .reset_index(drop=True)
        )
    finally:
        feature_list_1.deploy(enable=False, make_production_ready=True)
        feature_list_2.deploy(enable=False, make_production_ready=True)

    pd.testing.assert_frame_equal(df_features_1, df_features_deployed_1)
    pd.testing.assert_frame_equal(df_features_2, df_features_deployed_2)
