import pandas as pd

from featurebyte import (
    DimensionView,
    EventView,
    FeatureList,
    ItemView,
    MissingValueImputation,
    SlowlyChangingView,
    ValueBeyondEndpointImputation,
)


def test_event_data_update_critical_data_info(event_data):
    """Test EventData with critical data info preview & feature preview"""
    # add critical data info to amount column & check data preview
    assert event_data.preview()["AMOUNT"].isnull().sum() == 2
    assert event_data.node.type == "input"
    event_data.AMOUNT.update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=0.0),
            ValueBeyondEndpointImputation(type="less_than", end_point=0.0, imputed_value=0.0),
        ]
    )
    assert event_data.node.type == "graph"
    assert event_data.preview()["AMOUNT"].isnull().sum() == 0

    # create feature group & preview
    event_view = EventView.from_event_data(event_data)
    assert event_view.node.type == "graph"
    feature_group = event_view.groupby("CUST_ID").aggregate_over(
        method="count",
        windows=["2h", "24h"],
        feature_names=["COUNT_2h", "COUNT_24h"],
        feature_job_setting={
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
    )
    feat_preview_df = feature_group.preview(
        point_in_time_and_serving_name={"POINT_IN_TIME": "2001-01-14", "cust_id": 938}
    )
    assert list(feat_preview_df.columns) == ["POINT_IN_TIME", "cust_id", "COUNT_2h", "COUNT_24h"]
    assert feat_preview_df.COUNT_2h.iloc[0] == 0
    assert feat_preview_df.COUNT_24h.iloc[0] == 1

    # check historical request
    df_training_events = pd.DataFrame(
        {"POINT_IN_TIME": pd.to_datetime(["2001-01-14 00:00:00"]), "cust_id": [938]}
    )
    hist_feat = FeatureList([feature_group]).get_historical_features(df_training_events)
    pd.testing.assert_frame_equal(feat_preview_df, hist_feat, check_dtype=False)

    # remove critical data info
    event_data.AMOUNT.update_critical_data_info(cleaning_operations=[])
    assert event_data.node.type == "input"


def test_item_data_update_critical_data_info(item_data):
    """Test ItemData with critical data info preview & feature preview"""
    # add critical data info to item_type column & check data preview
    assert item_data.node.type == "input"
    item_data["item_type"].update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value="missing_item")]
    )
    assert item_data.node.type == "graph"
    _ = item_data.preview()

    # check feature & preview
    item_view = ItemView.from_item_data(item_data)
    window_feature = item_view.groupby("USER ID", category="item_type").aggregate_over(
        method="count",
        windows=["12h"],
        feature_names=["count_12h"],
    )["count_12h"]
    window_preview_df = window_feature.preview(
        {"POINT_IN_TIME": "2001-11-15 10:00:00", "user id": 1}
    )
    assert window_preview_df.count_12h.iloc[0] == (
        '{\n  "type_19": 1,\n  "type_38": 1,\n  "type_39": 1,\n  "type_44": 1,\n  "type_69": 1,\n  "type_85": 1,'
        '\n  "type_87": 1,\n  "type_90": 1\n}'
    )

    feature = item_view.groupby("order_id").aggregate(
        method="count",
        feature_name="order_size",
    )
    preview_df = feature.preview({"POINT_IN_TIME": "2001-11-15 10:00:00", "order_id": "T236"})
    assert preview_df["order_size"].iloc[0] == 6

    # check historical request
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"]),
            "user id": [1, 1],
            "order_id": ["T236", "T236"],
        }
    )
    hist_feat = FeatureList([feature, window_feature]).get_historical_features(df_training_events)
    assert list(hist_feat.columns) == [
        "POINT_IN_TIME",
        "user id",
        "order_id",
        "order_size",
        "count_12h",
    ]

    # remove critical data info
    item_data["item_type"].update_critical_data_info(cleaning_operations=[])
    assert item_data.node.type == "input"
