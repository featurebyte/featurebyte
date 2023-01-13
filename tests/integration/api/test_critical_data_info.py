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
    assert feat_preview_df.COUNT_2h.iloc[0] == 0
    assert feat_preview_df.COUNT_24h.iloc[0] == 1

    # check historical request
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"]),
            "cust_id": [938, 938],
        }
    )
    hist_feat = FeatureList([feature_group]).get_historical_features(df_training_events)
    assert (hist_feat["COUNT_2h"] == 0).all()
    assert (hist_feat["COUNT_24h"] == 0).all()

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


def test_scd_data_update_critical_data_info(event_data, scd_data):
    """Test SlowlyChangingData with critical data info preview & feature preview"""
    assert scd_data.node.type == "input"
    scd_data["User Status"].update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value="missing_status")]
    )
    assert scd_data.node.type == "graph"

    # check feature & preview
    event_view = EventView.from_event_data(event_data)
    scd_view = SlowlyChangingView.from_slowly_changing_data(scd_data)
    event_view.join(scd_view)
    feature = event_view.groupby("USER ID", category="User Status").aggregate_over(
        method="count",
        windows=["7d"],
        feature_names=["count_7d"],
    )["count_7d"]
    preview_df = feature.preview({"POINT_IN_TIME": "2001-11-15 10:00:00", "user id": 1})
    assert list(preview_df.columns) == ["POINT_IN_TIME", "user id", "count_7d"]

    # check historical request
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"]),
            "user id": [1, 1],
        }
    )
    hist_feat = FeatureList([feature]).get_historical_features(df_training_events)
    assert list(hist_feat.columns) == ["POINT_IN_TIME", "user id", "count_7d"]

    # remove critical data info
    scd_data["User Status"].update_critical_data_info(cleaning_operations=[])
    assert scd_data.node.type == "input"


def test_dimension_data_update_critical_data_info(dimension_data):
    """Test DimensionData with critical data info preview & feature preview"""
    assert dimension_data.node.type == "input"
    dimension_data["item_name"].update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value="missing_item")]
    )
    assert dimension_data.node.type == "graph"

    # check feature & preview
    dimension_view = DimensionView.from_dimension_data(dimension_data)
    feature = dimension_view["item_name"].as_feature("ItemTypeFeature")
    preview_df = feature.preview({"POINT_IN_TIME": "2001-11-15 10:00:00", "item_id": "item_42"})
    assert preview_df.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
        "item_id": "item_42",
        "ItemTypeFeature": "name_42",
    }

    # remove critical data info
    dimension_data["item_name"].update_critical_data_info(cleaning_operations=[])
    assert dimension_data.node.type == "input"
