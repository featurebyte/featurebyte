import pandas as pd
import pytest

from featurebyte import (
    AggFunc,
    DisguisedValueImputation,
    EventView,
    FeatureList,
    ItemView,
    MissingValueImputation,
    StringValueImputation,
    UnexpectedValueImputation,
    ValueBeyondEndpointImputation,
)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_event_data_update_critical_data_info(event_data):
    """Test EventData with critical data info preview & feature preview"""
    # add critical data info to amount column & check data preview
    original_df = event_data.preview()

    # check data column preview
    amount = event_data["ÀMOUNT"].preview()
    pd.testing.assert_frame_equal(original_df[["ÀMOUNT"]], amount)

    assert original_df["ÀMOUNT"].isnull().sum() == 2
    assert original_df["ÀMOUNT"].isnull().sum() == 2
    assert original_df["SESSION_ID"].isnull().sum() == 0
    assert set(original_df["PRODUCT_ACTION"].astype(str).unique()) == {
        "detail",
        "purchase",
        "rëmove",
        "àdd",
        "nan",
    }
    assert event_data.frame.node.type == "input"
    event_data["ÀMOUNT"].update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=0.0),
            ValueBeyondEndpointImputation(type="less_than", end_point=0.0, imputed_value=0.0),
        ]
    )
    event_data.SESSION_ID.update_critical_data_info(
        cleaning_operations=[DisguisedValueImputation(disguised_values=[979], imputed_value=None)]
    )
    event_data.PRODUCT_ACTION.update_critical_data_info(
        cleaning_operations=[
            UnexpectedValueImputation(
                expected_values=["detail", "purchase", "rëmove"], imputed_value="missing"
            ),
        ]
    )
    event_data.TRANSACTION_ID.update_critical_data_info(
        cleaning_operations=[StringValueImputation(imputed_value=0)]
    )
    event_data.CUST_ID.update_critical_data_info(
        cleaning_operations=[StringValueImputation(imputed_value=0)]
    )
    assert event_data.frame.node.type == "input"

    # create feature group & preview
    event_view = EventView.from_event_data(event_data)

    # check equality between cleaning data's vs view's preview, sample & describe operations
    view_df = event_view.preview()
    clean_df = event_data.preview(after_cleaning=True)
    pd.testing.assert_frame_equal(view_df, clean_df)

    view_sample_df = event_view.sample()
    clean_sample_df = event_data.sample(after_cleaning=True)
    pd.testing.assert_frame_equal(view_sample_df, clean_sample_df)

    # check describe operation between post-clean event data & event view
    view_describe_df = event_view.describe()
    clean_describe_df = event_data.describe(after_cleaning=True)
    pd.testing.assert_frame_equal(view_describe_df, clean_describe_df)

    assert view_df["ÀMOUNT"].isnull().sum() == 0
    assert view_df["SESSION_ID"].isnull().sum() == 1
    assert set(view_df["PRODUCT_ACTION"].astype(str).unique()) == {
        "detail",
        "purchase",
        "rëmove",
        "missing",
    }
    # check that values in string type column (TRANSACTION_ID) are imputed to 0 and
    # values in integer type column (CUST_ID) are not imputed.
    assert (view_df["TRANSACTION_ID"] == "0.00000").all()
    assert (view_df["CUST_ID"] == original_df["CUST_ID"]).all()

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
    hist_feat = FeatureList([feature_group], name="feature_list").get_historical_features(
        df_training_events
    )
    pd.testing.assert_frame_equal(feat_preview_df, hist_feat, check_dtype=False)

    # remove critical data info
    event_data["ÀMOUNT"].update_critical_data_info(cleaning_operations=[])
    event_data.SESSION_ID.update_critical_data_info(cleaning_operations=[])
    event_data.PRODUCT_ACTION.update_critical_data_info(cleaning_operations=[])
    event_data.TRANSACTION_ID.update_critical_data_info(cleaning_operations=[])
    event_data.CUST_ID.update_critical_data_info(cleaning_operations=[])


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_item_data_update_critical_data_info(item_data):
    """Test ItemData with critical data info preview & feature preview"""
    # add critical data info to item_type column & check data preview
    assert item_data.frame.node.type == "input"
    item_data["item_type"].update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value="missing_item")]
    )
    assert item_data.frame.node.type == "input"
    _ = item_data.preview()

    # check feature & preview
    item_view = ItemView.from_item_data(item_data)
    window_feature = item_view.groupby("ÜSER ID", category="item_type").aggregate_over(
        method="count",
        windows=["12h"],
        feature_names=["count_12h"],
    )["count_12h"]
    window_preview_df = window_feature.preview(
        {"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 1}
    )
    assert window_preview_df.count_12h.iloc[0] == '{\n  "type_84": 1\n}'

    feature = item_view.groupby("order_id").aggregate(
        method=AggFunc.COUNT,
        feature_name="order_size",
    )
    preview_df = feature.preview({"POINT_IN_TIME": "2001-11-15 10:00:00", "order_id": "T236"})
    assert preview_df["order_size"].iloc[0] == 6

    # check historical request
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"]),
            "üser id": [1, 1],
            "order_id": ["T236", "T236"],
        }
    )
    hist_feat = FeatureList([feature, window_feature], name="feature_list").get_historical_features(
        df_training_events
    )
    assert list(hist_feat.columns) == [
        "POINT_IN_TIME",
        "üser id",
        "order_id",
        "order_size",
        "count_12h",
    ]

    # remove critical data info
    item_data["item_type"].update_critical_data_info(cleaning_operations=[])
