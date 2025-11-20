import pandas as pd
import pytest

from featurebyte import (
    AggFunc,
    CastToNumeric,
    DisguisedValueImputation,
    FeatureJobSetting,
    FeatureList,
    MissingValueImputation,
    StringValueImputation,
    UnexpectedValueImputation,
    ValueBeyondEndpointImputation,
)
from featurebyte.enum import DBVarType


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_event_table_timestamp_imputation(event_table):
    """Test EventTable with timestamp imputation"""
    assert event_table.dtypes.loc["ËVENT_TIMESTAMP"] == "TIMESTAMP_TZ"
    event_table["ËVENT_TIMESTAMP"].update_critical_data_info(
        cleaning_operations=[
            ValueBeyondEndpointImputation(
                type="less_than", end_point="2020-01-01", imputed_value="2020-01-01"
            ),
        ]
    )
    event_view = event_table.get_view()
    preview_df = event_view.preview()
    assert preview_df["ËVENT_TIMESTAMP"].min() == pd.to_datetime("2020-01-01 00:00:00+00:00")

    # clean up event table cleaning operations
    event_table["ËVENT_TIMESTAMP"].update_critical_data_info(cleaning_operations=[])


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_event_table_update_critical_data_info(event_table):
    """Test EventTable with critical data info preview & feature preview"""
    # add critical data info to amount column & check table preview
    original_df = event_table.preview()

    # check table column preview
    amount = event_table["ÀMOUNT"].preview()
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
    assert event_table.frame.node.type == "input"
    event_table["ÀMOUNT"].update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=0.0),
            ValueBeyondEndpointImputation(type="less_than", end_point=0.0, imputed_value=0.0),
        ]
    )
    event_table.SESSION_ID.update_critical_data_info(
        cleaning_operations=[DisguisedValueImputation(disguised_values=[979], imputed_value=None)]
    )
    event_table.PRODUCT_ACTION.update_critical_data_info(
        cleaning_operations=[
            UnexpectedValueImputation(
                expected_values=["detail", "purchase", "rëmove"], imputed_value="missing"
            ),
        ]
    )
    event_table.TRANSACTION_ID.update_critical_data_info(
        cleaning_operations=[StringValueImputation(imputed_value=0)]
    )
    event_table.CUST_ID.update_critical_data_info(
        cleaning_operations=[StringValueImputation(imputed_value=0)]
    )
    assert event_table.frame.node.type == "input"

    # create feature group & preview
    event_view = event_table.get_view()

    # check equality between cleaning table's vs view's preview, sample & describe operations
    view_df = event_view.preview()
    clean_df = event_table.preview(after_cleaning=True)
    pd.testing.assert_frame_equal(view_df, clean_df)

    view_sample_df = event_view.sample()
    clean_sample_df = event_table.sample(after_cleaning=True)
    pd.testing.assert_frame_equal(view_sample_df, clean_sample_df)

    # check describe operation between post-clean event table & event view
    view_describe_df = event_view.describe()
    clean_describe_df = event_table.describe(after_cleaning=True)
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
    assert (view_df["TRANSACTION_ID"] == "0").all()
    assert (view_df["CUST_ID"] == original_df["CUST_ID"]).all()

    assert event_view.node.type == "graph"
    feature_group = event_view.groupby("CUST_ID").aggregate_over(
        value_column=None,
        method="count",
        windows=["2h", "24h"],
        feature_names=["COUNT_2h", "COUNT_24h"],
        feature_job_setting=FeatureJobSetting(blind_spot="10m", period="30m", offset="5m"),
    )
    feat_preview_df = feature_group.preview(
        observation_set=pd.DataFrame([{"POINT_IN_TIME": "2001-01-14", "cust_id": 938}])
    )
    assert list(feat_preview_df.columns) == ["POINT_IN_TIME", "cust_id", "COUNT_2h", "COUNT_24h"]
    assert feat_preview_df.COUNT_2h.isnull().sum() == 1
    assert feat_preview_df.COUNT_24h.iloc[0] == 1

    # check historical request
    df_training_events = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2001-01-14 00:00:00"]),
        "cust_id": [938],
    })
    hist_feat = FeatureList([feature_group], name="feature_list").compute_historical_features(
        df_training_events
    )
    pd.testing.assert_frame_equal(feat_preview_df, hist_feat, check_dtype=False)

    # remove critical data info
    event_table["ÀMOUNT"].update_critical_data_info(cleaning_operations=[])
    event_table.SESSION_ID.update_critical_data_info(cleaning_operations=[])
    event_table.PRODUCT_ACTION.update_critical_data_info(cleaning_operations=[])
    event_table.TRANSACTION_ID.update_critical_data_info(cleaning_operations=[])
    event_table.CUST_ID.update_critical_data_info(cleaning_operations=[])


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_item_table_update_critical_data_info(item_table):
    """Test ItemTable with critical data info preview & feature preview"""
    # add critical data info to item_type column & check table preview
    assert item_table.frame.node.type == "input"
    item_table["item_type"].update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value="missing_item")]
    )
    assert item_table.frame.node.type == "input"
    _ = item_table.preview()

    # check feature & preview
    item_view = item_table.get_view()
    window_feature = item_view.groupby("ÜSER ID", category="item_type").aggregate_over(
        value_column=None,
        method="count",
        windows=["12h"],
        feature_names=["count_12h"],
    )["count_12h"]
    window_preview_df = window_feature.preview(
        pd.DataFrame([{"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 1}])
    )
    assert window_preview_df.count_12h.iloc[0] == {"type_84": 1}

    feature = item_view.groupby("order_id").aggregate(
        value_column=None,
        method=AggFunc.COUNT,
        feature_name="order_size",
    )
    preview_df = feature.preview(
        pd.DataFrame([{"POINT_IN_TIME": "2001-11-15 10:00:00", "order_id": "T236"}])
    )
    assert preview_df["order_size"].iloc[0] == 6

    # check historical request
    df_training_events = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"]),
        "üser id": [1, 1],
        "order_id": ["T236", "T236"],
    })
    hist_feat = FeatureList(
        [feature, window_feature], name="feature_list"
    ).compute_historical_features(df_training_events)
    assert list(hist_feat.columns) == [
        "POINT_IN_TIME",
        "üser id",
        "order_id",
        "order_size",
        "count_12h",
    ]

    # remove critical data info
    item_table["item_type"].update_critical_data_info(cleaning_operations=[])


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_cast_to_numeric_varchar_to_float(event_table):
    """Test CastToNumeric cleaning operation - VARCHAR to FLOAT"""
    assert event_table.dtypes.loc["TRANSACTION_ID"] == "VARCHAR"
    event_table["TRANSACTION_ID"].update_critical_data_info(
        cleaning_operations=[CastToNumeric(target_dtype=DBVarType.FLOAT)]
    )

    # Get view and preview
    event_view = event_table.get_view()
    preview_df = event_view.preview()

    # Check that the column now contains numeric values (valid numeric strings are converted, invalid become null)
    assert preview_df["TRANSACTION_ID"].dtype == "float"
    assert preview_df["TRANSACTION_ID"].isnull().all()

    # Test with aggregation
    feature = event_view.groupby("CUST_ID").aggregate_over(
        value_column="TRANSACTION_ID",
        method="sum",
        windows=["24h"],
        feature_names=["sum_transaction_id_24h"],
        feature_job_setting=FeatureJobSetting(blind_spot="10m", period="30m", offset="5m"),
    )["sum_transaction_id_24h"]

    feat_preview_df = feature.preview(
        observation_set=pd.DataFrame([{"POINT_IN_TIME": "2001-01-14", "cust_id": 938}])
    )
    # Since all values are NULL after cast, sum should be NULL
    assert feat_preview_df["sum_transaction_id_24h"].isnull().all()

    # Clean up
    event_table["TRANSACTION_ID"].update_critical_data_info(cleaning_operations=[])


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_cast_to_numeric_varchar_to_int(source_table_with_numeric_strings, customer_entity):
    """Test CastToNumeric cleaning operation - VARCHAR to INT with valid numeric strings"""
    test_event_table = source_table_with_numeric_strings.create_event_table(
        name="test_cast_numeric_table",
        event_id_column="event_id",
        event_timestamp_column="event_timestamp",
    )

    # Check original dtype
    assert test_event_table.dtypes.loc["numeric_string"] == "VARCHAR"

    # Apply CastToNumeric to cast VARCHAR to INT
    test_event_table["numeric_string"].update_critical_data_info(
        cleaning_operations=[CastToNumeric(target_dtype=DBVarType.INT)]
    )

    # Mark cust_id as entity before creating view (needed for aggregation)
    test_event_table.cust_id.as_entity(customer_entity.name)

    # Get view and preview
    event_view = test_event_table.get_view()
    preview_df = event_view.preview()

    # Check that valid numeric strings are converted to INT, invalid ones become NULL
    assert preview_df["numeric_string"].dtype == "float"
    valid_values = preview_df["numeric_string"].dropna()
    assert len(valid_values) == 3  # Three valid values: 100, 200, 300
    assert set(valid_values.astype(int)) == {100, 200, 300}
    assert preview_df["numeric_string"].isnull().sum() == 1  # One invalid value

    # Test aggregation with the cast column
    # Use 'max' method which is supported for VARCHAR (like in the unit test)
    feature = event_view.groupby("cust_id").aggregate_over(
        value_column="numeric_string",
        method="max",
        windows=["7d"],
        feature_names=["max_numeric_7d"],
        feature_job_setting=FeatureJobSetting(blind_spot="10m", period="30m", offset="5m"),
    )["max_numeric_7d"]

    # Test preview - cust_id=1 should have max of 200 (from 100, 200)
    feat_preview_df = feature.preview(
        observation_set=pd.DataFrame([{"POINT_IN_TIME": "2001-01-02", "cust_id": 1}])
    )
    assert feat_preview_df["max_numeric_7d"].iloc[0] == 200.0

    # Test preview - cust_id=2 should have max of 300 (invalid value is NULL, not counted)
    feat_preview_df2 = feature.preview(
        observation_set=pd.DataFrame([{"POINT_IN_TIME": "2001-01-02", "cust_id": 2}])
    )
    assert feat_preview_df2["max_numeric_7d"].iloc[0] == 300.0

    # Test historical features
    df_training_events = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2001-01-02 00:00:00", "2001-01-02 00:00:00"]),
        "cust_id": [1, 2],
    })
    hist_feat = FeatureList([feature], name="cast_numeric_test").compute_historical_features(
        df_training_events
    )
    assert hist_feat["max_numeric_7d"].tolist() == [200.0, 300.0]
