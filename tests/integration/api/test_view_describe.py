"""
Test API View objects describe function
"""
import pandas as pd
from pandas.testing import assert_series_equal

from featurebyte.api.dimension_view import DimensionView
from featurebyte.api.event_view import EventView
from featurebyte.api.item_view import ItemView
from featurebyte.api.scd_view import SlowlyChangingView


def test_event_view_describe(snowflake_event_data):
    """
    Test describe for EventView
    """
    event_view = EventView.from_event_data(snowflake_event_data)

    describe_df = event_view.describe()
    assert describe_df.columns.tolist() == [
        "EVENT_TIMESTAMP",
        "CREATED_AT",
        "CUST_ID",
        "USER ID",
        "PRODUCT_ACTION",
        "SESSION_ID",
        "AMOUNT",
        "TRANSACTION_ID",
    ]
    assert describe_df.index.tolist() == [
        "dtype",
        "unique",
        "%missing",
        "%empty",
        "entropy",
        "top",
        "freq",
        "mean",
        "std",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
        "min TZ offset",
        "max TZ offset",
    ]

    assert describe_df.shape == (16, 8)
    assert describe_df.EVENT_TIMESTAMP["min"] == pd.to_datetime("2001-01-01 01:35:16.000223+01:00")
    assert describe_df.EVENT_TIMESTAMP["max"] == pd.to_datetime("2002-01-02 18:08:53.000960+22:00")


def test_event_view_describe_with_date_range(snowflake_event_data):
    """
    Test describe for EventView with date range
    """
    event_view = EventView.from_event_data(snowflake_event_data)
    sample_params = {
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }
    describe_df = event_view.describe(**sample_params)
    assert describe_df.shape == (16, 8)
    assert describe_df.EVENT_TIMESTAMP["min"] == pd.to_datetime("2001-10-10 00:15:16.000751+00:00")
    assert describe_df.EVENT_TIMESTAMP["max"] == pd.to_datetime("2001-10-14 17:51:02.000709+19:00")

    # describe single non-numeric column
    col_describe_df = event_view["TRANSACTION_ID"].describe(**sample_params)
    assert_series_equal(
        col_describe_df["TRANSACTION_ID"],
        describe_df["TRANSACTION_ID"][
            ["dtype", "unique", "%missing", "%empty", "entropy", "top", "freq"]
        ],
    )

    # describe single numeric column
    col_describe_df = event_view["AMOUNT"].describe(**sample_params)
    assert_series_equal(
        col_describe_df["AMOUNT"],
        describe_df["AMOUNT"][
            ["dtype", "unique", "%missing", "mean", "std", "min", "25%", "50%", "75%", "max"]
        ],
        check_dtype=False,
    )


def test_item_view_describe(snowflake_item_data):
    """
    Test describe for ItemView
    """
    item_view = ItemView.from_item_data(snowflake_item_data)

    describe_df = item_view.describe()
    assert describe_df.columns.tolist() == [
        "EVENT_TIMESTAMP",
        "CUST_ID",
        "USER ID",
        "PRODUCT_ACTION",
        "order_id",
        "item_id",
        "item_type",
    ]
    assert describe_df.index.tolist() == [
        "dtype",
        "unique",
        "%missing",
        "%empty",
        "entropy",
        "top",
        "freq",
        "mean",
        "std",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
        "min TZ offset",
        "max TZ offset",
    ]

    assert describe_df.shape == (16, 7)
    assert describe_df.EVENT_TIMESTAMP["min"] == pd.to_datetime("2001-01-01 01:35:16.000223+01:00")
    assert describe_df.EVENT_TIMESTAMP["max"] == pd.to_datetime("2002-01-02 18:08:53.000960+22:00")


def test_dimension_view_describe(snowflake_dimension_data):
    """
    Test sample for DimensionView
    """
    dimension_view = DimensionView.from_dimension_data(snowflake_dimension_data)
    describe_df = dimension_view.describe()
    assert describe_df.columns.tolist() == [
        "created_at",
        "item_id",
        "item_name",
        "item_type",
    ]
    assert describe_df.index.tolist() == [
        "dtype",
        "unique",
        "%missing",
        "%empty",
        "entropy",
        "top",
        "freq",
        "min",
        "max",
    ]
    assert describe_df.shape == (9, 4)


def test_scd_view_describe(snowflake_scd_data):
    """
    Test sample for DimensionView
    """
    scd_view = SlowlyChangingView.from_slowly_changing_data(snowflake_scd_data)
    describe_df = scd_view.describe()
    assert describe_df.columns.tolist() == [
        "Effective Timestamp",
        "User ID",
        "User Status",
        "ID",
    ]
    assert describe_df.index.tolist() == [
        "dtype",
        "unique",
        "%missing",
        "%empty",
        "entropy",
        "top",
        "freq",
        "mean",
        "std",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
        "min TZ offset",
        "max TZ offset",
    ]

    assert describe_df.shape == (16, 4)
    assert describe_df["Effective Timestamp"]["min"] == pd.to_datetime("2001-01-01 12:00:00+00:00")
    assert describe_df["Effective Timestamp"]["max"] == pd.to_datetime("2002-01-01 04:00:00+00:00")
