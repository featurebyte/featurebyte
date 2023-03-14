"""
Test API View objects describe function
"""
import pandas as pd
from pandas.testing import assert_series_equal

from featurebyte.api.dimension_view import DimensionView


def _to_utc_no_offset(date):
    """
    Comvert timestamp to timezone naive UTC
    """
    return pd.to_datetime(date, utc=True).tz_localize(None)


def test_event_view_describe(event_data):
    """
    Test describe for EventView
    """
    event_view = event_data.get_view()

    describe_df = event_view.describe()
    assert describe_df.columns.tolist() == [
        "ËVENT_TIMESTAMP",
        "CREATED_AT",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
        "SESSION_ID",
        "ÀMOUNT",
        "TRANSACTION_ID",
    ]

    expected_row_idx = [
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
    ]
    expected_min_timestamp = pd.to_datetime("2001-01-01 00:23:02.000349")
    expected_max_timestamp = pd.to_datetime("2002-01-01 22:43:16.000409")
    if event_data.name == "snowflake_event_data":
        # Snowflake event data has timestamps with tz offsets
        expected_row_idx += [
            "min TZ offset",
            "max TZ offset",
        ]

    assert describe_df.index.tolist() == expected_row_idx
    assert describe_df.shape == (len(expected_row_idx), 8)
    assert _to_utc_no_offset(describe_df["ËVENT_TIMESTAMP"]["min"]) == expected_min_timestamp
    assert _to_utc_no_offset(describe_df["ËVENT_TIMESTAMP"]["max"]) == expected_max_timestamp


def test_event_view_describe_with_date_range(event_data):
    """
    Test describe for EventView with date range
    """
    event_view = event_data.get_view()
    sample_params = {
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }

    describe_df = event_view.describe(**sample_params)
    expected_num_rows = 16
    expected_min_timestamp = pd.to_datetime("2001-10-10 00:15:16.000751")
    expected_max_timestamp = pd.to_datetime("2001-10-13 23:50:48.000003")
    if event_data.name != "snowflake_event_data":
        expected_num_rows = 14

    assert describe_df.shape == (expected_num_rows, 8)
    assert _to_utc_no_offset(describe_df["ËVENT_TIMESTAMP"]["min"]) == expected_min_timestamp
    assert _to_utc_no_offset(describe_df["ËVENT_TIMESTAMP"]["max"]) == expected_max_timestamp

    # describe single non-numeric column
    col_describe_df = event_view["TRANSACTION_ID"].describe(**sample_params)
    assert_series_equal(
        col_describe_df["TRANSACTION_ID"],
        describe_df["TRANSACTION_ID"][
            ["dtype", "unique", "%missing", "%empty", "entropy", "top", "freq"]
        ],
    )

    # describe single numeric column
    col_describe_df = event_view["ÀMOUNT"].describe(**sample_params)
    assert_series_equal(
        col_describe_df["ÀMOUNT"],
        describe_df["ÀMOUNT"][
            [
                "dtype",
                "unique",
                "%missing",
                "top",
                "freq",
                "mean",
                "std",
                "min",
                "25%",
                "50%",
                "75%",
                "max",
            ]
        ],
        check_dtype=False,
    )


def test_item_view_describe(item_data):
    """
    Test describe for ItemView
    """
    item_view = item_data.get_view()

    describe_df = item_view.describe()
    assert describe_df.columns.tolist() == [
        "ËVENT_TIMESTAMP",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
        "order_id",
        "item_id",
        "item_type",
    ]
    expected_row_idx = [
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
    ]
    expected_min_timestamp = pd.to_datetime("2001-01-01 00:23:02.000349")
    expected_max_timestamp = pd.to_datetime("2002-01-01 22:43:16.000409")
    if item_data.name == "snowflake_item_data":
        # Snowflake event data has timestamps with tz offsets
        expected_row_idx += [
            "min TZ offset",
            "max TZ offset",
        ]

    assert describe_df.index.tolist() == expected_row_idx
    assert describe_df.shape == (len(expected_row_idx), 7)
    assert _to_utc_no_offset(describe_df["ËVENT_TIMESTAMP"]["min"]) == expected_min_timestamp
    assert _to_utc_no_offset(describe_df["ËVENT_TIMESTAMP"]["max"]) == expected_max_timestamp


def test_dimension_view_describe(dimension_data):
    """
    Test sample for DimensionView
    """
    dimension_view = DimensionView.from_dimension_data(dimension_data)
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


def test_scd_view_describe(scd_data):
    """
    Test sample for DimensionView
    """
    scd_view = scd_data.get_view()
    describe_df = scd_view.describe()
    assert describe_df.columns.tolist() == [
        "Effective Timestamp",
        "User ID",
        "User Status",
        "ID",
    ]
    expected_row_idx = [
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
    ]
    expected_min_timestamp = pd.to_datetime("2001-01-01 12:00:00")
    expected_max_timestamp = pd.to_datetime("2002-01-01 04:00:00")
    if scd_data.name == "snowflake_scd_data":
        # Snowflake event data has timestamps with tz offsets
        expected_row_idx += [
            "min TZ offset",
            "max TZ offset",
        ]

    assert describe_df.index.tolist() == expected_row_idx
    assert describe_df.shape == (len(expected_row_idx), 4)
    assert _to_utc_no_offset(describe_df["Effective Timestamp"]["min"]) == expected_min_timestamp
    assert _to_utc_no_offset(describe_df["Effective Timestamp"]["max"]) == expected_max_timestamp
