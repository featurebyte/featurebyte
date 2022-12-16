"""
Test API View objects describe function
"""
import pandas as pd
import pytest
from pandas.testing import assert_series_equal
from pydantic.error_wrappers import ValidationError

from featurebyte.api.dimension_view import DimensionView
from featurebyte.api.event_view import EventView
from featurebyte.api.item_view import ItemView


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
        "count",
        "unique",
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

    assert describe_df.shape == (11, 8)
    assert describe_df.EVENT_TIMESTAMP["min"] == "2001-01-01T00:23:02.000349000"
    assert describe_df.EVENT_TIMESTAMP["max"] == "2002-01-01T22:43:16.000409000"


def test_event_view_sample_with_date_range(snowflake_event_data):
    """
    Test describe for EventView with date range
    """
    event_view = EventView.from_event_data(snowflake_event_data)
    sample_params = {
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }
    describe_df = event_view.describe(**sample_params)
    assert describe_df.shape == (11, 8)
    assert describe_df.EVENT_TIMESTAMP["min"] == "2001-10-10T00:15:16.000751000"
    assert describe_df.EVENT_TIMESTAMP["max"] == "2001-10-13T23:50:48.000003000"

    # describe single non-numeric column
    col_describe_df = event_view["TRANSACTION_ID"].describe(**sample_params)
    assert_series_equal(
        col_describe_df["TRANSACTION_ID"],
        describe_df["TRANSACTION_ID"][["count", "unique", "top", "freq"]],
    )

    # describe single numeric column
    col_describe_df = event_view["AMOUNT"].describe(**sample_params)
    assert_series_equal(
        col_describe_df["AMOUNT"],
        describe_df["AMOUNT"][["count", "mean", "std", "min", "25%", "50%", "75%", "max"]],
        check_dtype=False,
    )
