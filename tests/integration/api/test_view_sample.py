"""
Test API Data and View objects sample function
"""
import pandas as pd
import pytest
from pandas.testing import assert_series_equal
from pydantic.error_wrappers import ValidationError

from featurebyte.api.dimension_view import DimensionView
from featurebyte.api.event_view import EventView
from featurebyte.api.item_view import ItemView


@pytest.fixture(scope="session")
def source_type():
    """
    Source type(s) to test in this module
    """
    return "snowflake"


def test_event_data_sample(event_data):
    """
    Test event data sample & event data column sample
    """
    event_data_df = event_data.sample()
    ts_col = "ËVENT_TIMESTAMP"
    ev_ts = event_data[ts_col].sample()
    pd.testing.assert_frame_equal(event_data_df[[ts_col]], ev_ts)


def test_event_view_sample(event_data):
    """
    Test sample for EventView
    """
    sample_kwargs = {"size": 10, "seed": 1234}
    event_view = EventView.from_event_data(event_data)
    sample_df = event_view.sample(**sample_kwargs)
    assert sample_df.columns.tolist() == [
        "ËVENT_TIMESTAMP",
        "CREATED_AT",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
        "SESSION_ID",
        "ÀMOUNT",
        "TRANSACTION_ID",
    ]

    assert sample_df.shape == (10, 8)
    assert sample_df["ËVENT_TIMESTAMP"].min() == pd.Timestamp("2001-01-06 03:42:00.000640+10:00")
    assert sample_df["ËVENT_TIMESTAMP"].max() == pd.Timestamp("2001-10-14 13:57:21.000525+06:00")

    # test view column
    ts_col = "ËVENT_TIMESTAMP"
    ev_ts = event_view[ts_col].sample(**sample_kwargs)
    pd.testing.assert_frame_equal(sample_df[[ts_col]], ev_ts)


def test_event_view_sample_seed(event_data):
    """
    Test sample for EventView using a different seed
    """
    event_view = EventView.from_event_data(event_data)
    sample_df = event_view.sample(size=10, seed=4321)
    assert sample_df.shape == (10, 8)
    assert sample_df["ËVENT_TIMESTAMP"].min() == pd.Timestamp("2001-01-01 22:23:02.000349+22:00")
    assert sample_df["ËVENT_TIMESTAMP"].max() == pd.Timestamp("2001-10-05 14:34:01.000068+10:00")


def test_event_view_sample_with_date_range(event_data):
    """
    Test sample for EventView with date range
    """
    event_view = EventView.from_event_data(event_data)
    sample_params = {
        "size": 15,
        "seed": 1234,
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }
    sample_df = event_view.sample(**sample_params)
    assert sample_df.shape == (15, 8)
    assert sample_df["ËVENT_TIMESTAMP"].min() == pd.Timestamp("2001-10-10 18:58:16.000637+13:00")
    assert sample_df["ËVENT_TIMESTAMP"].max() == pd.Timestamp("2001-10-13 13:12:06.000903+09:00")

    col_sample_df = event_view["TRANSACTION_ID"].sample(**sample_params)
    assert_series_equal(col_sample_df["TRANSACTION_ID"], sample_df["TRANSACTION_ID"])


def test_item_view_sample(item_data):
    """
    Test sample for ItemView
    """
    item_view = ItemView.from_item_data(item_data)
    sample_df = item_view.sample(size=10, seed=1234)
    assert sample_df.columns.tolist() == [
        "ËVENT_TIMESTAMP",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
        "order_id",
        "item_id",
        "item_type",
    ]

    assert sample_df.shape == (10, 7)
    assert sample_df["ËVENT_TIMESTAMP"].min() == pd.Timestamp("2001-01-02 21:55:20.000561+1000")
    assert sample_df["ËVENT_TIMESTAMP"].max() == pd.Timestamp("2001-12-27 14:51:49.000824+1300")


def test_item_view_sample_with_date_range(item_data):
    """
    Test sample for ItemView with date range
    """
    item_view = ItemView.from_item_data(item_data)
    sample_params = {
        "size": 15,
        "seed": 1234,
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }
    sample_df = item_view.sample(**sample_params)
    assert sample_df.shape == (15, 7)
    assert sample_df["ËVENT_TIMESTAMP"].min() == pd.Timestamp("2001-10-10 18:12:15.000088+1400")
    assert sample_df["ËVENT_TIMESTAMP"].max() == pd.Timestamp("2001-10-14 16:08:02.000346+2200")

    col_sample_df = item_view["item_id"].sample(**sample_params)
    assert_series_equal(col_sample_df["item_id"], sample_df["item_id"])


def test_dimension_view_sample(dimension_data):
    """
    Test sample for DimensionView
    """
    dimension_view = DimensionView.from_dimension_data(dimension_data)
    sample_df = dimension_view.sample(size=10, seed=1234)
    assert sample_df.columns.tolist() == [
        "created_at",
        "item_id",
        "item_name",
        "item_type",
    ]

    assert sample_df.shape == (10, 4)


def test_dimension_view_sample_with_date_range(dimension_data):
    """
    Test sample for DimensionView with date range
    """
    dimension_view = DimensionView.from_dimension_data(dimension_data)
    with pytest.raises(ValidationError) as exc:
        dimension_view.sample(
            size=15, seed=1234, from_timestamp="2001-10-10", to_timestamp="2001-10-14"
        )
        assert "timestamp_column must be specified." in str(exc)
