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


def test_event_view_sample(snowflake_event_data):
    """
    Test sample for EventView
    """
    event_view = EventView.from_event_data(snowflake_event_data)
    sample_df = event_view.sample(size=10, seed=1234)
    assert sample_df.columns.tolist() == [
        "EVENT_TIMESTAMP",
        "CREATED_AT",
        "CUST_ID",
        "USER ID",
        "PRODUCT_ACTION",
        "SESSION_ID",
        "AMOUNT",
        "TRANSACTION_ID",
    ]

    assert sample_df.shape == (10, 8)
    assert sample_df.EVENT_TIMESTAMP.min() == pd.Timestamp("2001-01-05 17:42:00.000640")
    assert sample_df.EVENT_TIMESTAMP.max() == pd.Timestamp("2001-10-14 07:57:21.000525")


def test_event_view_sample_with_date_range(snowflake_event_data):
    """
    Test sample for EventView with date range
    """
    event_view = EventView.from_event_data(snowflake_event_data)
    sample_params = {
        "size": 15,
        "seed": 1234,
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }
    sample_df = event_view.sample(**sample_params)
    assert sample_df.shape == (15, 8)
    assert sample_df.EVENT_TIMESTAMP.min() == pd.Timestamp("2001-10-10 05:58:16.000637")
    assert sample_df.EVENT_TIMESTAMP.max() == pd.Timestamp("2001-10-13 04:12:06.000903")

    col_sample_df = event_view["TRANSACTION_ID"].sample(**sample_params)
    assert_series_equal(col_sample_df["TRANSACTION_ID"], sample_df["TRANSACTION_ID"])


def test_item_view_sample(snowflake_item_data):
    """
    Test sample for ItemView
    """
    item_view = ItemView.from_item_data(snowflake_item_data)
    sample_df = item_view.sample(size=10, seed=1234)
    assert sample_df.columns.tolist() == [
        "EVENT_TIMESTAMP",
        "USER ID",
        "PRODUCT_ACTION",
        "order_id",
        "item_id",
        "item_type",
    ]

    assert sample_df.shape == (10, 6)
    assert sample_df.EVENT_TIMESTAMP.min() == pd.Timestamp("2001-01-03 12:45:53.000756")
    assert sample_df.EVENT_TIMESTAMP.max() == pd.Timestamp("2001-12-08 23:37:22.000888")


def test_item_view_sample_with_date_range(snowflake_item_data):
    """
    Test sample for ItemView with date range
    """
    item_view = ItemView.from_item_data(snowflake_item_data)
    sample_params = {
        "size": 15,
        "seed": 1234,
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }
    sample_df = item_view.sample(**sample_params)
    assert sample_df.shape == (15, 6)
    assert sample_df.EVENT_TIMESTAMP.min() == pd.Timestamp("2001-10-10 05:58:16.000637")
    assert sample_df.EVENT_TIMESTAMP.max() == pd.Timestamp("2001-10-13 23:50:48.000003")

    col_sample_df = item_view["item_id"].sample(**sample_params)
    assert_series_equal(col_sample_df["item_id"], sample_df["item_id"])


def test_dimension_view_sample(snowflake_dimension_data):
    """
    Test sample for DimensionView
    """
    dimension_view = DimensionView.from_dimension_data(snowflake_dimension_data)
    sample_df = dimension_view.sample(size=10, seed=1234)
    assert sample_df.columns.tolist() == [
        "created_at",
        "item_id",
        "item_name",
        "item_type",
    ]

    assert sample_df.shape == (10, 4)


def test_dimension_view_sample_with_date_range(snowflake_dimension_data):
    """
    Test sample for DimensionView with date range
    """
    dimension_view = DimensionView.from_dimension_data(snowflake_dimension_data)
    with pytest.raises(ValidationError) as exc:
        dimension_view.sample(
            size=15, seed=1234, from_timestamp="2001-10-10", to_timestamp="2001-10-14"
        )
        assert "timestamp_column must be specified." in str(exc)
