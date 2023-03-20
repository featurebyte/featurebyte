"""
Test API Data and View objects sample function
"""
import pandas as pd
import pytest
from pandas.testing import assert_series_equal
from pydantic.error_wrappers import ValidationError

from tests.integration.api.feature_preview_utils import _to_utc_no_offset


def test_event_table_sample(event_table):
    """
    Test event table sample & event table column sample
    """
    sample_df = event_table.sample()
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


def test_event_view_sample(event_table):
    """
    Test sample for EventView
    """
    sample_kwargs = {"size": 10}
    event_view = event_table.get_view()
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


def test_event_view_sample_with_date_range(event_table):
    """
    Test sample for EventView with date range
    """
    event_view = event_table.get_view()
    sample_params = {
        "size": 15,
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }
    sample_df = event_view.sample(**sample_params)
    assert sample_df.shape == (15, 8)
    assert _to_utc_no_offset(sample_df["ËVENT_TIMESTAMP"].min()) >= pd.Timestamp(
        "2001-10-10 00:00:00"
    )
    assert _to_utc_no_offset(sample_df["ËVENT_TIMESTAMP"].max()) <= pd.Timestamp(
        "2001-10-14 00:00:00"
    )


def test_item_view_sample(item_table):
    """
    Test sample for ItemView
    """
    item_view = item_table.get_view()
    sample_df = item_view.sample(size=10)
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


def test_item_view_sample_with_date_range(item_table):
    """
    Test sample for ItemView with date range
    """
    item_view = item_table.get_view()
    sample_params = {
        "size": 15,
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }
    sample_df = item_view.sample(**sample_params)
    assert sample_df.shape == (15, 7)
    assert _to_utc_no_offset(sample_df["ËVENT_TIMESTAMP"].min()) >= pd.Timestamp(
        "2001-10-10 00:00:00"
    )
    assert _to_utc_no_offset(sample_df["ËVENT_TIMESTAMP"].max()) <= pd.Timestamp(
        "2001-10-14 00:00:00"
    )


def test_dimension_view_sample(dimension_table):
    """
    Test sample for DimensionView
    """
    dimension_view = dimension_table.get_view()
    sample_df = dimension_view.sample(size=10)
    assert sample_df.columns.tolist() == [
        "created_at",
        "item_id",
        "item_name",
        "item_type",
    ]

    assert sample_df.shape == (10, 4)


def test_dimension_view_sample_with_date_range(dimension_table):
    """
    Test sample for DimensionView with date range
    """
    dimension_view = dimension_table.get_view()
    with pytest.raises(ValidationError) as exc:
        dimension_view.sample(size=15, from_timestamp="2001-10-10", to_timestamp="2001-10-14")
        assert "timestamp_column must be specified." in str(exc)
