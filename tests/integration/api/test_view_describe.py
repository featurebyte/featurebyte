"""
Test API View objects describe function
"""

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_series_equal


def _to_utc_no_offset(date):
    """
    Comvert timestamp to timezone naive UTC
    """
    return pd.to_datetime(date, utc=True).tz_localize(None)


@pytest.mark.parametrize("default_columns_batch_size", [None, 2])
def test_event_view_describe(event_table, default_columns_batch_size):
    """
    Test describe for EventView
    """
    event_view = event_table.get_view()

    if default_columns_batch_size is not None:
        with patch(
            "featurebyte.service.preview.DEFAULT_COLUMNS_BATCH_SIZE",
            default_columns_batch_size,
        ):
            describe_df = event_view.describe()
    else:
        describe_df = event_view.describe()

    assert describe_df.columns.tolist() == [
        "ËVENT_TIMESTAMP",
        "CREATED_AT",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
        "SESSION_ID",
        "ÀMOUNT",
        "TZ_OFFSET",
        "TRANSACTION_ID",
        "EMBEDDING_ARRAY",
        "ARRAY",
        "FLAT_DICT",
        "NESTED_DICT",
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
    if event_table.name == "snowflake_event_table":
        # Snowflake event table has timestamps with tz offsets
        expected_row_idx += [
            "min TZ offset",
            "max TZ offset",
        ]

    assert describe_df.index.tolist() == expected_row_idx
    assert describe_df.shape == (len(expected_row_idx), 13)
    assert _to_utc_no_offset(describe_df["ËVENT_TIMESTAMP"]["min"]) == expected_min_timestamp
    assert _to_utc_no_offset(describe_df["ËVENT_TIMESTAMP"]["max"]) == expected_max_timestamp


def test_event_view_describe_with_date_range(event_table):
    """
    Test describe for EventView with date range
    """
    event_view = event_table.get_view()
    sample_params = {
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }

    describe_df = event_view.describe(**sample_params)
    expected_num_rows = 16
    expected_min_timestamp = pd.to_datetime("2001-10-10 00:15:16.000751")
    expected_max_timestamp = pd.to_datetime("2001-10-13 23:50:48.000003")
    if event_table.name != "snowflake_event_table":
        expected_num_rows = 14

    assert describe_df.shape == (expected_num_rows, 13)
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


def test_event_view_describe_with_date_range_and_size(event_table):
    """
    Test describe for EventView with date range and number of rows
    """
    event_view = event_table.get_view()
    sample_params = {
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-11-10",
        "size": 100,
    }
    describe_df = event_view.describe(**sample_params)
    col_describe_df = event_view["ÀMOUNT"].describe(**sample_params)
    assert_series_equal(
        col_describe_df["ÀMOUNT"],
        describe_df["ÀMOUNT"][
            [
                "dtype",
                "unique",
                "%missing",
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


def test_item_view_describe(item_table):
    """
    Test describe for ItemView
    """
    item_view = item_table.get_view()

    describe_df = item_view.describe()
    assert describe_df.columns.tolist() == [
        "order_id",
        "item_id",
        "item_type",
        "ËVENT_TIMESTAMP",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
        "TZ_OFFSET",
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
    if item_table.name == "snowflake_item_table":
        # Snowflake event table has timestamps with tz offsets
        expected_row_idx += [
            "min TZ offset",
            "max TZ offset",
        ]

    assert describe_df.index.tolist() == expected_row_idx
    assert describe_df.shape == (len(expected_row_idx), 8)
    assert _to_utc_no_offset(describe_df["ËVENT_TIMESTAMP"]["min"]) == expected_min_timestamp
    assert _to_utc_no_offset(describe_df["ËVENT_TIMESTAMP"]["max"]) == expected_max_timestamp


def test_dimension_view_describe(dimension_table):
    """
    Test sample for DimensionView
    """
    dimension_view = dimension_table.get_view()
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


def test_scd_view_describe(scd_table):
    """
    Test sample for DimensionView
    """
    scd_view = scd_table.get_view()
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
    if scd_table.name == "snowflake_scd_table":
        # Snowflake event table has timestamps with tz offsets
        expected_row_idx += [
            "min TZ offset",
            "max TZ offset",
        ]

    assert describe_df.index.tolist() == expected_row_idx
    assert describe_df.shape == (len(expected_row_idx), 4)
    assert _to_utc_no_offset(describe_df["Effective Timestamp"]["min"]) == expected_min_timestamp
    assert _to_utc_no_offset(describe_df["Effective Timestamp"]["max"]) == expected_max_timestamp


def test_describe_empty_view(event_table):
    """
    Test describe for an empty view
    """
    event_view = event_table.get_view()
    view = event_view[event_view["ÀMOUNT"] > 10000000]
    assert view.preview().shape[0] == 0

    # check describe should not error
    describe_df = view.describe()
    assert (describe_df.loc["unique"] == 0).all()


@pytest.mark.asyncio
async def test_describe_invalid_dates(source_table_with_invalid_dates):
    """
    Test describe with invalid dates
    """
    describe_df = source_table_with_invalid_dates.describe()
    assert describe_df.shape[0] > 0
    result = describe_df.iloc[:, 1]
    expected = pd.Series(
        {
            "dtype": "TIMESTAMP",
            "unique": 4,
            "%missing": 0.0,
            "top": np.nan,
            "freq": np.nan,
            "mean": np.nan,
            "std": np.nan,
            "min": "2021-01-01T10:00:00.000000000",
            "25%": np.nan,
            "50%": np.nan,
            "75%": np.nan,
            "max": "2023-01-01T10:00:00.000000000",
        }
    )
    pd.testing.assert_series_equal(result, expected, check_names=False)
