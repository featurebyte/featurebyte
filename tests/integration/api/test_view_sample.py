"""
Test API Data and View objects sample function
"""

from typing import Any

import pandas as pd
import pytest
from pandas.testing import assert_series_equal
from pydantic import ValidationError


def maybe_tz_convert(ts: Any):
    """
    If ts is time zone aware, convert it to UTC and remove timezone information
    """
    if isinstance(ts, pd.Series) and ts.dt.tz is not None:
        return ts.dt.tz_convert(None)
    elif isinstance(ts, pd.Timestamp) and ts.tz is not None:
        return ts.tz_convert(None)
    return ts


def test_event_table_sample(event_table, source_type):
    """
    Test event table sample & event table column sample
    """
    sample_kwargs = {"from_timestamp": "2001-01-01", "to_timestamp": "2001-02-01"}
    event_table_df = event_table.sample(**sample_kwargs)

    ts_col = "ËVENT_TIMESTAMP"
    ev_ts = event_table[ts_col].sample(**sample_kwargs)

    # bigquery view sample and series sample are not expected to produce the same result
    if source_type != "bigquery":
        pd.testing.assert_frame_equal(event_table_df[[ts_col]], ev_ts)

    # check the sample result
    assert ev_ts.shape[0] == 10
    assert maybe_tz_convert(event_table_df[ts_col].min()) >= pd.Timestamp(
        sample_kwargs["from_timestamp"]
    )
    assert maybe_tz_convert(event_table_df[ts_col].max()) <= pd.Timestamp(
        sample_kwargs["to_timestamp"]
    )


@pytest.mark.parametrize(
    "source_type, expected_min, expected_max",
    [
        (
            "snowflake",
            pd.Timestamp("2001-01-24 06:13:34.000562+1200"),
            pd.Timestamp("2001-10-30 21:17:24.000101-0400"),
        ),
        (
            "spark",
            pd.Timestamp("2001-01-05 03:26:02.000051"),
            pd.Timestamp("2001-12-25 20:06:37.000928"),
        ),
    ],
    indirect=["source_type"],
    scope="session",
)
def test_event_view_sample(event_table, expected_min, expected_max):
    """
    Test sample for EventView
    """
    sample_kwargs = {"size": 10, "seed": 1234}
    event_view = event_table.get_view()
    sample_df = event_view.sample(**sample_kwargs)
    assert set(sample_df.columns.tolist()) == {
        "ËVENT_TIMESTAMP",
        "CREATED_AT",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
        "SESSION_ID",
        "ÀMOUNT",
        "TZ_OFFSET",
        "TRANSACTION_ID",
        "TIMESTAMP_STRING",
        "EMBEDDING_ARRAY",
        "ARRAY",
        "ARRAY_STRING",
        "FLAT_DICT",
        "NESTED_DICT",
    }

    assert sample_df.shape == (10, 15)
    actual_min = sample_df["ËVENT_TIMESTAMP"].min()
    actual_max = sample_df["ËVENT_TIMESTAMP"].max()
    assert (actual_min, actual_max) == (expected_min, expected_max)

    # test view column
    ts_col = "ËVENT_TIMESTAMP"
    ev_ts = event_view[ts_col].sample(**sample_kwargs)
    pd.testing.assert_frame_equal(sample_df[[ts_col]], ev_ts)


@pytest.mark.parametrize(
    "source_type, expected_min, expected_max",
    [
        (
            "snowflake",
            pd.Timestamp("2001-01-14 13:10:27.000894+1100"),
            pd.Timestamp("2001-11-28 13:13:55.000988-1500"),
        ),
        (
            "spark",
            pd.Timestamp("2001-01-03 22:23:17.000382"),
            pd.Timestamp("2001-12-30 06:14:35.000448"),
        ),
    ],
    indirect=["source_type"],
    scope="session",
)
def test_event_view_sample_seed(event_table, expected_min, expected_max):
    """
    Test sample for EventView using a different seed
    """
    event_view = event_table.get_view()
    sample_df = event_view.sample(size=10, seed=4321)
    assert sample_df.shape == (10, 15)
    actual_min = sample_df["ËVENT_TIMESTAMP"].min()
    actual_max = sample_df["ËVENT_TIMESTAMP"].max()
    assert (actual_min, actual_max) == (expected_min, expected_max)


@pytest.mark.parametrize(
    "source_type, expected_min, expected_max",
    [
        (
            "snowflake",
            pd.Timestamp("2001-10-10 00:44:07.000193-0700"),
            pd.Timestamp("2001-10-13 14:51:02.000709-0800"),
        ),
        (
            "spark",
            pd.Timestamp("2001-10-10 03:52:49.000447"),
            pd.Timestamp("2001-10-13 23:50:48.000003"),
        ),
    ],
    indirect=["source_type"],
    scope="session",
)
def test_event_view_sample_with_date_range(event_table, expected_min, expected_max):
    """
    Test sample for EventView with date range
    """
    event_view = event_table.get_view()
    sample_params = {
        "size": 15,
        "seed": 1234,
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }
    sample_df = event_view.sample(**sample_params)
    assert sample_df.shape == (15, 15)
    actual_min = sample_df["ËVENT_TIMESTAMP"].min()
    actual_max = sample_df["ËVENT_TIMESTAMP"].max()
    assert (actual_min, actual_max) == (expected_min, expected_max)

    col_sample_df = event_view["TRANSACTION_ID"].sample(**sample_params)
    assert_series_equal(col_sample_df["TRANSACTION_ID"], sample_df["TRANSACTION_ID"])


@pytest.mark.parametrize(
    "source_type, expected_min, expected_max, expected_row_num",
    [
        (
            "snowflake",
            pd.Timestamp("2001-02-14 11:42:07.000848-1000"),
            pd.Timestamp("2001-04-05 11:36:38.000227-1200"),
            10,
        ),
        (
            "spark",
            pd.Timestamp("2001-01-24 07:34:32.000383"),
            pd.Timestamp("2001-12-25 20:06:37.000928"),
            8,
        ),
    ],
    indirect=["source_type"],
    scope="session",
)
def test_item_view_sample(item_table, expected_min, expected_max, expected_row_num):
    """
    Test sample for ItemView
    """
    item_view = item_table.get_view()
    sample_df = item_view.sample(size=10, seed=1234)
    assert sample_df.columns.tolist() == [
        "order_id",
        "item_id",
        "item_type",
        "ËVENT_TIMESTAMP",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
        "TZ_OFFSET",
    ]

    assert sample_df.shape == (expected_row_num, 8)
    actual_min = sample_df["ËVENT_TIMESTAMP"].min()
    actual_max = sample_df["ËVENT_TIMESTAMP"].max()
    assert (actual_min, actual_max) == (expected_min, expected_max)


@pytest.mark.parametrize(
    "source_type, expected_min, expected_max, expected_row_num",
    [
        (
            "snowflake",
            pd.Timestamp("2001-10-10 00:44:07.000193-0700"),
            pd.Timestamp("2001-10-12 23:58:39.000579-1200"),
            15,
        ),
        (
            "spark",
            pd.Timestamp("2001-10-10 03:52:49.000447"),
            pd.Timestamp("2001-10-12 04:50:19.000719"),
            14,
        ),
    ],
    indirect=["source_type"],
    scope="session",
)
def test_item_view_sample_with_date_range(item_table, expected_min, expected_max, expected_row_num):
    """
    Test sample for ItemView with date range
    """
    item_view = item_table.get_view()
    sample_params = {
        "size": 15,
        "seed": 1234,
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }
    sample_df = item_view.sample(**sample_params)
    assert sample_df.shape == (expected_row_num, 8)
    actual_min = sample_df["ËVENT_TIMESTAMP"].min()
    actual_max = sample_df["ËVENT_TIMESTAMP"].max()
    assert (actual_min, actual_max) == (expected_min, expected_max)

    col_sample_df = item_view["item_id"].sample(**sample_params)
    assert col_sample_df.shape[0] == expected_row_num
    assert col_sample_df.columns.tolist() == ["item_id"]


def test_dimension_view_sample(dimension_table):
    """
    Test sample for DimensionView
    """
    dimension_view = dimension_table.get_view()
    sample_df = dimension_view.sample(size=10, seed=1234)
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
        dimension_view.sample(
            size=15, seed=1234, from_timestamp="2001-10-10", to_timestamp="2001-10-14"
        )
        assert "timestamp_column must be specified." in str(exc)


@pytest.mark.asyncio
async def test_sample_invalid_dates(session, source_table_with_invalid_dates):
    """
    Test sample with invalid dates
    """
    sample_df = source_table_with_invalid_dates.sample()
    expected = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "date_col": pd.to_datetime([
            "2021-01-01 10:00:00",
            None,
            None,
            None,
            None,
            "2023-01-01 10:00:00",
        ]),
    })
    sample_df = sample_df.sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(sample_df, expected)
