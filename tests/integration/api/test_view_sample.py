"""
Test API Data and View objects sample function
"""
from typing import Any

import pandas as pd
import pytest
from pandas.testing import assert_series_equal
from pydantic.error_wrappers import ValidationError


def maybe_tz_convert(ts: Any):
    """
    If ts is time zone aware, convert it to UTC and remove timezone information
    """
    if isinstance(ts, pd.Series) and ts.dt.tz is not None:
        return ts.dt.tz_convert(None)
    elif isinstance(ts, pd.Timestamp) and ts.tz is not None:
        return ts.tz_convert(None)
    return ts


def test_event_table_sample(event_table):
    """
    Test event table sample & event table column sample
    """
    sample_kwargs = {"from_timestamp": "2001-01-01", "to_timestamp": "2001-02-01"}
    event_table_df = event_table.sample(**sample_kwargs)
    ts_col = "ËVENT_TIMESTAMP"
    ev_ts = event_table[ts_col].sample(**sample_kwargs)
    pd.testing.assert_frame_equal(event_table_df[[ts_col]], ev_ts)

    # check the sample result
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
            pd.Timestamp("2001-01-06 03:42:00.000640+10:00"),
            pd.Timestamp("2001-10-14 13:57:21.000525+06:00"),
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
    assert sample_df.columns.tolist() == [
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

    assert sample_df.shape == (10, 13)
    assert sample_df["ËVENT_TIMESTAMP"].min() == expected_min
    assert sample_df["ËVENT_TIMESTAMP"].max() == expected_max

    # test view column
    ts_col = "ËVENT_TIMESTAMP"
    ev_ts = event_view[ts_col].sample(**sample_kwargs)
    pd.testing.assert_frame_equal(sample_df[[ts_col]], ev_ts)


@pytest.mark.parametrize(
    "source_type, expected_min, expected_max",
    [
        (
            "snowflake",
            pd.Timestamp("2001-01-01 22:23:02.000349+22:00"),
            pd.Timestamp("2001-10-05 14:34:01.000068+10:00"),
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
    assert sample_df.shape == (10, 13)
    assert sample_df["ËVENT_TIMESTAMP"].min() == expected_min
    assert sample_df["ËVENT_TIMESTAMP"].max() == expected_max


@pytest.mark.parametrize(
    "source_type, expected_min, expected_max",
    [
        (
            "snowflake",
            pd.Timestamp("2001-10-10 18:58:16.000637+13:00"),
            pd.Timestamp("2001-10-13 13:12:06.000903+09:00"),
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
    assert sample_df.shape == (15, 13)
    assert sample_df["ËVENT_TIMESTAMP"].min() == expected_min
    assert sample_df["ËVENT_TIMESTAMP"].max() == expected_max

    col_sample_df = event_view["TRANSACTION_ID"].sample(**sample_params)
    assert_series_equal(col_sample_df["TRANSACTION_ID"], sample_df["TRANSACTION_ID"])


@pytest.mark.parametrize(
    "source_type, expected_min, expected_max",
    [
        (
            "snowflake",
            pd.Timestamp("2001-01-02 21:55:20.000561+1000"),
            pd.Timestamp("2001-12-27 14:51:49.000824+1300"),
        ),
        (
            "spark",
            pd.Timestamp("2001-01-03 22:12:15.000735"),
            pd.Timestamp("2001-12-22 18:28:52.000837"),
        ),
    ],
    indirect=["source_type"],
    scope="session",
)
def test_item_view_sample(item_table, expected_min, expected_max):
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

    assert sample_df.shape == (10, 8)
    assert sample_df["ËVENT_TIMESTAMP"].min() == expected_min
    assert sample_df["ËVENT_TIMESTAMP"].max() == expected_max


@pytest.mark.parametrize(
    "source_type, expected_min, expected_max",
    [
        (
            "snowflake",
            pd.Timestamp("2001-10-09 22:12:15.000088-0600"),
            pd.Timestamp("2001-10-14 04:08:02.000346+1000"),
        ),
        (
            "spark",
            pd.Timestamp("2001-10-10 00:15:16.000751"),
            pd.Timestamp("2001-10-13 12:04:24.000171"),
        ),
    ],
    indirect=["source_type"],
    scope="session",
)
def test_item_view_sample_with_date_range(item_table, expected_min, expected_max):
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
    assert sample_df.shape == (15, 8)
    assert sample_df["ËVENT_TIMESTAMP"].min() == expected_min
    assert sample_df["ËVENT_TIMESTAMP"].max() == expected_max

    col_sample_df = item_view["item_id"].sample(**sample_params)
    assert_series_equal(col_sample_df["item_id"], sample_df["item_id"])


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
