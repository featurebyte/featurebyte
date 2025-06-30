"""
Test API View objects describe function
"""

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_series_equal
from sqlglot import expressions

from featurebyte.query_graph.sql.interpreter import PreviewMixin
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY


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

    assert set(describe_df.columns.tolist()) == {
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
        "ARRAY_STRING",
        "FLAT_DICT",
        "NESTED_DICT",
        "TIMESTAMP_STRING",
    }

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
    assert describe_df.shape == (len(expected_row_idx), 15)
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

    assert describe_df.shape == (expected_num_rows, 15)
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


@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
def test_event_view_describe_with_date_range_and_size(event_table):
    """
    Test describe for EventView with date range and number of rows

    Skipped for BigQuery as the sampling is not deterministic.
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
    event_view = event_table.get_view()[["ÀMOUNT"]]
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
    expected = pd.Series({
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
    })
    pd.testing.assert_series_equal(result, expected, check_names=False)


@pytest.mark.parametrize("source_type", ["spark", "databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_databricks_varchar_with_max_length(session, feature_store, catalog):
    """
    Test describe on tables with varchar columns with maximum length
    """
    _ = catalog

    await session.execute_query("CREATE TABLE TABLE_VARCHAR_VALID (COL_A VARCHAR(10))")
    await session.execute_query(
        """
        INSERT INTO TABLE_VARCHAR_VALID (COL_A)
        VALUES
            ('First'),
            ('Second'),
            ('Third');
        """
    )
    ds = feature_store.get_data_source()
    source_table = ds.get_source_table(
        table_name="TABLE_VARCHAR_VALID",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    result = source_table.describe()
    assert result.index.tolist() == [
        "dtype",
        "unique",
        "%missing",
        "%empty",
        "entropy",
        "top",
        "freq",
    ]
    assert result.loc["dtype"][0] == "VARCHAR"

    result = source_table.sample()
    assert result.shape[0] > 0


def test_dynamic_batching(event_table):
    """
    Test describe for EventView
    """
    original_construct_stats_sql = PreviewMixin._construct_stats_sql

    num_patched_queries = {"count": 0}

    def patched_construct_stats_sql(*args, **kwargs):
        """
        Patch construct_stats_sql to deliberately cause error when there are more than 5 columns
        in the select statement. This is to test dynamic batching of describe queries
        """
        select_expr, *remaining = original_construct_stats_sql(*args, **kwargs)
        columns = kwargs["columns"]
        if len(columns) > 5:
            select_expr = select_expr.select(
                expressions.alias_(
                    expressions.Anonymous(this="FAIL_NOW"), alias="_debug_col", quoted=True
                )
            )
            num_patched_queries["count"] += 1
        return select_expr, *remaining

    event_view = event_table.get_view()

    with patch.object(
        PreviewMixin,
        "_construct_stats_sql",
        new=patched_construct_stats_sql,
    ):
        describe_df = event_view.describe()

    assert num_patched_queries["count"] > 0
    assert set(describe_df.columns.tolist()) == {
        "TIMESTAMP_STRING",
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
        "ARRAY_STRING",
        "FLAT_DICT",
        "NESTED_DICT",
    }


def test_event_view_with_timestamp_schema_describe_with_date_range(
    event_table_with_timestamp_schema,
):
    """
    Test describe for EventView with date range
    """
    event_view = event_table_with_timestamp_schema.get_view()
    sample_params = {
        "from_timestamp": "2001-10-10",
        "to_timestamp": "2001-10-14",
    }
    describe_df = event_view.describe(**sample_params)
    raise
