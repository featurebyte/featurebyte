"""
This module contains session to EventView integration tests
"""
import os

import pandas as pd

from featurebyte.api.event_view import EventView


def test_query_object_operation_on_sqlite_source(sqlite_session, transaction_data):
    """
    Test loading event view from sqlite source
    """
    event_view = EventView.from_session(
        sqlite_session, table_name='"test_table"', timestamp_column="created_at"
    )
    assert event_view.columns == ["created_at", "cust_id", "product_action", "session_id"]

    # need to specify the constant as float, otherwise results will get truncated
    event_view["cust_id_x_session_id"] = event_view["cust_id"] * event_view["session_id"] / 1000.0
    event_view["lucky_customer"] = event_view["cust_id_x_session_id"] > 140.0

    # construct expected results
    expected = transaction_data.copy()
    expected["cust_id_x_session_id"] = (expected["cust_id"] * expected["session_id"]) / 1000.0
    expected["lucky_customer"] = (expected["cust_id_x_session_id"] > 140.0).astype(int)

    # check agreement
    output = event_view.preview(limit=expected.shape[0])
    pd.testing.assert_frame_equal(output, expected[output.columns], check_dtype=False)


def test_query_object_operation_on_snowflake_source(snowflake_session, transaction_data_upper_case):
    """
    Test loading event view from snowflake source
    """
    database_name = os.getenv("SNOWFLAKE_DATABASE")
    event_view = EventView.from_session(
        snowflake_session,
        table_name=f'"{database_name}"."PUBLIC"."TEST_TABLE"',
        timestamp_column="CREATED_AT",
    )
    assert event_view.columns == ["CREATED_AT", "CUST_ID", "PRODUCT_ACTION", "SESSION_ID"]

    # need to specify the constant as float, otherwise results will get truncated
    event_view["CUST_ID_X_SESSION_ID"] = event_view["CUST_ID"] * event_view["SESSION_ID"] / 1000.0
    event_view["LUCKY_CUSTOMER"] = event_view["CUST_ID_X_SESSION_ID"] > 140.0

    # construct expected results
    expected = transaction_data_upper_case.copy()
    expected["CUST_ID_X_SESSION_ID"] = (expected["CUST_ID"] * expected["SESSION_ID"]) / 1000.0
    expected["LUCKY_CUSTOMER"] = (expected["CUST_ID_X_SESSION_ID"] > 140.0).astype(int)

    # check agreement
    output = event_view.preview(limit=expected.shape[0])
    output["CUST_ID_X_SESSION_ID"] = output["CUST_ID_X_SESSION_ID"].astype(
        float
    )  # type is not correct here
    pd.testing.assert_frame_equal(output, expected[output.columns], check_dtype=False)
