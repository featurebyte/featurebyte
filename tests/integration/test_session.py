"""
This module contains session to EventView integration tests
"""
import pandas as pd

from featurebyte.core.event_view import EventView
from featurebyte.session.sqlite import SQLiteSession


def test_query_object_operation_on_sqlite_source(sqlite_db_filename, transaction_data):
    """
    Test loading event view from sqlite source
    """
    session = SQLiteSession(filename=sqlite_db_filename)
    event_view = EventView.from_session(
        session, table_name='"browsing_raw"', timestamp_column="created_at"
    )
    assert event_view.columns == ["created_at", "cust_id", "product_action", "session_id"]

    event_view["cust_id_x_session_id"] = event_view["cust_id"] * event_view["session_id"]
    event_view["lucky_customer"] = (event_view["cust_id_x_session_id"] / 1000) > 140.0

    # construct expected results
    expected = transaction_data.copy()
    expected["cust_id_x_session_id"] = expected["cust_id"] * expected["session_id"]
    expected["lucky_customer"] = (expected["cust_id_x_session_id"] / 1000) > 140.0

    # check agreement
    output = event_view.preview(limit=expected.shape[0])
    pd.testing.assert_frame_equal(output, expected[output.columns], check_dtype=False)
