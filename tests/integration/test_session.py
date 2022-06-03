"""
This module contains session to EventView integration tests
"""
from featurebyte.core.event_view import EventView
from featurebyte.session.sqlite import SQLiteSession


def test_query_object_operation_on_sqlite_source(sqlite_db_filename):
    """
    Test loading event view from sqlite source
    """
    session = SQLiteSession(filename=sqlite_db_filename)
    event_view = EventView.from_session(
        session, table_name='"browsing_raw"', timestamp_column="created_at"
    )
    assert event_view.columns == ["created_at", "cust_id", "product_action", "session_id"]

    event_view["cust_id_x_session_id"] = event_view["cust_id"] * event_view["session_id"]
    event_view["lucky_customer"] = (event_view["cust_id_x_session_id"] / 1000) > 146.0
