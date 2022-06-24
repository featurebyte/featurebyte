"""
This module contains session to EventView integration tests
"""
import pandas as pd

from featurebyte.api.database_source import DatabaseSource
from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView


def test_query_object_operation_on_sqlite_source(sqlite_session, transaction_data, config):
    """
    Test loading event view from sqlite source
    """
    _ = sqlite_session
    sqlite_database_source = DatabaseSource(**config.db_sources["sqlite_datasource"].dict())
    assert sqlite_database_source.list_tables(config=config) == ["test_table"]

    sqlite_database_table = sqlite_database_source["test_table", config]
    expected_dtypes = pd.Series(
        {
            "created_at": "INT",
            "cust_id": "INT",
            "product_action": "VARCHAR",
            "session_id": "INT",
        }
    )
    pd.testing.assert_series_equal(expected_dtypes, sqlite_database_table.dtypes)

    event_data = EventData.from_tabular_source(
        tabular_source=sqlite_database_table,
        name="sqlite_event_data",
        event_timestamp_column="created_at",
        credentials=config.credentials,
    )
    event_view = EventView.from_event_data(event_data)
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


def test_query_object_operation_on_snowflake_source(
    snowflake_session, transaction_data_upper_case, config
):
    """
    Test loading event view from snowflake source
    """
    _ = snowflake_session
    snowflake_database_source = DatabaseSource(**config.db_sources["snowflake_datasource"].dict())
    assert snowflake_database_source.list_tables(config=config) == ["TEST_TABLE"]

    snowflake_database_table = snowflake_database_source["TEST_TABLE", config]
    expected_dtypes = pd.Series(
        {
            "CREATED_AT": "INT",
            "CUST_ID": "INT",
            "PRODUCT_ACTION": "VARCHAR",
            "SESSION_ID": "INT",
        }
    )
    pd.testing.assert_series_equal(expected_dtypes, snowflake_database_table.dtypes)

    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="snowflake_event_data",
        event_timestamp_column="CREATED_AT",
        credentials=config.credentials,
    )
    event_view = EventView.from_event_data(event_data)
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
