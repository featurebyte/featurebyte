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
    sqlite_database_source = DatabaseSource(**config.feature_stores["sqlite_datasource"].dict())
    assert sqlite_database_source.list_tables(credentials=config.credentials) == ["test_table"]

    sqlite_database_table = sqlite_database_source.get_table(
        database_name=None,
        schema_name=None,
        table_name="test_table",
        credentials=config.credentials,
    )
    expected_dtypes = pd.Series(
        {
            "event_timestamp": "VARCHAR",
            "created_at": "INT",
            "cust_id": "INT",
            "user_id": "INT",
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
    assert event_view.columns == [
        "event_timestamp",
        "created_at",
        "cust_id",
        "user_id",
        "product_action",
        "session_id",
    ]

    # need to specify the constant as float, otherwise results will get truncated
    event_view["cust_id_x_session_id"] = event_view["cust_id"] * event_view["session_id"] / 1000.0
    event_view["lucky_customer"] = event_view["cust_id_x_session_id"] > 140.0

    # construct expected results
    expected = transaction_data.copy()
    expected["cust_id_x_session_id"] = (expected["cust_id"] * expected["session_id"]) / 1000.0
    expected["lucky_customer"] = (expected["cust_id_x_session_id"] > 140.0).astype(int)

    # check agreement
    output = event_view.preview(limit=expected.shape[0], credentials=config.credentials)
    pd.testing.assert_frame_equal(output, expected[output.columns], check_dtype=False)


def test_query_object_operation_on_snowflake_source(
    snowflake_session, transaction_data_upper_case, config
):
    """
    Test loading event view from snowflake source
    """
    _ = snowflake_session
    table_name = "TEST_TABLE"
    snowflake_database_source = DatabaseSource(
        **config.feature_stores["snowflake_featurestore"].dict()
    )
    assert table_name in snowflake_database_source.list_tables(credentials=config.credentials)

    snowflake_database_table = snowflake_database_source.get_table(
        database_name=snowflake_session.database,
        schema_name=snowflake_session.sf_schema,
        table_name=table_name,
        credentials=config.credentials,
    )
    expected_dtypes = pd.Series(
        {
            "EVENT_TIMESTAMP": "TIMESTAMP",
            "CREATED_AT": "INT",
            "CUST_ID": "INT",
            "USER_ID": "INT",
            "PRODUCT_ACTION": "VARCHAR",
            "SESSION_ID": "INT",
        }
    )
    pd.testing.assert_series_equal(expected_dtypes, snowflake_database_table.dtypes)

    event_data = EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="snowflake_event_data",
        event_timestamp_column="EVENT_TIMESTAMP",
        credentials=config.credentials,
    )
    event_view = EventView.from_event_data(event_data)
    assert event_view.columns == [
        "EVENT_TIMESTAMP",
        "CREATED_AT",
        "CUST_ID",
        "USER_ID",
        "PRODUCT_ACTION",
        "SESSION_ID",
    ]

    # need to specify the constant as float, otherwise results will get truncated
    event_view["CUST_ID_X_SESSION_ID"] = event_view["CUST_ID"] * event_view["SESSION_ID"] / 1000.0
    event_view["LUCKY_CUSTOMER"] = event_view["CUST_ID_X_SESSION_ID"] > 140.0

    # construct expected results
    expected = transaction_data_upper_case.copy()
    expected["CUST_ID_X_SESSION_ID"] = (expected["CUST_ID"] * expected["SESSION_ID"]) / 1000.0
    expected["LUCKY_CUSTOMER"] = (expected["CUST_ID_X_SESSION_ID"] > 140.0).astype(int)

    # check agreement
    output = event_view.preview(limit=expected.shape[0], credentials=config.credentials)
    output["CUST_ID_X_SESSION_ID"] = output["CUST_ID_X_SESSION_ID"].astype(
        float
    )  # type is not correct here
    # the EVENT_TIMESTAMP has to be str for write_pandas to work correctly
    output["EVENT_TIMESTAMP"] = output["EVENT_TIMESTAMP"].astype(str)
    pd.testing.assert_frame_equal(output, expected[output.columns], check_dtype=False)

    # create some features
    event_view["USER_ID"].as_entity("User")
    feature_group = event_view.groupby("USER_ID").aggregate(
        "USER_ID",
        "count",
        windows=["2h", "24h"],
        blind_spot="30m",
        frequency="1h",
        time_modulo_frequency="30m",
        feature_names=["COUNT_2h", "COUNT_24h"],
    )

    # preview the features
    preview_param = {
        "POINT_IN_TIME": "2001-01-02 10:00:00",
        "USER_ID": 1,
    }
    df_feature_preview = feature_group.preview(
        preview_param,
        credentials=config.credentials,
    )
    assert df_feature_preview.shape[0] == 1
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "USER_ID": 1,
        "COUNT_2h": 1,
        "COUNT_24h": 9,
    }
