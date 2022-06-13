"""
This module contains integration tests between timestamp and tile index conversion
"""
import os

import pytest

from featurebyte.session.snowflake import SnowflakeSession


@pytest.fixture(name="fb_db_session")
def snowflake_featurebyte_session():
    """
    Create Snowflake session between timestamp and index conversion functions
    """
    database_name = os.getenv("SNOWFLAKE_DATABASE")
    session = SnowflakeSession(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=database_name,
        schema=os.getenv("SNOWFLAKE_SCHEMA_FEATUREBYTE"),
    )
    sql_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "sql")
    with open(os.path.join(sql_dir, "F_TIMESTAMP_TO_INDEX.sql"), encoding="utf8") as file:
        func_sql_1 = file.read()
    with open(os.path.join(sql_dir, "F_INDEX_TO_TIMESTAMP.sql"), encoding="utf8") as file:
        func_sql_2 = file.read()

    session.execute_query(func_sql_1)
    session.execute_query(func_sql_2)

    yield session

    session.execute_query("DROP FUNCTION F_TIMESTAMP_TO_INDEX(VARCHAR, NUMBER, NUMBER, NUMBER)")
    session.execute_query("DROP FUNCTION F_INDEX_TO_TIMESTAMP(NUMBER, NUMBER, NUMBER, NUMBER)")


@pytest.mark.parametrize(
    "time_modulo_frequency_seconds,blind_spot_seconds,frequency_minute,test_input,expected",
    [
        (15, 25, 1, "2022-06-13T08:51:50.000Z", 27585172),
        (15, 25, 1, "2022-06-13T08:52:49.000Z", 27585172),
        (15, 25, 1, "2022-06-13T08:52:50.000Z", 27585173),
        (15, 25, 1, "2022-06-13 08:51:50", 27585172),
        (15, 25, 1, "2022-06-13 08:52:49", 27585172),
        (15, 25, 1, "2022-06-13 08:52:50", 27585173),
        (15, 100, 1, "2022-06-13T09:25:35.000Z", 27585207),
        (15, 100, 1, "2022-06-13T09:26:34.000Z", 27585207),
        (15, 100, 1, "2022-06-13T09:26:35.000Z", 27585208),
        (15, 100, 2, "2022-06-13T09:24:35.000Z", 13792603),
        (15, 100, 2, "2022-06-13T09:25:35.000Z", 13792603),
        (15, 100, 2, "2022-06-13T09:26:34.000Z", 13792603),
        (15, 100, 2, "2022-06-13T09:26:35.000Z", 13792604),
    ],
)
def test_timestamp_to_index(
    fb_db_session,
    time_modulo_frequency_seconds,
    blind_spot_seconds,
    frequency_minute,
    test_input,
    expected,
):
    """
    Test timestamp to tile index conversion with both iso/non-iso format and different job settings
    """
    sql = f"SELECT F_TIMESTAMP_TO_INDEX('{test_input}', {time_modulo_frequency_seconds}, {blind_spot_seconds}, {frequency_minute}) as INDEX"
    result = fb_db_session.execute_query(sql)
    assert result["INDEX"].iloc[0] == expected


@pytest.mark.parametrize(
    "time_modulo_frequency_seconds,blind_spot_seconds,frequency_minute,test_input,expected",
    [
        (15, 25, 1, 27585172, "2022-06-13T08:51:50.000Z"),
        (15, 25, 1, 27585173, "2022-06-13T08:52:50.000Z"),
        (15, 100, 1, 27585207, "2022-06-13T09:25:35.000Z"),
        (15, 100, 1, 27585208, "2022-06-13T09:26:35.000Z"),
        (15, 100, 2, 13792603, "2022-06-13T09:24:35.000Z"),
        (15, 100, 2, 13792604, "2022-06-13T09:26:35.000Z"),
    ],
)
def test_index_to_timestamp(
    fb_db_session,
    time_modulo_frequency_seconds,
    blind_spot_seconds,
    frequency_minute,
    test_input,
    expected,
):
    """
    Test tile index conversion to timestamp conversion with different job settings
    """
    sql = f"SELECT F_INDEX_TO_TIMESTAMP({test_input}, {time_modulo_frequency_seconds}, {blind_spot_seconds}, {frequency_minute}) as TS"
    result = fb_db_session.execute_query(sql)
    assert result["TS"].iloc[0] == expected
