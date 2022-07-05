"""
This module contains integration tests between timestamp and tile index conversion
"""
import pytest


@pytest.mark.parametrize(
    "time_modulo_frequency_seconds,blind_spot_seconds,frequency_minute,test_input,expected",
    [
        (15, 25, 1, "2022-06-13T08:51:50.000Z", 27585172),
        (15, 25, 1, "2022-06-13T08:52:49.000Z", 27585172),
        (15, 25, 1, "2022-06-13T08:52:50.000Z", 27585173),
        (15, 25, 1, "2022-06-13 08:51:50", 27585172),
        (15, 25, 1, "2022-06-13 08:52:49", 27585172),
        (15, 25, 1, "2022-06-13 08:52:50", 27585173),
        (15, 100, 2, "2022-06-13T09:24:35.000Z", 13792603),
        (15, 100, 2, "2022-06-13T09:25:35.000Z", 13792603),
        (15, 100, 2, "2022-06-13T09:26:34.000Z", 13792603),
        (15, 100, 2, "2022-06-13T09:26:35.000Z", 13792604),
    ],
)
def test_timestamp_to_index(
    snowflake_session,
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
    result = snowflake_session.execute_query(sql)
    assert result["INDEX"].iloc[0] == expected


@pytest.mark.parametrize(
    "time_modulo_frequency_seconds,blind_spot_seconds,frequency_minute,test_input,expected",
    [
        (15, 25, 1, 27585172, "2022-06-13T08:51:50.000Z"),
        (15, 25, 1, 27585173, "2022-06-13T08:52:50.000Z"),
        (15, 100, 2, 13792603, "2022-06-13T09:24:35.000Z"),
        (15, 100, 2, 13792604, "2022-06-13T09:26:35.000Z"),
    ],
)
def test_index_to_timestamp(
    snowflake_session,
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
    result = snowflake_session.execute_query(sql)
    assert result["TS"].iloc[0] == expected
