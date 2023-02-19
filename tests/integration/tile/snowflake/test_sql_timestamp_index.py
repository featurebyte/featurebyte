"""
This module contains integration tests between timestamp and tile index conversion
"""
import pytest


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.parametrize(
    "time_modulo_frequency_seconds,blind_spot_seconds,frequency_minute,test_input,expected",
    [
        (15, 25, 1, 27585172, "2022-06-13T08:51:50.000Z"),
        (15, 25, 1, 27585173, "2022-06-13T08:52:50.000Z"),
        (15, 100, 2, 13792603, "2022-06-13T09:24:35.000Z"),
        (15, 100, 2, 13792604, "2022-06-13T09:26:35.000Z"),
    ],
)
@pytest.mark.asyncio
async def test_index_to_timestamp(
    session,
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
    result = await session.execute_query(sql)
    assert result["TS"].iloc[0] == expected
