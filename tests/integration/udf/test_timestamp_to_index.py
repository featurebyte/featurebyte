"""
This module contains integration tests for F_TIMESTAMP_TO_INDEX UDF
"""
import pytest

from featurebyte.session.base import BaseSession


@pytest.fixture(name="db_session", scope="session")
def db_session_fixture(request):
    if request.param == "snowflake":
        return request.getfixturevalue("snowflake_session")
    elif request.param == "databricks":
        return request.getfixturevalue("databricks_session")
    raise NotImplementedError(f"{request.param}")


@pytest.mark.parametrize(
    "db_session",
    [
        "snowflake",
        # "databricks",
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_timestamp_to_index(
    db_session,
    timestamp_to_index_fixture,
):
    """
    Test timestamp to tile index conversion with both iso/non-iso format and different job settings
    """
    (
        time_modulo_frequency_second,
        blind_spot_second,
        frequency_minute,
        test_input,
        tile_index,
    ) = timestamp_to_index_fixture

    sql = f"SELECT F_TIMESTAMP_TO_INDEX('{test_input}', {time_modulo_frequency_second}, {blind_spot_second}, {frequency_minute}) as INDEX"
    assert isinstance(db_session, BaseSession)
    result = await db_session.execute_query(sql)
    res = result["INDEX"].iloc[0]
    assert res == tile_index


@pytest.mark.parametrize(
    "db_session",
    [
        "snowflake",
        # "databricks",
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_index_to_timestamp(
    db_session,
    index_to_timestamp_fixture,
):
    """
    Test timestamp to tile index conversion with both iso/non-iso format and different job settings
    """
    (
        tile_index,
        time_modulo_frequency_second,
        blind_spot_second,
        frequency_minute,
        time_stamp_str,
    ) = index_to_timestamp_fixture

    sql = f"SELECT F_INDEX_TO_TIMESTAMP({tile_index}, {time_modulo_frequency_second}, {blind_spot_second}, {frequency_minute}) as TIMESTAMP"
    assert isinstance(db_session, BaseSession)
    result = await db_session.execute_query(sql)
    res = result["TIMESTAMP"].iloc[0]
    assert res == time_stamp_str
