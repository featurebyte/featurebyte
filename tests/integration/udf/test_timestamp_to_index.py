"""
This module contains integration tests for F_TIMESTAMP_TO_INDEX UDF
"""
import dateutil.parser
import pytest

from featurebyte.common import date_util
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
@pytest.mark.asyncio
async def test_timestamp_to_index(
    db_session,
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
    assert isinstance(db_session, BaseSession)
    result = await db_session.execute_query(sql)
    res = result["INDEX"].iloc[0]
    assert res == expected

    tile_ind = date_util.timestamp_utc_to_tile_index(
        dateutil.parser.isoparse(test_input),
        time_modulo_frequency_seconds,
        blind_spot_seconds,
        frequency_minute,
    )
    assert tile_ind == expected


@pytest.mark.parametrize(
    "db_session",
    [
        "snowflake",
        # "databricks",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_input,time_modulo_frequency_seconds,blind_spot_seconds,frequency_minute,expected",
    [
        (27585172, 15, 25, 1, "2022-06-13T08:51:50.000Z"),
        (
            27585173,
            15,
            25,
            1,
            "2022-06-13T08:52:50.000Z",
        ),
        (13792603, 15, 100, 2, "2022-06-13T09:24:35.000Z"),
        (13792604, 15, 100, 2, "2022-06-13T09:26:35.000Z"),
    ],
)
@pytest.mark.asyncio
async def test_index_to_timestamp(
    db_session,
    test_input,
    time_modulo_frequency_seconds,
    blind_spot_seconds,
    frequency_minute,
    expected,
):
    """
    Test timestamp to tile index conversion with both iso/non-iso format and different job settings
    """
    sql = f"SELECT F_INDEX_TO_TIMESTAMP({test_input}, {time_modulo_frequency_seconds}, {blind_spot_seconds}, {frequency_minute}) as TIMESTAMP"
    assert isinstance(db_session, BaseSession)
    result = await db_session.execute_query(sql)
    res = result["TIMESTAMP"].iloc[0]
    assert res == expected

    ts = date_util.tile_index_to_timestamp_utc(
        test_input,
        time_modulo_frequency_seconds,
        blind_spot_seconds,
        frequency_minute,
    )
    ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    assert ts_str == expected
