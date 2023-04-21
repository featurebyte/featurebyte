"""
This module contains integration tests for F_TIMEZONE_OFFSET_TO_SECOND
"""
import pytest

from featurebyte.session.base import BaseSession


@pytest.mark.parametrize(
    "source_type",
    [
        # "snowflake",
        # "databricks",
        "spark",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "timezone_offset, expected",
    [
        ("+08:00", 8 * 3600),
        ("-05:30", -(5 * 3600 + 30 * 60)),
    ],
)
@pytest.mark.asyncio
async def test_timezone_offset_to_second(session, timezone_offset, expected):
    """
    Test conversion of timezone offset to seconds
    """
    query = f"SELECT F_TIMEZONE_OFFSET_TO_SECOND('{timezone_offset}') AS OUT"
    assert isinstance(session, BaseSession)
    result = await session.execute_query(query)
    res = result["OUT"].iloc[0]
    assert res == expected
