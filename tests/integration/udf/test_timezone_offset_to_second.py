"""
This module contains integration tests for F_TIMEZONE_OFFSET_TO_SECOND
"""

import pytest

from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.session.base import BaseSession
from tests.integration.udf.util import execute_query_with_udf


@pytest.mark.parametrize(
    "timezone_offset, expected",
    [
        ("+08:00", 8 * 3600),
        ("-05:30", -(5 * 3600 + 30 * 60)),
    ],
)
@pytest.mark.asyncio
async def test_timezone_offset_to_second__valid(session, timezone_offset, expected):
    """
    Test conversion of timezone offset to seconds
    """
    res = await execute_query_with_udf(
        session, "F_TIMEZONE_OFFSET_TO_SECOND", [make_literal_value(timezone_offset)]
    )
    assert res == expected


@pytest.mark.parametrize(
    "timezone_offset",
    [
        "+ab:cd",
        "+123:456",
    ],
)
@pytest.mark.asyncio
async def test_timezone_offset_to_second__invalid(session, timezone_offset):
    """
    Test conversion of timezone offset to seconds
    """
    assert isinstance(session, BaseSession)
    with pytest.raises(session._no_schema_error):
        await execute_query_with_udf(
            session, "F_TIMEZONE_OFFSET_TO_SECOND", [make_literal_value(timezone_offset)]
        )
