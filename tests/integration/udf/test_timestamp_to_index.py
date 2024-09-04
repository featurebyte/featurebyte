"""
This module contains integration tests for F_TIMESTAMP_TO_INDEX UDF
"""

import pytest

from featurebyte.query_graph.sql.ast.literal import make_literal_value
from tests.integration.udf.util import execute_query_with_udf


@pytest.mark.asyncio
async def test_timestamp_to_index(
    session,
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

    res = await execute_query_with_udf(
        session,
        "F_TIMESTAMP_TO_INDEX",
        [
            f"CAST('{test_input}' AS TIMESTAMP)",
            make_literal_value(time_modulo_frequency_second),
            make_literal_value(blind_spot_second),
            make_literal_value(frequency_minute),
        ],
    )
    assert res == tile_index


@pytest.mark.asyncio
async def test_index_to_timestamp(
    session,
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

    res = await execute_query_with_udf(
        session,
        "F_INDEX_TO_TIMESTAMP",
        [
            make_literal_value(tile_index),
            make_literal_value(time_modulo_frequency_second),
            make_literal_value(blind_spot_second),
            make_literal_value(frequency_minute),
        ],
    )
    assert res == time_stamp_str
