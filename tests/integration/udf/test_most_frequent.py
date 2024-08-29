"""
Tests for most frequent UDF
"""

import numpy as np
import pytest

from tests.integration.udf.util import execute_query_with_udf


@pytest.mark.parametrize(
    "counts, expected",
    [
        ({}, None),
        ({"__MISSING__": None}, None),
        (None, None),
        ({"a": 1}, "a"),
        ({"a": 1, "b": 2, "c": 3}, "c"),
        ({"a": 1, "b": np.nan, "c": 3}, "c"),
        ({"a": np.nan}, None),
        ({"a": -1, "b": np.nan, "c": -3}, "a"),
    ],
)
@pytest.mark.asyncio
async def test_most_frequent_udf(session, to_object, counts, expected):
    """
    Test most frequent UDF
    """

    expr = to_object(counts)
    actual = await execute_query_with_udf(session, "F_COUNT_DICT_MOST_FREQUENT", [expr])
    assert actual == expected


@pytest.mark.parametrize(
    "counts, expected",
    [
        ({}, np.nan),
        ({"__MISSING__": None}, np.nan),
        # (None, np.nan),  skipped because spark returns None bypassing UDF call
        ({"a": 1}, 1),
        ({"a": 1, "b": 2, "c": 3}, 3),
        ({"a": 1, "b": np.nan, "c": 3.5}, 3.5),
        ({"a": np.nan}, np.nan),
    ],
)
@pytest.mark.asyncio
async def test_most_frequent_value_udf(session, to_object, counts, expected):
    """
    Test most frequent value UDF
    """

    expr = to_object(counts)
    actual = await execute_query_with_udf(session, "F_COUNT_DICT_MOST_FREQUENT_VALUE", [expr])
    np.testing.assert_allclose(actual, expected, equal_nan=True)
