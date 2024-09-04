"""
Tests for least frequent UDF
"""

import json

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
        ({"a": 1, "b": 2, "c": 3}, "a"),
        ({"a": 1, "b": np.nan, "c": 3}, "a"),
        ({"a": np.nan}, None),
        ({"a": -1, "b": np.nan, "c": -3}, "c"),
        (
            json.loads(
                '{"\\u00e0dd": 338.51, "r\\u00ebmove": 11.39, "__MISSING__": 234.77, "purchase": 225.78, "detail": 194.16000000000003}'
            ),
            "rëmove",
        ),
    ],
)
@pytest.mark.asyncio
async def test_least_frequent_udf(session, to_object, counts, expected):
    """
    Test least frequent UDF
    """

    expr = to_object(counts)
    actual = await execute_query_with_udf(session, "F_COUNT_DICT_LEAST_FREQUENT", [expr])
    assert actual == expected
