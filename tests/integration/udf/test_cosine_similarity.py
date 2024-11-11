"""
Tests for snowflake cosine similarity UDF
"""

import numpy as np
import pytest

from tests.integration.udf.util import execute_query_with_udf


@pytest.mark.parametrize(
    "counts1, counts2, expected",
    [
        ({}, {}, 0),
        (None, None, np.nan),
        ({"a": 1}, None, np.nan),
        ({"a": 1}, {}, 0),
        ({"a": 1, "b": 2, "c": 3}, {"x": 1}, 0),
        ({"a": 1, "b": 2, "c": 3}, {"a": 1, "x": 1}, 0.188982),
        ({"a": 1, "b": 2, "c": 3}, {"a": 1}, 0.267261),
        ({"a": 1, "b": 2, "c": 3}, {"b": 2}, 0.53452248),
        ({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2}, 0.5976143),
        ({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 3}, 1.0),
        ({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": None}, 0.597614),
        ({"a": 1, "b": np.nan, "c": 3}, {"a": 1, "b": 2, "c": None}, 0.141421),
        ({"a": 1}, {"b": 2, "c": None}, 0),
        ({"a": 0}, {"b": 0}, 0),
        ({"a": 123123123123123}, {"a": 123123123123123}, 1.0),
    ],
)
@pytest.mark.asyncio
async def test_cosine_similarity_udf(session, to_object, counts1, counts2, expected):
    """
    Test cosine similarity UDF
    """

    async def _check(a, b):
        a_expr = to_object(a)
        b_expr = to_object(b)
        actual = await execute_query_with_udf(
            session, "F_COUNT_DICT_COSINE_SIMILARITY", [a_expr, b_expr]
        )
        if actual is None:
            actual = np.nan
        np.testing.assert_allclose(actual, expected, 1e-5)

    await _check(counts1, counts2)
    await _check(counts2, counts1)
