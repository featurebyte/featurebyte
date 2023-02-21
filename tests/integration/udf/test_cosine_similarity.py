"""
Tests for snowflake cosine similarity UDF
"""
import numpy as np
import pytest

from tests.integration.udf.snowflake.util import to_object


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
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
    ],
)
@pytest.mark.asyncio
async def test_cosine_similarity_udf(session, counts1, counts2, expected):
    """
    Test cosine similarity UDF
    """

    async def _check(a, b):
        a_expr = to_object(a)
        b_expr = to_object(b)
        query = f"SELECT F_COUNT_DICT_COSINE_SIMILARITY({a_expr}, {b_expr}) AS OUT"
        df = await session.execute_query(query)
        np.testing.assert_allclose(df.iloc[0]["OUT"], expected, 1e-5)

    await _check(counts1, counts2)
    await _check(counts2, counts1)
