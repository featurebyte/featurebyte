"""
Test vector cosine similarity
"""
import numpy as np
import pytest


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.parametrize(
    "array1, array2, expected",
    [
        ([], [], 0),
        ([1], [], 0),
        ([1, 2, 3], [4], 0.267261),
        ([1, 2, 3], [1, 2, 3], 1.0),
        ([1, 2, 3], [3, 2, 1], 0.714286),
    ],
)
@pytest.mark.asyncio
async def test_vector_cosine_similarity(to_object, session, array1, array2, expected):
    """
    Test vector cosine similarity
    """

    async def _check(a, b):
        query = f"SELECT F_VECTOR_COSINE_SIMILARITY({a}, {b}) AS OUT"
        df = await session.execute_query(query)
        actual = df.iloc[0]["OUT"]
        if actual is None:
            actual = 0
        np.testing.assert_allclose(actual, expected, 1e-5)

    await _check(array1, array2)
    await _check(array2, array1)
