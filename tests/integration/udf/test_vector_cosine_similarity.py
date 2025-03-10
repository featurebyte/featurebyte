"""
Test vector cosine similarity
"""

import numpy as np
import pytest

from tests.integration.udf.util import execute_query_with_udf


@pytest.mark.parametrize(
    "array1, array2, expected",
    [
        (None, None, 0),
        ([], [], 0),
        ([1], [], None),
        ([1, 2, 3], [4], None),
        ([1, 2, 3], [1, 2, 3], 1.0),
        ([1, 2, 3], [3, 2, 1], 0.714286),
    ],
)
@pytest.mark.asyncio
async def test_vector_cosine_similarity(to_array, session, array1, array2, expected):
    """
    Test vector cosine similarity
    """

    async def _check(a, b):
        array_expr_a = to_array(a)
        array_expr_b = to_array(b)

        # If expected is None, this means we expect an error.
        if expected is None:
            with pytest.raises(Exception):
                await execute_query_with_udf(
                    session,
                    "F_VECTOR_COSINE_SIMILARITY",
                    [array_expr_a, array_expr_b],
                )
            return

        # If expected is not None, proceed to assert the result.
        actual = await execute_query_with_udf(
            session,
            "F_VECTOR_COSINE_SIMILARITY",
            [array_expr_a, array_expr_b],
        )
        if actual is None:
            actual = 0
        np.testing.assert_allclose(actual, expected, 1e-5)

    await _check(array1, array2)
    await _check(array2, array1)
