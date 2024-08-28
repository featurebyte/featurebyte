"""
Tests for count dict entropy UDF
"""

import numpy as np
import pytest

from tests.integration.udf.util import execute_query_with_udf


@pytest.mark.parametrize(
    "dictionary, expected",
    [
        (None, np.nan),
        ({"a": 0}, 0),
        ({}, 0),
        ({"a": 1}, 0),
        ({"a": 1, "b": 0}, 0),
        ({"a": 1, "b": 1, "c": 1}, 1.098612),
        ({"a": 1, "b": 2, "c": 3}, 1.011404),
        ({"a": -1, "b": 2, "c": -3}, 1.011404),
    ],
)
@pytest.mark.asyncio
async def test_count_dict_entropy_udf(session, to_object, dictionary, expected):
    """
    Test count dict entropy UDF
    """
    dictionary_expr = to_object(dictionary)
    actual = await execute_query_with_udf(session, "F_COUNT_DICT_ENTROPY", [dictionary_expr])
    if actual is None:
        actual = np.nan
    np.testing.assert_allclose(actual, expected, 1e-5)
