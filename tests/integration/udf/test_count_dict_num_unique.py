"""
Tests for count dict num unique UDF
"""

import numpy as np
import pytest

from tests.integration.udf.util import OVERFLOW_INT, execute_query_with_udf


@pytest.mark.parametrize(
    "dictionary, expected",
    [
        (None, 0),
        ({}, 0),
        ({"a": 1}, 1),
        ({"a": 1, "b": 1, "c": 1}, 3),
        ({"a": OVERFLOW_INT}, 1),
    ],
)
@pytest.mark.asyncio
async def test_count_dict_num_unique_udf(session, to_object, dictionary, expected):
    """
    Test count dict num unique UDF
    """
    dictionary_expr = to_object(dictionary)
    actual = await execute_query_with_udf(session, "F_COUNT_DICT_NUM_UNIQUE", [dictionary_expr])
    if actual is None:
        actual = np.nan
    np.testing.assert_allclose(actual, expected, 1e-5)
