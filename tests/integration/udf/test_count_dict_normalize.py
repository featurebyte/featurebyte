"""
Tests for count dict normalize UDF
"""

import json

import numpy as np
import pytest

from tests.integration.udf.util import OVERFLOW_INT, execute_query_with_udf


def assert_dict_allclose(actual, expected, rtol=1e-5):
    """
    Assert that two dictionaries have the same keys and values within tolerance.
    """
    # Parse JSON string if returned as string (e.g., Snowflake VARIANT)
    if isinstance(actual, str):
        actual = json.loads(actual)

    if expected is None:
        assert actual is None
        return
    if expected == {}:
        assert actual in (None, {}), f"Expected empty dict, got {actual}"
        return

    assert actual is not None, f"Expected {expected}, got None"
    assert set(actual.keys()) == set(expected.keys()), (
        f"Keys mismatch: {actual.keys()} vs {expected.keys()}"
    )
    for key in expected:
        np.testing.assert_allclose(actual[key], expected[key], rtol=rtol)


@pytest.mark.parametrize(
    "dictionary, expected",
    [
        # Edge cases
        (None, None),
        ({}, {}),
        ({"a": 0}, {}),  # Sum is 0, return empty dict
        ({"a": 0, "b": 0}, {}),  # Sum is 0, return empty dict
        # Single value
        ({"a": 1}, {"a": 1.0}),
        ({"a": 5}, {"a": 1.0}),
        # Multiple values - equal weights
        ({"a": 1, "b": 1}, {"a": 0.5, "b": 0.5}),
        ({"a": 2, "b": 2, "c": 2}, {"a": 1 / 3, "b": 1 / 3, "c": 1 / 3}),
        # Multiple values - different weights
        ({"a": 1, "b": 2, "c": 3}, {"a": 1 / 6, "b": 2 / 6, "c": 3 / 6}),
        ({"a": 1, "b": 3}, {"a": 0.25, "b": 0.75}),
        # Large values (overflow testing)
        (
            {"a": OVERFLOW_INT, "b": OVERFLOW_INT * 2, "c": OVERFLOW_INT * 3},
            {"a": 1 / 6, "b": 2 / 6, "c": 3 / 6},
        ),
    ],
)
@pytest.mark.asyncio
async def test_count_dict_normalize_udf(session, to_object, dictionary, expected):
    """
    Test count dict normalize UDF
    """
    dictionary_expr = to_object(dictionary)
    actual = await execute_query_with_udf(session, "F_COUNT_DICT_NORMALIZE", [dictionary_expr])
    assert_dict_allclose(actual, expected)
