"""
Tests for count dict divide UDF
"""

import json

import numpy as np
import pytest
from sqlglot import expressions

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

    assert actual is not None, f"Expected {expected}, got None"
    assert set(actual.keys()) == set(expected.keys()), (
        f"Keys mismatch: {actual.keys()} vs {expected.keys()}"
    )
    for key in expected:
        np.testing.assert_allclose(actual[key], expected[key], rtol=rtol)


@pytest.mark.parametrize(
    "dictionary, divisor, expected",
    [
        # Null inputs
        (None, 5.0, None),
        ({"a": 10}, None, None),
        # Zero divisor
        ({"a": 10}, 0.0, None),
        # Single value
        ({"a": 10}, 2.0, {"a": 5.0}),
        ({"a": 10}, 10.0, {"a": 1.0}),
        # Multiple values
        ({"a": 10, "b": 20}, 10.0, {"a": 1.0, "b": 2.0}),
        ({"a": 1, "b": 2, "c": 3}, 2.0, {"a": 0.5, "b": 1.0, "c": 1.5}),
        # Large values (overflow testing)
        (
            {"a": OVERFLOW_INT, "b": OVERFLOW_INT * 2},
            float(OVERFLOW_INT),
            {"a": 1.0, "b": 2.0},
        ),
    ],
)
@pytest.mark.asyncio
async def test_count_dict_divide_udf(session, to_object, dictionary, divisor, expected):
    """
    Test count dict divide UDF
    """
    dictionary_expr = to_object(dictionary)
    divisor_expr = (
        expressions.Literal.number(divisor) if divisor is not None else expressions.null()
    )
    actual = await execute_query_with_udf(
        session, "F_COUNT_DICT_DIVIDE", [dictionary_expr, divisor_expr]
    )
    assert_dict_allclose(actual, expected)
