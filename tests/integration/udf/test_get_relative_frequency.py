"""
Tests for snowflake get relative frequency UDF
"""

import numpy as np
import pytest
from sqlglot import parse_one

from tests.integration.udf.util import execute_query_with_udf

same_values = {"a": 1, "b": 1, "c": 1}
ascending_values = {"a": 1, "b": 2, "c": 3}


@pytest.mark.parametrize(
    "dictionary, key, expected",
    [
        (None, "null", np.nan),
        ({"a": 1}, "null", np.nan),
        (same_values, "'non_existing_key'", 0),
        (same_values, "'a'", float(1 / 3)),
        (same_values, "'c'", float(1 / 3)),
        (ascending_values, "'a'", float(1 / 6)),
        (ascending_values, "'c'", float(1 / 2)),
        ({"a": 0}, "'a'", np.nan),
    ],
)
@pytest.mark.asyncio
async def test_get_relative_frequency_udf(session, to_object, dictionary, key, expected):
    """
    Test get relative frequency UDF
    """
    dictionary_expr = to_object(dictionary)
    actual = await execute_query_with_udf(
        session, "F_GET_RELATIVE_FREQUENCY", [dictionary_expr, parse_one(key)]
    )
    if actual is None:
        actual = np.nan
    np.testing.assert_allclose(actual, expected, 1e-5)
