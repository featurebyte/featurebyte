"""
Tests for snowflake get rank UDF
"""

import numpy as np
import pytest
from sqlglot import parse_one

from featurebyte.query_graph.sql.ast.literal import make_literal_value
from tests.integration.udf.util import execute_query_with_udf

same_values = {"a": 1, "b": 1, "c": 1}
ascending_values = {"a": 1, "b": 2, "c": 3}


@pytest.mark.parametrize(
    "dictionary, key, is_descending, expected",
    [
        (None, "null", False, np.nan),
        ({"a": 1}, "null", False, np.nan),
        (same_values, "'non_existing_key'", False, np.nan),
        (same_values, "'a'", False, 1),
        (same_values, "'c'", False, 1),
        (ascending_values, "'a'", False, 1),
        (ascending_values, "'c'", False, 3),
        (same_values, "'a'", True, 1),
        (same_values, "'c'", True, 1),
        (ascending_values, "'a'", True, 3),
        (ascending_values, "'c'", True, 1),
        ({"a": 1, "b": 1, "c": 2}, "'c'", False, 3),
        ({"a": 1, "b": 1, "c": 2}, "'c'", True, 1),
        ({"a": 1, "b": 2, "c": 2, "d": 3}, "'d'", False, 4),
        ({"a": 1, "b": 2, "c": 2, "d": 3}, "'c'", False, 2),
    ],
)
@pytest.mark.asyncio
async def test_get_rank_udf(session, to_object, dictionary, key, is_descending, expected):
    """
    Test get rank UDF
    """
    dictionary_expr = to_object(dictionary)
    actual = await execute_query_with_udf(
        session,
        "F_GET_RANK",
        [
            dictionary_expr,
            parse_one(key),
            make_literal_value(is_descending),
        ],
    )
    if actual is None:
        actual = np.nan
    np.testing.assert_allclose(actual, expected, 1e-5)
