"""
Tests for snowflake get rank UDF
"""
import numpy as np
import pytest

from tests.integration.udf.snowflake.util import to_object

same_values = {"a": 1, "b": 1, "c": 1}
ascending_values = {"a": 1, "b": 2, "c": 3}


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
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
async def test_get_rank_udf(session, dictionary, key, is_descending, expected):
    """
    Test get rank UDF
    """
    dictionary_expr = to_object(dictionary)
    query = f"SELECT F_GET_RANK({dictionary_expr}, {key}, {is_descending}) AS OUT"
    df = await session.execute_query(query)
    np.testing.assert_allclose(df.iloc[0]["OUT"], expected, 1e-5)
