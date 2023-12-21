"""
Tests for least frequent UDF
"""
import numpy as np
import pytest


@pytest.mark.parametrize(
    "counts, expected",
    [
        ({}, None),
        ({"__MISSING__": None}, None),
        (None, None),
        ({"a": 1}, "a"),
        ({"a": 1, "b": 2, "c": 3}, "a"),
        ({"a": 1, "b": np.nan, "c": 3}, "a"),
        ({"a": np.nan}, None),
        ({"a": -1, "b": np.nan, "c": -3}, "c"),
    ],
)
@pytest.mark.asyncio
async def test_least_frequent_udf(session, to_object, counts, expected):
    """
    Test least frequent UDF
    """

    expr = to_object(counts)
    query = f"SELECT F_COUNT_DICT_LEAST_FREQUENT({expr}) AS OUT"
    df = await session.execute_query(query)
    actual = df.iloc[0]["OUT"]
    assert actual == expected
