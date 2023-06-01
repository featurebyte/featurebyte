"""
Tests for snowflake get relative freuqency UDF
"""
import numpy as np
import pytest

same_values = {"a": 1, "b": 1, "c": 1}
ascending_values = {"a": 1, "b": 2, "c": 3}


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
@pytest.mark.parametrize(
    "dictionary, key, expected",
    [
        (None, "null", np.nan),
        ({"a": 1}, "null", 0),
        (same_values, "'non_existing_key'", 0),
        (same_values, "'a'", float(1 / 3)),
        (same_values, "'c'", float(1 / 3)),
        (ascending_values, "'a'", float(1 / 6)),
        (ascending_values, "'c'", float(1 / 2)),
    ],
)
@pytest.mark.asyncio
async def test_get_relative_frequency_udf(session, to_object, dictionary, key, expected):
    """
    Test get relative frequency UDF
    """
    dictionary_expr = to_object(dictionary)
    query = f"SELECT F_GET_RELATIVE_FREQUENCY({dictionary_expr}, {key}) AS OUT"
    df = await session.execute_query(query)
    actual = df.iloc[0]["OUT"]
    if actual is None:
        actual = np.nan
    np.testing.assert_allclose(actual, expected, 1e-5)
