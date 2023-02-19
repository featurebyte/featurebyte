"""
Tests for snowflake get relative freuqency UDF
"""
import numpy as np
import pytest

from tests.integration.udf.snowflake.util import to_object

same_values = {"a": 1, "b": 1, "c": 1}
ascending_values = {"a": 1, "b": 2, "c": 3}


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.parametrize(
    "dictionary, key, expected",
    [
        (None, "null", np.nan),
        ({"a": 1}, "null", np.nan),
        (same_values, "'non_existing_key'", np.nan),
        (same_values, "'a'", float(1 / 3)),
        (same_values, "'c'", float(1 / 3)),
        (ascending_values, "'a'", float(1 / 6)),
        (ascending_values, "'c'", float(1 / 2)),
    ],
)
@pytest.mark.asyncio
async def test_get_relative_frequency_udf(session, dictionary, key, expected):
    """
    Test get relative frequency UDF
    """
    dictionary_expr = to_object(dictionary)
    query = f"SELECT F_GET_RELATIVE_FREQUENCY({dictionary_expr}, {key}) AS OUT"
    df = await session.execute_query(query)
    np.testing.assert_allclose(df.iloc[0]["OUT"], expected, 1e-5)
