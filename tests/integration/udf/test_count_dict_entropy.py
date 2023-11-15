"""
Tests for count dict entropy UDF
"""
import numpy as np
import pytest

from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS


@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS, indirect=True)
@pytest.mark.parametrize(
    "dictionary, expected",
    [
        (None, np.nan),
        ({"a": 1}, 0),
        ({"a": 1, "b": 1, "c": 1}, 1.098612),
        ({"a": 1, "b": 2, "c": 3}, 1.011404),
    ],
)
@pytest.mark.asyncio
async def test_count_dict_entropy_udf(session, to_object, dictionary, expected):
    """
    Test count dict entropy UDF
    """
    dictionary_expr = to_object(dictionary)
    query = f"SELECT F_COUNT_DICT_ENTROPY({dictionary_expr}) AS OUT"
    df = await session.execute_query(query)
    actual = df.iloc[0]["OUT"]
    if actual is None:
        actual = np.nan
    np.testing.assert_allclose(actual, expected, 1e-5)
