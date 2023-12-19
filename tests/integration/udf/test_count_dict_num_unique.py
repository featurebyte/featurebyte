"""
Tests for count dict num unique UDF
"""
import numpy as np
import pytest

from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_AND_UNITY


@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_AND_UNITY, indirect=True)
@pytest.mark.parametrize(
    "dictionary, expected",
    [
        (None, 0),
        ({}, 0),
        ({"a": 1}, 1),
        ({"a": 1, "b": 1, "c": 1}, 3),
    ],
)
@pytest.mark.asyncio
async def test_count_dict_num_unique_udf(session, to_object, dictionary, expected):
    """
    Test count dict num unique UDF
    """
    dictionary_expr = to_object(dictionary)
    query = f"SELECT F_COUNT_DICT_NUM_UNIQUE({dictionary_expr}) AS OUT"
    df = await session.execute_query(query)
    actual = df.iloc[0]["OUT"]
    if actual is None:
        actual = np.nan
    np.testing.assert_allclose(actual, expected, 1e-5)
