"""
Tests for count dict num unique UDF
"""

import numpy as np
import pytest
from sqlglot import expressions

from featurebyte.query_graph.sql.common import get_fully_qualified_function_call, sql_to_string


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
    udf_expr = get_fully_qualified_function_call(
        session.database_name, session.schema_name, "F_COUNT_DICT_NUM_UNIQUE", [dictionary_expr]
    )
    query = sql_to_string(
        expressions.select(expressions.alias_(udf_expr, alias="OUT", quoted=False)),
        session.source_type,
    )
    df = await session.execute_query(query)
    actual = df.iloc[0]["OUT"]
    if actual is None:
        actual = np.nan
    np.testing.assert_allclose(actual, expected, 1e-5)
