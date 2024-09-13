"""
Tests for F_GET_VALUE UDF
"""

import json

import numpy as np
import pytest
from sqlglot import parse_one

from featurebyte.enum import SourceType
from tests.integration.udf.util import execute_query_with_udf


@pytest.mark.parametrize("source_type", ["bigquery"], indirect=True)
@pytest.mark.parametrize(
    "dictionary, key, expected",
    [
        (None, "null", np.nan),
        ({"a": 1}, "null", np.nan),
        ({"a": 1}, "'a'", 1),
        ({"a": 1}, "'b'", np.nan),
    ],
)
@pytest.mark.asyncio
async def test_get_value_udf(session, to_object, dictionary, key, expected):
    """
    Test get_value UDF
    """
    dictionary_expr = to_object(dictionary)
    actual = await execute_query_with_udf(
        session,
        "F_GET_VALUE",
        [
            dictionary_expr,
            parse_one(key),
        ],
    )
    if session.source_type == SourceType.BIGQUERY:
        # Result is JSON encoded in case of BigQuery
        actual = json.loads(actual)
    if actual is None:
        actual = np.nan
    np.testing.assert_allclose(actual, expected, 1e-5)
