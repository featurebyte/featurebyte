"""
Test spark adapter module
"""

import pytest
from sqlglot import expressions

from featurebyte import SourceType
from featurebyte.query_graph.sql.adapter import SparkAdapter
from tests.unit.query_graph.sql.adapter.base_adapter_test import BaseAdapterTest
from tests.util.helper import get_sql_adapter_from_source_type


class TestSparkAdapter(BaseAdapterTest):
    """
    Test spark adapter class
    """

    adapter = get_sql_adapter_from_source_type(SourceType.SPARK)
    expected_physical_type_from_dtype_mapping = {
        "BOOL": "BOOLEAN",
        "CHAR": "STRING",
        "DATE": "DATE",
        "FLOAT": "DOUBLE",
        "INT": "DOUBLE",
        "TIME": "STRING",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMP_TZ": "STRING",
        "VARCHAR": "STRING",
        "ARRAY": "ARRAY<STRING>",
        "DICT": "STRING",
        "TIMEDELTA": "STRING",
        "EMBEDDING": "ARRAY<DOUBLE>",
        "FLAT_DICT": "STRING",
        "OBJECT": "MAP<STRING, DOUBLE>",
        "TIMESTAMP_TZ_TUPLE": "STRING",
        "UNKNOWN": "STRING",
        "BINARY": "STRING",
        "VOID": "STRING",
        "MAP": "STRING",
        "STRUCT": "STRING",
    }

    def test_dateadd_seconds(self):
        """
        Test dateadd_second method for spark adapter
        """
        out = self.adapter.dateadd_second(
            expressions.Literal.number(100), expressions.Identifier(this="my_timestamp")
        )
        expected = "CAST(CAST(CAST(my_timestamp AS TIMESTAMP) AS DOUBLE) + 100 AS TIMESTAMP)"
        assert out.sql() == expected

    def test_datediff_microseond(self):
        """
        Test datediff_microsecond method for spark adapter
        """
        out = self.adapter.datediff_microsecond(
            expressions.Identifier(this="t1"),
            expressions.Identifier(this="t2"),
        )
        expected = "(CAST(CAST(t2 AS TIMESTAMP) AS DOUBLE) * 1000000.0 - CAST(CAST(t1 AS TIMESTAMP) AS DOUBLE) * 1000000.0)"
        assert out.sql() == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT abc as A", "SELECT abc as A"),
        ("SELECT 'abc' as A", "SELECT \\'abc\\' as A"),
        ("SELECT \\'abc\\' as A", "SELECT \\'abc\\' as A"),
    ],
)
def test_escape_quote_char__spark(query, expected):
    """
    Test escape_quote_char for SparkAdapter
    """
    assert SparkAdapter.escape_quote_char(query) == expected
