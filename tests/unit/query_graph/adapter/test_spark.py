import pytest

from featurebyte.query_graph.sql.adapter import SparkAdapter


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
