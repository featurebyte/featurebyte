"""
Unit Test Cases for LocalSourceDBManager
"""
from unittest import mock

import pytest

from featurebyte.access import LocalSourceDBManager


@pytest.fixture(name="test_localdb")
def localdb():
    """
    Pytest Fixture for LocalSourceDBManager instance.

    Returns:
        f_localdb: Fixture for LocalSourceDBManager instance.
    """
    f_localdb = LocalSourceDBManager()
    return f_localdb


@mock.patch("featurebyte.access.LocalSourceDBManager._tinydb_op")
def test_search_sources(tinydb_op, test_localdb):
    """
    Test Case for search_sources
    """
    tinydb_op.return_value = [{"name": "fb", "author": "author2", "status": "DRAFT"}]
    docs = test_localdb.search_sources(exact_match=False, name="fb", author="author2")
    query_str = (
        "(query.name.matches('fb', flags=re.IGNORECASE))&"
        "(query.author.matches('author2', flags=re.IGNORECASE))"
    )
    tinydb_op.assert_called_with("query", None, query_str=query_str)
    assert len(docs) == 1
    assert docs[0]["name"] == "fb"
    assert docs[0]["author"] == "author2"
    assert docs[0]["status"] == "DRAFT"


@pytest.mark.xfail(raises=AttributeError)
def test_search_sources_with_empty_query(test_localdb):
    """
    Test Case for search_sources with empty query params
    """
    test_localdb.search_sources(exact_match=False)


@pytest.mark.xfail(raises=ValueError)
def test_search_sources_with_unsupported_query(test_localdb):
    """
    Test Case for search_sources with unsupported query params
    """
    test_localdb.search_sources(exact_match=False, a=1)


@mock.patch("featurebyte.access.LocalSourceDBManager.search_sources")
@mock.patch("featurebyte.access.LocalSourceDBManager._tinydb_op")
def test_insert_sources(mock_tinydb_op, mock_search_sources, test_localdb):
    """
    Test Case for insert source
    """
    mock_search_sources.return_value = []
    doc = {"name": "name1"}
    test_localdb.insert_source(doc)
    mock_tinydb_op.assert_called_with("insert", doc)


@mock.patch("featurebyte.access.LocalSourceDBManager.search_sources")
@mock.patch("featurebyte.access.LocalSourceDBManager._tinydb_op")
def test_update_sources(mock_tinydb_op, mock_search_sources, test_localdb):
    """
    Test Case for update source
    """
    mock_search_sources.return_value = [{"name": "name1"}]
    doc = {"name": "name1"}
    test_localdb.insert_source(doc)
    mock_tinydb_op.assert_called_with("update", doc)
