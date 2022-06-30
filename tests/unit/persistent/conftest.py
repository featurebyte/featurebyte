"""
Test fixtures for persistent tests
"""
from __future__ import annotations

from typing import Any, Dict, List

import pytest
from bson import ObjectId


@pytest.fixture(name="test_document")
def test_document_fixture() -> Dict[str, Any]:
    """
    Test document to be used for testing

    Returns
    -------
    Dict[str, Any]
        Document for testing
    """
    return {
        "id": ObjectId(),
        "name": "Generic Document",
        "value": [
            {
                "key1": "value1",
                "key2": "value2",
            }
        ],
    }


@pytest.fixture(name="test_documents")
def test_documents_fixture(test_document) -> List[Dict[str, Any]]:
    """
    Test documents to be used for testing

    Returns
    -------
    List[Dict[str, Any]]
        Document for testing
    """
    return [{**test_document, **{"id": ObjectId(), "name": f"Object {i}"}} for i in range(3)]
