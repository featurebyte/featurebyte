"""
Tests for functions in featurebyte/persistent/audit.py
"""
import pytest

from featurebyte.models.persistent import AuditActionType
from featurebyte.persistent.audit import get_audit_doc_name, get_previous_and_current_values


@pytest.fixture(name="original_doc")
def original_doc_fixture():
    """Original document before update operation"""
    return {
        "field_a": None,
        "field_b": None,
        "field_c": "value_c",
        "field_d": "value_d",
        "field_e": "value_e",
        "field_g": None,
    }


@pytest.fixture(name="updated_doc")
def updated_doc_fixture():
    """Updated document before update operation"""
    return {
        "field_a": None,
        "field_b": 1,
        "field_c": "value_c",
        "field_d": "value_d1",
        "field_f": "value_f",
        "field_h": None,
    }


def test_get_previous_and_current_values__original_empty(updated_doc):
    """
    Test get_previous_and_current_values function (original is empty)
    """
    previous_values, current_values = get_previous_and_current_values(
        original_doc={}, updated_doc=updated_doc
    )
    assert previous_values == {}
    assert current_values == updated_doc


def test_get_previous_and_current_values__updated_empty(original_doc):
    """
    Test get_previous_and_current_values function (original is empty)
    """
    previous_values, current_values = get_previous_and_current_values(
        original_doc=original_doc, updated_doc={}
    )
    assert previous_values == original_doc
    assert current_values == {}


def test_get_previous_and_current_values(original_doc, updated_doc):
    """
    Test get_previous_and_current_values function
    """
    previous_values, current_values = get_previous_and_current_values(
        original_doc=original_doc, updated_doc=updated_doc
    )
    assert previous_values == {
        "field_b": None,
        "field_d": "value_d",
        "field_e": "value_e",
        "field_g": None,
    }
    assert current_values == {
        "field_b": 1,
        "field_d": "value_d1",
        "field_f": "value_f",
        "field_h": None,
    }

    # apply previous_values, current_values & updated_doc to reconstruct original doc
    recons_original_doc = {
        key: previous_values.get(key, original_doc.get(key))
        for key in set(previous_values).union(set(original_doc).difference(current_values))
    }
    assert recons_original_doc == original_doc

    # apply previous_values, current_values & original_doc to reconstruct updated doc
    recons_updated_doc = {
        key: current_values.get(key, original_doc.get(key))
        for key in set(current_values).union(set(updated_doc).difference(previous_values))
    }
    assert recons_updated_doc == updated_doc


@pytest.mark.parametrize(
    "action_type, original_doc, updated_doc, expected",
    [
        (AuditActionType.INSERT, {}, {"name": "insert_doc_name"}, 'insert: "insert_doc_name"'),
        (
            AuditActionType.UPDATE,
            {"name": "origin_name"},
            {"name": "updated_name"},
            'update: "origin_name"',
        ),
        (AuditActionType.UPDATE, {}, {"name": "updated_name"}, "update: None"),
        (AuditActionType.UPDATE, {"name": ""}, {}, 'update: ""'),
        (AuditActionType.DELETE, {"name": "origin_name"}, {}, 'delete: "origin_name"'),
        (AuditActionType.REPLACE, {"name": "origin_name"}, {}, 'replace: "origin_name"'),
    ],
)
def test_get_audit_doc_name(action_type, original_doc, updated_doc, expected):
    """
    Test get_audit_doc_name function
    """
    assert get_audit_doc_name(action_type, original_doc, updated_doc) == expected
