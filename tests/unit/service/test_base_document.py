"""
Tests functions/methods in routes/common directory
"""
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import numpy as np
import pytest

from featurebyte.exception import DocumentConflictError
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    UniqueConstraintResolutionSignature,
)
from featurebyte.models.persistent import AuditActionType
from featurebyte.routes.common.base import BaseDocumentService


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_filter, conflict_signature, resolution_signature, expected_msg",
    [
        (
            {"_id": "id_val"},
            {"id": "id_val"},
            UniqueConstraintResolutionSignature.GET_BY_ID,
            (
                'Col (id: "id_val") already exists. '
                'Get the existing object by `Col.get_by_id(id="conflict_id_val")`.'
            ),
        ),
        (
            {"_id": "id_val"},
            {"id": "id_val", "name": "name_val"},
            UniqueConstraintResolutionSignature.GET_BY_ID,
            (
                'Col (id: "id_val", name: "name_val") already exists. '
                'Get the existing object by `Col.get_by_id(id="conflict_id_val")`.'
            ),
        ),
        (
            {"_id": "id_val"},
            {"id": "id_val"},
            UniqueConstraintResolutionSignature.GET_NAME,
            (
                'Col (id: "id_val") already exists. '
                'Get the existing object by `Col.get(name="conflict_name_val")`.'
            ),
        ),
    ],
)
async def test_check_document_creation_conflict(
    query_filter, conflict_signature, resolution_signature, expected_msg
):
    """
    Test check_document_creation_conflict error message
    """

    class Document(FeatureByteBaseDocumentModel):
        class Settings(FeatureByteBaseDocumentModel.Settings):
            collection_name = "col"

    class DocumentService(BaseDocumentService):
        document_class = Document

    persistent = AsyncMock()
    persistent.find_one.return_value = {"_id": "conflict_id_val", "name": "conflict_name_val"}
    with pytest.raises(DocumentConflictError) as exc:
        await DocumentService(user=Mock(), persistent=persistent)._check_document_unique_constraint(
            query_filter=query_filter,
            conflict_signature=conflict_signature,
            resolution_signature=resolution_signature,
        )
    assert expected_msg in str(exc.value)


INSERTION_WITH_FIELD_AUDIT_LOG = {
    "action_type": AuditActionType.INSERT,
    "previous_values": {},
    "current_values": {"field": "init_value", "created_at": datetime(2022, 2, 1)},
}
INSERTION_WITH_OTHER_FIELD_AUDIT_LOG = {
    "action_type": AuditActionType.INSERT,
    "previous_values": {},
    "current_values": {"other_field": "other_value", "created_at": datetime(2022, 2, 1)},
}


@pytest.mark.parametrize(
    "audit_docs,expected",
    [
        ([], []),  # no audit_doc record
        (
            # one insertion record only
            [INSERTION_WITH_FIELD_AUDIT_LOG],
            [{"created_at": datetime(2022, 2, 1), "value": "init_value"}],
        ),
        (
            # one unrelated insertion, one related update after that
            [
                INSERTION_WITH_OTHER_FIELD_AUDIT_LOG,
                {
                    "action_type": AuditActionType.UPDATE,
                    "previous_values": {"updated_at": None},
                    "current_values": {
                        "field": "updated_value",
                        "updated_at": datetime(2022, 2, 2),
                    },
                },
            ],
            [
                {"created_at": datetime(2022, 2, 2), "value": "updated_value"},
                {"created_at": datetime(2022, 2, 1), "value": np.nan},
            ],
        ),
        (
            # one unrelated insertion, one non-related update after that, following one related update
            [
                INSERTION_WITH_OTHER_FIELD_AUDIT_LOG,
                {
                    "action_type": AuditActionType.UPDATE,
                    "previous_values": {
                        "other_field": "other_value",
                        "updated_at": datetime(2022, 2, 1),
                    },
                    "current_values": {
                        "other_field": "other_updated_value",
                        "updated_at": datetime(2022, 2, 2),
                    },
                },
                {
                    "action_type": AuditActionType.UPDATE,
                    "previous_values": {"updated_at": datetime(2022, 2, 3)},
                    "current_values": {
                        "field": "updated_value",
                        "updated_at": datetime(2022, 2, 3),
                    },
                },
            ],
            [
                {"created_at": datetime(2022, 2, 3), "value": "updated_value"},
                {"created_at": datetime(2022, 2, 1), "value": np.nan},
            ],
        ),
        (
            # one related insertion, two non-related update after that, following one related update
            [
                INSERTION_WITH_FIELD_AUDIT_LOG,
                {
                    "action_type": AuditActionType.UPDATE,
                    "previous_values": {"updated_at": datetime(2022, 2, 1)},
                    "current_values": {
                        "other_field": "other_value",
                        "updated_at": datetime(2022, 2, 2),
                    },
                },
                {
                    "action_type": AuditActionType.UPDATE,
                    "previous_values": {
                        "other_field": "other_value",
                        "updated_at": datetime(2022, 2, 2),
                    },
                    "current_values": {
                        "other_field": "other_updated_value",
                        "updated_at": datetime(2022, 2, 3),
                    },
                },
                {
                    "action_type": AuditActionType.UPDATE,
                    "previous_values": {"field": "init_value", "updated_at": datetime(2022, 2, 3)},
                    "current_values": {
                        "field": "updated_value",
                        "updated_at": datetime(2022, 2, 4),
                    },
                },
            ],
            [
                {"created_at": datetime(2022, 2, 4), "value": "updated_value"},
                {"created_at": datetime(2022, 2, 1), "value": "init_value"},
            ],
        ),
    ],
)
def test_get_field_history__new_field_introduction(audit_docs, expected):
    """Test a new field get introduced or updated"""
    output = BaseDocumentService._get_field_history(field="field", audit_docs=audit_docs)
    assert output == expected


@pytest.mark.parametrize(
    "audit_docs,expected",
    [
        (
            # one unrelated insertion, remove that field after that
            [
                INSERTION_WITH_FIELD_AUDIT_LOG,
                {
                    "action_type": AuditActionType.UPDATE,
                    "previous_values": {"field": "init_value", "updated_at": datetime(2022, 2, 1)},
                    "current_values": {"updated_at": datetime(2022, 2, 2)},
                },
            ],
            [
                {"created_at": datetime(2022, 2, 2), "value": np.nan},
                {"created_at": datetime(2022, 2, 1), "value": "init_value"},
            ],
        ),
        (
            # one related insertion, one unrelated updated after that, then remove that field
            [
                INSERTION_WITH_FIELD_AUDIT_LOG,
                {
                    "action_type": AuditActionType.UPDATE,
                    "previous_values": {"updated_at": datetime(2022, 2, 1)},
                    "current_values": {
                        "other_field": "other_value",
                        "updated_at": datetime(2022, 2, 2),
                    },
                },
                {
                    "action_type": AuditActionType.UPDATE,
                    "previous_values": {"field": "init_value", "updated_at": datetime(2022, 2, 2)},
                    "current_values": {"updated_at": datetime(2022, 2, 3)},
                },
            ],
            [
                {"created_at": datetime(2022, 2, 3), "value": np.nan},
                {"created_at": datetime(2022, 2, 1), "value": "init_value"},
            ],
        ),
        (
            # one unrelated insertion, one related update after that, then remove that field
            [
                INSERTION_WITH_OTHER_FIELD_AUDIT_LOG,
                {
                    "action_type": AuditActionType.UPDATE,
                    "previous_values": {"updated_at": None},
                    "current_values": {"updated_at": datetime(2022, 2, 2), "field": "value"},
                },
                {
                    "action_type": AuditActionType.UPDATE,
                    "previous_values": {"updated_at": datetime(2022, 2, 2), "field": "value"},
                    "current_values": {"updated_at": datetime(2022, 2, 3)},
                },
            ],
            [
                {"created_at": datetime(2022, 2, 3), "value": np.nan},
                {"created_at": datetime(2022, 2, 2), "value": "value"},
                {"created_at": datetime(2022, 2, 1), "value": np.nan},
            ],
        ),
    ],
)
def test_get_filed_history__existing_field_removal(audit_docs, expected):
    """Test an existing field get removed or updated"""
    output = BaseDocumentService._get_field_history(field="field", audit_docs=audit_docs)
    assert output == expected


@pytest.mark.parametrize(
    "doc, field_path, expected",
    [
        ({"a": [1]}, [], {"a": [1]}),
        ({"a": [1]}, ["a"], [1]),
        ({"a": [1]}, ["a", 0], 1),
        ({"a": {"b": {"c": {"d": [123]}}}}, ["a", "b", "c", "d", 0], 123),
        ({"a": [{"b": {"c": [{"d": [123]}]}}]}, ["a", 0, "b", "c", 0, "d", 0], 123),
    ],
)
def test_field_path_value(doc, field_path, expected):
    """Test field_path_value logic"""
    assert (
        BaseDocumentService._get_field_path_value(doc_dict=doc, field_path=field_path) == expected
    )
