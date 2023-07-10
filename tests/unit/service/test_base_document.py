"""
Tests functions/methods in routes/common directory
"""
from typing import List

from datetime import datetime
from unittest.mock import AsyncMock, Mock

import numpy as np
import pytest

from featurebyte.exception import DocumentConflictError, DocumentNotFoundError
from featurebyte.models.base import (
    DEFAULT_CATALOG_ID,
    FeatureByteBaseDocumentModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.persistent import AuditActionType
from featurebyte.service.base_document import BaseDocumentService


class Document(FeatureByteBaseDocumentModel):
    """Document class"""

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """Settings class"""

        collection_name = "col"
        unique_constraints: List[UniqueValuesConstraint] = []


class DocumentService(BaseDocumentService):
    """DocumentService class for testing"""

    # pylint: disable=abstract-method

    document_class = Document


@pytest.fixture(name="document_service")
def document_service_fixture(user, persistent):
    """Fixture for DocumentService"""
    return DocumentService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)


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

    persistent = AsyncMock()
    persistent.find_one.return_value = {"_id": "conflict_id_val", "name": "conflict_name_val"}
    with pytest.raises(DocumentConflictError) as exc:
        await DocumentService(
            user=Mock(), persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
        )._check_document_unique_constraint(
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
    "kwargs, expected",
    [
        ({}, {"catalog_id": "catalog_id"}),
        ({"name": "some_name"}, {"name": "some_name", "catalog_id": "catalog_id"}),
        (
            {"search": "some_value"},
            {"$text": {"$search": "some_value"}, "catalog_id": "catalog_id"},
        ),
        (
            {"query_filter": {"field": {"$in": ["a", "b"]}}},
            {"field": {"$in": ["a", "b"]}, "catalog_id": "catalog_id"},
        ),
        (
            {
                "name": "some_name",
                "search": "some_value",
                "query_filter": {"field": {"$in": ["a", "b"]}},
            },
            {
                "name": "some_name",
                "$text": {"$search": "some_value"},
                "field": {"$in": ["a", "b"]},
                "catalog_id": "catalog_id",
            },
        ),
    ],
)
def test_construct_list_query_filter(kwargs, expected):
    """Test construct_list_query_filter logic"""
    assert (
        BaseDocumentService.construct_list_query_filter(
            Mock(is_catalog_specific=True, catalog_id="catalog_id"), **kwargs
        )
        == expected
    )


@pytest.mark.asyncio
async def test_list_documents_iterator(document_service):
    """Test list document iterator"""
    total = 15
    for _ in range(total):
        await document_service.create_document(data=Document())

    list_results = await document_service.list_documents_as_dict(
        page_size=0, page=1, query_filter={}
    )
    expected_doc_ids = set(doc["_id"] for doc in list_results["data"])
    assert list_results["total"] == total

    # retrieve list iterator & check results
    for page_size in [1, 10, 15, 20]:
        doc_ids = [
            doc["_id"]
            async for doc in document_service.list_documents_as_dict_iterator(
                query_filter={}, page_size=page_size
            )
        ]
        assert set(doc_ids) == expected_doc_ids

    # check list_documents_iterator output type
    async for doc in document_service.list_documents_iterator(query_filter={}, page_size=1):
        assert isinstance(doc, Document)


@pytest.mark.asyncio
async def test_delete_document(document_service):
    """Test delete document"""
    # create document
    document = await document_service.create_document(data=Document())

    # delete document
    await document_service.delete_document(document_id=document.id)

    # try to delete document - expect an error
    with pytest.raises(DocumentNotFoundError):
        await document_service.get_document(document_id=document.id)
