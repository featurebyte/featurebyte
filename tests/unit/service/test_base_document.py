"""
Tests functions/methods in routes/common directory
"""
from typing import List

from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import numpy as np
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.exception import (
    CatalogNotSpecifiedError,
    DocumentConflictError,
    DocumentModificationBlockedError,
    DocumentNotFoundError,
)
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    ReferenceInfo,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.persistent import AuditActionType
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.entity import EntityServiceUpdate
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


class NonAuditableDocument(FeatureByteBaseDocumentModel):
    """NonAuditableDocument class"""

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """Settings class"""

        collection_name = "non_auditable_col"
        unique_constraints: List[UniqueValuesConstraint] = []
        auditable = False


class NonAuditableDocumentService(BaseDocumentService):
    """NonAuditableDocumentService class for testing"""

    # pylint: disable=abstract-method

    document_class = NonAuditableDocument


@pytest.fixture(name="document_service")
def document_service_fixture(user, persistent):
    """Fixture for DocumentService"""
    return DocumentService(
        user=user,
        persistent=persistent,
        catalog_id=None,
        block_modification_handler=BlockModificationHandler(),
    )


@pytest.fixture(name="non_auditable_document_service")
def non_auditable_document_service_fixture(user, persistent):
    """Fixture for NonAuditableDocumentService"""
    return NonAuditableDocumentService(
        user=user,
        persistent=persistent,
        catalog_id=None,
        block_modification_handler=BlockModificationHandler(),
    )


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
            user=Mock(),
            persistent=persistent,
            catalog_id=None,
            block_modification_handler=BlockModificationHandler(),
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
            {
                "$or": [
                    {"$text": {"$search": "some_value"}},
                    {"name": {"$regex": "some_value", "$options": "i"}},
                ],
                "catalog_id": "catalog_id",
            },
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
                "$or": [
                    {"$text": {"$search": "some_value"}},
                    {"name": {"$regex": "some_value", "$options": "i"}},
                ],
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


@pytest_asyncio.fixture(name="document_with_block_modification")
async def document_with_block_modification_fixture(document_service):
    """Create a document with block_modification_by"""
    # create document
    document = await document_service.create_document(data=Document())

    # add block_by_modification
    reference_info = ReferenceInfo(asset_name="Asset", document_id=ObjectId())
    await document_service.add_block_modification_by(
        query_filter={"_id": document.id},
        reference_info=reference_info,
    )

    # retrieve document & check
    document = await document_service.get_document(document_id=document.id)
    assert document.block_modification_by == [reference_info]
    return document


@pytest.mark.asyncio
@pytest.mark.parametrize("remove_block_modification_attribute", [True, False])
async def test_add_block_modification_by(
    document_service, remove_block_modification_attribute, document_with_block_modification
):
    """Test update block by modification"""
    document = document_with_block_modification
    ref_info = document.block_modification_by[0]
    if remove_block_modification_attribute:
        # remove block_by_modification from document (check backward compatibility)
        await document_service.persistent.update_one(
            collection_name=document.collection_name(),
            query_filter={"_id": document.id},
            update={"$unset": {"block_modification_by": ""}},
            user_id=ObjectId(),
        )
        # check persistent document
        record = await document_service.persistent.find_one(
            collection_name=document.collection_name(), query_filter={"_id": document.id}
        )
        assert "block_modification_by" not in record

    # add the same reference info again
    await document_service.add_block_modification_by(
        query_filter={"_id": document.id}, reference_info=ref_info
    )

    # make sure it's not duplicated
    document = await document_service.get_document(document_id=document.id)
    assert document.block_modification_by == [ref_info]


@pytest.mark.asyncio
async def test_remove_block_modification_by(document_service, document_with_block_modification):
    """Test remove block by modification"""
    document = document_with_block_modification
    ref_info = document.block_modification_by[0]

    # remove block by modification
    await document_service.remove_block_modification_by(
        query_filter={"_id": document.id}, reference_info=ref_info
    )

    # make sure it's removed
    document = await document_service.get_document(document_id=document.id)
    assert document.block_modification_by == []


@pytest.mark.asyncio
async def test_document_not_modifiable_if_block_modification_by_not_empty(
    document_service, document_with_block_modification
):
    """Test document not modifiable if block_modification_by not empty"""
    document = document_with_block_modification
    ref_info = document.block_modification_by[0]

    # try to update document - expect an error
    expected_error = f"Document {document.id} is blocked from modification by ['Asset(id: {ref_info.document_id})']"
    with pytest.raises(DocumentModificationBlockedError) as exc:
        await document_service.update_document(
            document_id=document.id, data=Document(name="new_name")
        )
    assert expected_error in str(exc.value)

    # try to update document description - should be no error
    await document_service.update_document_description(
        document_id=document.id, description="new_description"
    )
    document = await document_service.get_document(document_id=document.id)
    assert document.description == "new_description"

    # try to delete document - expect an error
    with pytest.raises(DocumentModificationBlockedError) as exc:
        await document_service.delete_document(document_id=document.id)
    assert expected_error in str(exc.value)

    # add another block by modification
    another_ref_info = ReferenceInfo(asset_name="Asset", document_id=ObjectId())
    await document_service.add_block_modification_by(
        query_filter={"_id": document.id}, reference_info=another_ref_info
    )
    document = await document_service.get_document(document_id=document.id)
    assert document.block_modification_by == [ref_info, another_ref_info]

    # remove block by modification
    await document_service.remove_block_modification_by(
        query_filter={"_id": document.id}, reference_info=ref_info
    )

    # try to update document - expect an error
    expected_error = f"Document {document.id} is blocked from modification by ['Asset(id: {another_ref_info.document_id})']"
    with pytest.raises(DocumentModificationBlockedError) as exc:
        await document_service.update_document(
            document_id=document.id, data=Document(name="new_name")
        )
    assert expected_error in str(exc.value)

    # try to delete document - expect an error
    with pytest.raises(DocumentModificationBlockedError) as exc:
        await document_service.delete_document(document_id=document.id)
    assert expected_error in str(exc.value)

    # remove block by modification
    await document_service.remove_block_modification_by(
        query_filter={"_id": document.id}, reference_info=another_ref_info
    )
    document = await document_service.get_document(document_id=document.id)
    assert document.block_modification_by == []

    # try to update document - expect no error
    document = await document_service.update_document(
        document_id=document.id, data=Document(name="new_name")
    )
    assert document.name == "new_name"

    # try to delete document - expect no error
    await document_service.delete_document(document_id=document.id)
    with pytest.raises(DocumentNotFoundError):
        await document_service.get_document(document_id=document.id)


@pytest.mark.asyncio
async def test_document_disable_block_modification_check(
    document_service, document_with_block_modification
):
    """Test document not modifiable if block_modification_by not empty"""
    document = document_with_block_modification
    with pytest.raises(DocumentModificationBlockedError) as exc:
        await document_service.update_document(
            document_id=document.id, data=Document(name="new_name")
        )
    expected_error = f"Document {document.id} is blocked from modification by "
    assert expected_error in str(exc.value)

    # try to update document - expect no error
    with document_service.block_modification_handler.disable_block_modification_check():
        document = await document_service.update_document(
            document_id=document.id, data=Document(name="new_name")
        )
        assert document.name == "new_name"


def test_catalog_specific_service_requires_catalog_id(user, persistent):
    """
    Test catalog specific service initialization without catalog_id
    """
    with patch(
        "featurebyte.service.base_document.BaseDocumentService.is_catalog_specific"
    ) as mock_is_catalog_specific:
        with pytest.raises(CatalogNotSpecifiedError) as exc:
            mock_is_catalog_specific.return_value = True
            DocumentService(
                user=user,
                persistent=persistent,
                catalog_id=None,
                block_modification_handler=BlockModificationHandler(),
            )
        assert str(exc.value) == "Catalog not specified. Please specify a catalog."


@pytest.mark.asyncio
async def test_non_auditable_document_service(non_auditable_document_service):
    """Test modifying non-auditable documents"""
    document = NonAuditableDocument()
    service = non_auditable_document_service

    await service.create_document(data=document)
    await service.update_document(
        document_id=document.id, data=NonAuditableDocument(name="new_name")
    )
    await service.delete_document(document_id=document.id)

    audits = await service.list_document_audits(document_id=document.id)
    assert audits["total"] == 0


@pytest.mark.asyncio
async def test_app_container__disable_block_modification_check(app_container, entity):
    """
    Test app_container's disable_block_modification_check
    """
    # add block modification by to entity object
    await app_container.entity_service.add_block_modification_by(
        query_filter={"_id": entity.id},
        reference_info=ReferenceInfo(asset_name="Asset", document_id=ObjectId()),
    )

    # check that entity is blocked from modification
    with pytest.raises(DocumentModificationBlockedError):
        await app_container.entity_service.update_document(
            document_id=entity.id, data=EntityServiceUpdate(name="new_name")
        )

    # case 1: use context manager without using the yielded app_container
    with app_container.block_modification_handler.disable_block_modification_check():
        service = app_container.entity_service
        updated_entity = await app_container.entity_service.update_document(
            document_id=entity.id, data=EntityServiceUpdate(name="new_name")
        )
        assert updated_entity.name == "new_name"

        # inside the context manager, check that block modification check is disabled
        assert service.block_modification_handler.block_modification is False

    # outside the context manager, check that block modification check is enabled
    assert service.block_modification_handler.block_modification is True

    # case 2: use context manager with using the yielded app_container
    with app_container.block_modification_handler.disable_block_modification_check():
        updated_entity = await app_container.entity_service.update_document(
            document_id=entity.id, data=EntityServiceUpdate(name="another_name")
        )
        assert updated_entity.name == "another_name"

        # inside the context manager, check that block modification check is disabled
        assert service.block_modification_handler.block_modification is False

    # outside the context manager, check that block modification check is enabled
    assert service.block_modification_handler.block_modification is True
