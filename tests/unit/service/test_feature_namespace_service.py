"""
Test FeatureNamespaceService
"""
import asyncio
import json

import pytest
from bson.objectid import ObjectId

from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from featurebyte.schema.feature_namespace import FeatureNamespaceCreate, FeatureNamespaceUpdate
from featurebyte.service.feature_namespace import FeatureNamespaceService


@pytest.fixture(name="feature_namespace_service")
def feature_namespace_service_fixture(persistent, user):
    """FeatureNamespaceService fixture"""
    return FeatureNamespaceService(persistent=persistent, user=user)


@pytest.fixture(name="created_document")
def created_document_fixture(feature_namespace_service):
    """Inserted feature namespace document"""
    with open("tests/fixtures/request_payloads/feature_namespace.json", encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
    return asyncio.run(feature_namespace_service.create_document(FeatureNamespaceCreate(**payload)))


@pytest.mark.asyncio
async def test_update_document(feature_namespace_service, created_document):
    """Test update document"""
    data = await feature_namespace_service.list_documents()
    assert data["total"] == 1
    feature_ids_before = data["data"][0]["feature_ids"]
    assert len(feature_ids_before) == 1

    # insert a feature_id to feature collection
    persistent = feature_namespace_service.persistent
    feature_id = await persistent.insert_one(
        collection_name="feature",
        document={
            "_id": ObjectId(),
            "user_id": created_document.user_id,
            "name": created_document.name,
            "dtype": "FLOAT",
            "entity_ids": [ObjectId(eid) for eid in created_document.entity_ids],
            "event_data_ids": [ObjectId(eid) for eid in created_document.event_data_ids],
            "readiness": FeatureReadiness.DRAFT.value,
        },
    )

    # update feature namespace by using the newly inserted feature_id
    updated_doc = await feature_namespace_service.update_document(
        document_id=created_document.id, data=FeatureNamespaceUpdate(feature_id=feature_id)
    )
    assert updated_doc.user_id == created_document.user_id
    assert updated_doc.feature_ids == [feature_ids_before[0], feature_id]
    assert updated_doc.readiness == FeatureReadiness.DRAFT
    assert updated_doc.default_feature_id == feature_id
    assert updated_doc.default_version_mode == DefaultVersionMode.AUTO

    # update another feature_id with lower readiness level
    worse_readiness_feature_id = await persistent.insert_one(
        collection_name="feature",
        document={
            "_id": ObjectId(),
            "user_id": ObjectId(created_document.user_id),
            "name": created_document.name,
            "dtype": "FLOAT",
            "entity_ids": [ObjectId(eid) for eid in created_document.entity_ids],
            "event_data_ids": [ObjectId(eid) for eid in created_document.event_data_ids],
            "readiness": FeatureReadiness.DEPRECATED.value,
        },
    )

    updated_doc = await feature_namespace_service.update_document(
        document_id=created_document.id,
        data=FeatureNamespaceUpdate(feature_id=worse_readiness_feature_id),
    )
    assert updated_doc.user_id == created_document.user_id
    assert updated_doc.feature_ids == [
        feature_ids_before[0],
        feature_id,
        worse_readiness_feature_id,
    ]
    assert updated_doc.readiness == FeatureReadiness.DRAFT
    assert updated_doc.default_feature_id == feature_id
    assert updated_doc.default_version_mode == DefaultVersionMode.AUTO

    # test update default version mode
    updated_doc = await feature_namespace_service.update_document(
        document_id=created_document.id,
        data=FeatureNamespaceUpdate(default_version_mode=DefaultVersionMode.MANUAL),
    )
    assert updated_doc.user_id == created_document.user_id
    assert updated_doc.feature_ids == [
        feature_ids_before[0],
        feature_id,
        worse_readiness_feature_id,
    ]
    assert updated_doc.readiness == FeatureReadiness.DRAFT
    assert updated_doc.default_feature_id == feature_id
    assert updated_doc.default_version_mode == DefaultVersionMode.MANUAL
