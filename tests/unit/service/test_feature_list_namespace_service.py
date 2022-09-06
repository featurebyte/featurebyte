"""
Test FeatureListNamespaceService
"""
import asyncio
import json

import pytest
from bson.objectid import ObjectId

from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceCreate,
    FeatureListNamespaceUpdate,
)
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService


@pytest.fixture(name="feature_list_namespace_service")
def feature_list_namespace_service_fixture(persistent, user):
    """FeatureListNamespaceService fixture"""
    return FeatureListNamespaceService(persistent=persistent, user=user)


@pytest.fixture(name="created_document")
def created_document_fixture(feature_list_namespace_service):
    """Inserted feature list namespace document"""
    with open(
        "tests/fixtures/request_payloads/feature_list_namespace.json", encoding="utf"
    ) as fhandle:
        payload = json.loads(fhandle.read())
    return asyncio.run(
        feature_list_namespace_service.create_document(FeatureListNamespaceCreate(**payload))
    )


@pytest.mark.asyncio
async def test_update_document(feature_list_namespace_service, created_document):
    """Test update document"""
    data = await feature_list_namespace_service.list_documents()
    assert data["total"] == 1
    feature_list_ids_before = data["data"][0]["feature_list_ids"]
    assert len(feature_list_ids_before) == 1

    # insert a feature_list_id to feature collection
    persistent = feature_list_namespace_service.persistent
    feature_list_id = await persistent.insert_one(
        collection_name="feature_list",
        document={
            "_id": ObjectId(),
            "user_id": created_document.user_id,
            "name": created_document.name,
            "readiness": FeatureReadiness.PRODUCTION_READY.value,
            "readiness_distribution": [{"readiness": "PRODUCTION_READY", "count": 2}],
        },
    )

    # update feature namespace by using the newly inserted feature_id
    updated_doc = await feature_list_namespace_service.update_document(
        document_id=created_document.id,
        data=FeatureListNamespaceUpdate(feature_list_id=feature_list_id),
    )
    assert updated_doc.user_id == created_document.user_id
    assert updated_doc.feature_list_ids == [feature_list_ids_before[0], feature_list_id]
    assert updated_doc.readiness == FeatureReadiness.PRODUCTION_READY
    assert updated_doc.default_feature_list_id == feature_list_id
    assert updated_doc.default_version_mode == DefaultVersionMode.AUTO

    # update another feature_id with lower readiness level
    worse_readiness_feature_list_id = await persistent.insert_one(
        collection_name="feature_list",
        document={
            "_id": ObjectId(),
            "user_id": created_document.user_id,
            "name": created_document.name,
            "readiness": FeatureReadiness.PRODUCTION_READY.value,
            "readiness_distribution": [{"readiness": "DEPRECATED", "count": 2}],
        },
    )

    updated_doc = await feature_list_namespace_service.update_document(
        document_id=created_document.id,
        data=FeatureListNamespaceUpdate(feature_list_id=worse_readiness_feature_list_id),
    )
    assert updated_doc.user_id == created_document.user_id
    assert updated_doc.feature_list_ids == [
        feature_list_ids_before[0],
        feature_list_id,
        worse_readiness_feature_list_id,
    ]
    assert updated_doc.readiness == FeatureReadiness.PRODUCTION_READY
    assert updated_doc.default_feature_list_id == feature_list_id
    assert updated_doc.default_version_mode == DefaultVersionMode.AUTO

    # test update default version mode
    updated_doc = await feature_list_namespace_service.update_document(
        document_id=created_document.id,
        data=FeatureListNamespaceUpdate(default_version_mode=DefaultVersionMode.MANUAL),
    )
    assert updated_doc.user_id == created_document.user_id
    assert updated_doc.feature_list_ids == [
        feature_list_ids_before[0],
        feature_list_id,
        worse_readiness_feature_list_id,
    ]
    assert updated_doc.readiness == FeatureReadiness.PRODUCTION_READY
    assert updated_doc.default_feature_list_id == feature_list_id
    assert updated_doc.default_version_mode == DefaultVersionMode.MANUAL
