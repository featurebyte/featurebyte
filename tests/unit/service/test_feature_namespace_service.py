"""
Test FeatureNamespaceService
"""
import json

import pytest
from bson.objectid import ObjectId

from featurebyte.exception import DocumentInconsistencyError
from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from featurebyte.schema.feature import FeatureCreate
from featurebyte.schema.feature_namespace import FeatureNamespaceUpdate


async def insert_feature_into_persistent(user, persistent, version_suffix, readiness, name=None):
    """Insert a feature into persistent"""
    with open("tests/fixtures/request_payloads/feature_sum_30m.json", encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        payload = FeatureCreate(**payload, user_id=user.id).dict(by_alias=True)
        payload["_id"] = ObjectId()
        payload["version"] = f'{payload["version"]}_{version_suffix}'
        payload["readiness"] = readiness
        if name:
            payload["name"] = name
        feature_id = await persistent.insert_one(
            collection_name="feature",
            document=payload,
            user_id=user.id,
        )
        return feature_id


@pytest.mark.asyncio
async def test_update_document__auto_default_version_mode(feature_namespace_service, feature, user):
    """Test update document"""
    data = await feature_namespace_service.list_documents()
    persistent = feature_namespace_service.persistent
    assert data["total"] == 1
    feature_ids_before = data["data"][0]["feature_ids"]
    assert len(feature_ids_before) == 1

    # insert a feature_id to feature collection
    feature_id = await insert_feature_into_persistent(
        user=user,
        persistent=persistent,
        version_suffix="_1",
        readiness=FeatureReadiness.DRAFT.value,
    )

    # update feature namespace by using the newly inserted feature_id
    updated_doc = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id, data=FeatureNamespaceUpdate(feature_id=feature_id)
    )
    assert updated_doc.user_id == feature.user_id
    assert updated_doc.feature_ids == sorted([feature_ids_before[0], feature_id])
    assert updated_doc.readiness == FeatureReadiness.DRAFT
    assert updated_doc.default_feature_id == feature_id
    assert updated_doc.default_version_mode == DefaultVersionMode.AUTO

    # update another feature_id with lower readiness level
    worse_readiness_feature_id = await insert_feature_into_persistent(
        user=user,
        persistent=persistent,
        version_suffix="_2",
        readiness=FeatureReadiness.DEPRECATED.value,
    )

    updated_doc = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceUpdate(feature_id=worse_readiness_feature_id),
    )
    assert updated_doc.user_id == feature.user_id
    assert updated_doc.feature_ids == sorted(
        [
            feature_ids_before[0],
            feature_id,
            worse_readiness_feature_id,
        ]
    )
    assert updated_doc.readiness == FeatureReadiness.DRAFT
    assert updated_doc.default_feature_id == feature_id
    assert updated_doc.default_version_mode == DefaultVersionMode.AUTO


@pytest.mark.asyncio
async def test_update_document__manual_default_version_mode(
    feature_namespace_service, feature, user
):
    """Test update document"""
    data = await feature_namespace_service.list_documents()
    persistent = feature_namespace_service.persistent
    assert data["total"] == 1
    feature_ids_before = data["data"][0]["feature_ids"]
    assert len(feature_ids_before) == 1

    # test update default version mode
    updated_doc = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceUpdate(default_version_mode=DefaultVersionMode.MANUAL),
    )
    assert updated_doc.user_id == feature.user_id
    assert updated_doc.feature_ids == feature_ids_before
    assert updated_doc.readiness == FeatureReadiness.DRAFT
    assert updated_doc.default_feature_id == feature_ids_before[0]
    assert updated_doc.default_version_mode == DefaultVersionMode.MANUAL

    # test insert a better readiness level feature (default feature should be the same)
    prod_ready_feature_id = await insert_feature_into_persistent(
        user=user,
        persistent=persistent,
        version_suffix="_1",
        readiness=FeatureReadiness.PRODUCTION_READY.value,
    )
    updated_doc = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceUpdate(feature_id=prod_ready_feature_id),
    )
    assert updated_doc.feature_ids == sorted(feature_ids_before + [prod_ready_feature_id])
    assert updated_doc.default_feature_id == feature_ids_before[0]
    assert updated_doc.readiness == FeatureReadiness.DRAFT

    # test update default version mode to auto
    updated_doc = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceUpdate(default_version_mode=DefaultVersionMode.AUTO),
    )
    assert updated_doc.feature_ids == sorted(feature_ids_before + [prod_ready_feature_id])
    assert updated_doc.default_feature_id == prod_ready_feature_id
    assert updated_doc.readiness == FeatureReadiness.PRODUCTION_READY


@pytest.mark.asyncio
async def test_update_document__inconsistency_error(feature_namespace_service, feature, user):
    """Test feature namespace update - document inconsistency error"""
    inconsistent_feature_id = await insert_feature_into_persistent(
        user=user,
        persistent=feature_namespace_service.persistent,
        version_suffix="_1",
        readiness=FeatureReadiness.DRAFT.value,
        name="random_name",
    )
    with pytest.raises(DocumentInconsistencyError) as exc:
        await feature_namespace_service.update_document(
            document_id=feature.feature_namespace_id,
            data=FeatureNamespaceUpdate(feature_id=inconsistent_feature_id),
        )
    expected_msg = (
        'Feature (name: "random_name") object(s) within the same namespace must have the same "name" value '
        '(namespace: "sum_30m", feature: "random_name").'
    )
    assert expected_msg in str(exc.value)
