"""
Test FeatureListNamespaceService
"""
import json

import pytest
from bson.objectid import ObjectId

from featurebyte.exception import DocumentInconsistencyError
from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from featurebyte.models.feature_list import FeatureListStatus, FeatureReadinessDistribution
from featurebyte.schema.feature import FeatureCreate
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceUpdate
from featurebyte.service.feature import FeatureService


async def insert_feature_list_into_persistent(
    user, persistent, version_suffix, readiness, name=None
):
    """Insert a feature list into persistent"""
    feature_service = FeatureService(user=user, persistent=persistent)
    with open("tests/fixtures/request_payloads/feature_sum_30m.json", encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        payload = FeatureCreate(**payload, user_id=user.id).dict(by_alias=True)
        payload["_id"] = ObjectId()
        payload["version"] = f'{payload["version"]}_{version_suffix}'
        payload["readiness"] = readiness
        if name:
            payload["name"] = name
            payload["feature_namespace_id"] = str(ObjectId())
        feature = await feature_service.create_document(data=FeatureCreate(**payload))

    with open(
        "tests/fixtures/request_payloads/feature_list_single.json", encoding="utf"
    ) as fhandle:
        payload = json.loads(fhandle.read())
        payload = FeatureListCreate(**payload, user_id=user.id).dict(by_alias=True)
        payload["_id"] = ObjectId()
        payload["feature_ids"] = [feature.id]
        payload["version"] = f'{payload["version"]}_{version_suffix}'
        payload["readiness_distribution"] = [{"readiness": readiness, "count": 1}]

    feature_list_id = await persistent.insert_one(
        collection_name="feature_list",
        document=payload,
        user_id=user.id,
    )
    return feature_list_id


@pytest.mark.asyncio
async def test_update_document__auto_default_version_mode(
    feature_list_namespace_service, feature_list, user, mock_insert_feature_registry
):
    """Test update document"""
    _ = mock_insert_feature_registry
    data = await feature_list_namespace_service.list_documents()
    persistent = feature_list_namespace_service.persistent
    assert data["total"] == 1
    feature_list_ids_before = data["data"][0]["feature_list_ids"]
    assert len(feature_list_ids_before) == 1

    # insert a feature_list_id to feature collection
    feature_list_id = await insert_feature_list_into_persistent(
        user=user,
        persistent=persistent,
        version_suffix={"_1"},
        readiness=FeatureReadiness.DRAFT.value,
    )

    # update feature list namespace by using newly inserted feature_list_id
    updated_doc = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceUpdate(feature_list_id=feature_list_id),
    )
    assert updated_doc.user_id == feature_list.user_id
    assert updated_doc.feature_list_ids == sorted([feature_list_ids_before[0], feature_list_id])
    assert updated_doc.readiness_distribution == FeatureReadinessDistribution(
        __root__=[{"readiness": FeatureReadiness.DRAFT, "count": 1}]
    )
    assert updated_doc.default_feature_list_id == feature_list_id
    assert updated_doc.default_version_mode == DefaultVersionMode.AUTO

    # update another feature_list_id with lower readiness distribution level
    worse_readiness_dist_feature_list_id = await insert_feature_list_into_persistent(
        user=user,
        persistent=persistent,
        version_suffix={"_2"},
        readiness=FeatureReadiness.DEPRECATED.value,
    )
    updated_doc = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceUpdate(feature_list_id=worse_readiness_dist_feature_list_id),
    )
    assert updated_doc.user_id == feature_list.user_id
    assert updated_doc.feature_list_ids == sorted(
        [
            feature_list_ids_before[0],
            feature_list_id,
            worse_readiness_dist_feature_list_id,
        ]
    )
    assert updated_doc.readiness_distribution == FeatureReadinessDistribution(
        __root__=[{"readiness": FeatureReadiness.DRAFT, "count": 1}]
    )
    assert updated_doc.default_feature_list_id == feature_list_id
    assert updated_doc.default_version_mode == DefaultVersionMode.AUTO


@pytest.mark.asyncio
async def test_update_document__manual_default_version_mode(
    feature_list_namespace_service, feature_list, user, mock_insert_feature_registry
):
    """Test update document"""
    _ = mock_insert_feature_registry
    data = await feature_list_namespace_service.list_documents()
    persistent = feature_list_namespace_service.persistent
    assert data["total"] == 1
    feature_list_ids_before = data["data"][0]["feature_list_ids"]
    assert len(feature_list_ids_before) == 1

    # test update default version mode
    updated_doc = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceUpdate(default_version_mode=DefaultVersionMode.MANUAL),
    )
    assert updated_doc.user_id == feature_list.user_id
    assert updated_doc.feature_list_ids == feature_list_ids_before
    assert updated_doc.readiness_distribution == FeatureReadinessDistribution(
        __root__=[{"readiness": FeatureReadiness.DRAFT, "count": 1}]
    )
    assert updated_doc.default_feature_list_id == feature_list_ids_before[0]
    assert updated_doc.default_version_mode == DefaultVersionMode.MANUAL

    # test insert a better readiness feature list (default feature list should be the same)
    prod_ready_feature_list_id = await insert_feature_list_into_persistent(
        user=user,
        persistent=persistent,
        version_suffix="_1",
        readiness=FeatureReadiness.PRODUCTION_READY.value,
    )
    updated_doc = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceUpdate(feature_list_id=prod_ready_feature_list_id),
    )
    assert updated_doc.feature_list_ids == sorted(
        feature_list_ids_before + [prod_ready_feature_list_id]
    )
    assert updated_doc.default_feature_list_id == feature_list_ids_before[0]
    assert updated_doc.readiness_distribution == FeatureReadinessDistribution(
        __root__=[{"readiness": FeatureReadiness.DRAFT, "count": 1}]
    )

    # test update default version mode to auto
    updated_doc = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceUpdate(default_version_mode=DefaultVersionMode.AUTO),
    )
    assert updated_doc.feature_list_ids == sorted(
        feature_list_ids_before + [prod_ready_feature_list_id]
    )
    assert updated_doc.default_feature_list_id == prod_ready_feature_list_id
    assert updated_doc.readiness_distribution == FeatureReadinessDistribution(
        __root__=[{"readiness": FeatureReadiness.PRODUCTION_READY, "count": 1}]
    )


@pytest.mark.asyncio
async def test_update_document__status(feature_list_namespace_service, feature_list):
    """Test update status"""
    original_doc = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert original_doc.status == FeatureListStatus.DRAFT
    updated_doc = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceUpdate(status=FeatureListStatus.PUBLISHED),
    )
    assert updated_doc.status == FeatureListStatus.PUBLISHED
    assert updated_doc.readiness_distribution == original_doc.readiness_distribution
    assert updated_doc.feature_list_ids == original_doc.feature_list_ids
    assert updated_doc.default_version_mode == original_doc.default_version_mode


@pytest.mark.asyncio
async def test_update_document__inconsistency_error(
    feature_list_namespace_service, feature_list, user
):
    """Test feature list namespace update - document inconsistency error"""
    inconsistent_feature_list_id = await insert_feature_list_into_persistent(
        user=user,
        persistent=feature_list_namespace_service.persistent,
        version_suffix="_1",
        readiness=FeatureReadiness.DRAFT.value,
        name="random_name",
    )
    with pytest.raises(DocumentInconsistencyError) as exc:
        await feature_list_namespace_service.update_document(
            document_id=feature_list.feature_list_namespace_id,
            data=FeatureListNamespaceUpdate(feature_list_id=inconsistent_feature_list_id),
        )
    expected_msg = (
        'FeatureList (name: "sf_feature_list") object(s) within the same namespace must '
        "share the same feature name(s)."
    )
    assert expected_msg in str(exc.value)
