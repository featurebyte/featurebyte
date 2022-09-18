"""
Test for OnlineEnableService
"""
import pytest
import pytest_asyncio

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature import FeatureModel
from featurebyte.service.online_enable import OnlineEnableService


@pytest.fixture(name="online_enable_service")
def online_enable_service_fixture(user, persistent):
    """OnlineEnableService fixture"""
    return OnlineEnableService(user=user, persistent=persistent)


@pytest_asyncio.fixture(name="production_ready_feature")
async def production_ready_feature_fixture(feature_readiness_service, feature):
    """Production ready readiness feature fixture"""
    prod_feat = await feature_readiness_service.update_feature(
        feature_id=feature.id, readiness="PRODUCTION_READY"
    )
    assert prod_feat.readiness == "PRODUCTION_READY"
    return prod_feat


@pytest.mark.asyncio
async def test_update_feature__validation_error(online_enable_service, feature):
    """Test update feature (validation error)"""
    assert feature.readiness == "DRAFT"
    with pytest.raises(DocumentUpdateError) as exc:
        await online_enable_service.update_feature(feature_id=feature.id, online_enabled=True)
    expected_msg = "Only Feature object with PRODUCTION_READY readiness can be online enabled (readiness: DRAFT)."
    assert expected_msg in str(exc.value)


async def check_states_after_online_enabled_change(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    feature,
    expected_online_enabled,
    expected_online_enabled_feature_ids,
):
    """Check states after online_enabled get changed"""
    updated_feature = await feature_service.get_document(document_id=feature.id)
    assert updated_feature.online_enabled == expected_online_enabled

    namespace = await feature_namespace_service.get_document(
        document_id=feature.feature_namespace_id
    )
    assert namespace.online_enabled_feature_ids == expected_online_enabled_feature_ids

    for feature_list_id in feature.feature_list_ids:
        feature_list = await feature_list_service.get_document(document_id=feature_list_id)
        assert feature_list.online_enabled_feature_ids == expected_online_enabled_feature_ids


@pytest.mark.asyncio
async def test_update_feature(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    online_enable_service,
    production_ready_feature,
):
    """Test update feature"""
    online_enabled_feat = await online_enable_service.update_feature(
        feature_id=production_ready_feature.id,
        online_enabled=True,
        return_document=True,
    )
    assert isinstance(online_enabled_feat, FeatureModel)
    await check_states_after_online_enabled_change(
        feature_service=feature_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature=online_enabled_feat,
        expected_online_enabled=True,
        expected_online_enabled_feature_ids=[production_ready_feature.id],
    )

    online_disabled_feat = await online_enable_service.update_feature(
        feature_id=online_enabled_feat.id,
        online_enabled=False,
        return_document=True,
    )
    assert isinstance(online_disabled_feat, FeatureModel)
    await check_states_after_online_enabled_change(
        feature_service=feature_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature=online_disabled_feat,
        expected_online_enabled=False,
        expected_online_enabled_feature_ids=[],
    )
