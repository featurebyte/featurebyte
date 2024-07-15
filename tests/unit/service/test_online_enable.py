"""
Tests for OnlineEnableService
"""

import pytest
import pytest_asyncio

from featurebyte.models.feature import FeatureModel


async def check_states_after_online_enabled_change(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    feature,
    expected_online_enabled,
    expected_online_enabled_feature_ids,
    expected_number_of_feature_lists,
):
    """Check states after online_enabled get changed"""
    updated_feature = await feature_service.get_document(document_id=feature.id)
    assert updated_feature.online_enabled == expected_online_enabled

    namespace = await feature_namespace_service.get_document(document_id=feature.feature_namespace_id)
    assert namespace.online_enabled_feature_ids == expected_online_enabled_feature_ids

    # make sure there is something to test
    assert len(feature.feature_list_ids) == expected_number_of_feature_lists
    for feature_list_id in feature.feature_list_ids:
        feature_list = await feature_list_service.get_document(document_id=feature_list_id)
        assert feature_list.online_enabled_feature_ids == expected_online_enabled_feature_ids


@pytest_asyncio.fixture(name="online_enabled_production_ready_feature")
async def online_enabled_production_ready_feature_fixture(
    online_enable_service,
    production_ready_feature,
    feature_list,
):
    """
    Online enabled feature when the feature is used in one FeatureList
    """
    _ = feature_list
    online_enabled_feat = await online_enable_service.update_feature(
        feature_id=production_ready_feature.id,
        online_enabled=True,
    )
    assert isinstance(online_enabled_feat, FeatureModel)
    assert online_enabled_feat.id == production_ready_feature.id
    return online_enabled_feat


@pytest.mark.asyncio
async def test_update_feature(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    online_enable_service,
    online_enabled_production_ready_feature,
):
    """Test update feature"""
    await check_states_after_online_enabled_change(
        feature_service=feature_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature=online_enabled_production_ready_feature,
        expected_online_enabled=True,
        expected_online_enabled_feature_ids=[online_enabled_production_ready_feature.id],
        expected_number_of_feature_lists=1,
    )

    online_disabled_feat = await online_enable_service.update_feature(
        feature_id=online_enabled_production_ready_feature.id,
        online_enabled=False,
    )
    assert isinstance(online_disabled_feat, FeatureModel)
    await check_states_after_online_enabled_change(
        feature_service=feature_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature=online_disabled_feat,
        expected_online_enabled=False,
        expected_online_enabled_feature_ids=[],
        expected_number_of_feature_lists=1,
    )


@pytest.mark.asyncio
async def test_update_feature_when_already_enabled(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    online_enable_service,
    online_enabled_production_ready_feature,
    feature_list_repeated,
):
    """
    Test update feature when the feature is ready feature enabled and there is a new FeatureList
    referencing it
    """
    _ = feature_list_repeated
    online_enabled_feature = await online_enable_service.update_feature(
        feature_id=online_enabled_production_ready_feature.id,
        online_enabled=True,
    )
    await check_states_after_online_enabled_change(
        feature_service=feature_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature=online_enabled_feature,
        expected_online_enabled=True,
        expected_online_enabled_feature_ids=[online_enabled_feature.id],
        expected_number_of_feature_lists=2,
    )
