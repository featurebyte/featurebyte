"""
Tests for OnlineEnableService
"""
from unittest.mock import Mock

import pytest

from featurebyte.models.feature import FeatureModel


async def check_states_after_online_enabled_change(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    feature,
    expected_online_enabled,
    expected_online_enabled_feature_ids,
    online_enable_service_data_warehouse_mocks,
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

    if expected_online_enabled:
        online_enable_service_data_warehouse_mocks[
            "feature_manager"
        ].online_enable.assert_called_once()
    else:
        online_enable_service_data_warehouse_mocks[
            "feature_manager"
        ].online_disable.assert_called_once()


@pytest.mark.asyncio
async def test_update_feature(
    feature_service,
    feature_namespace_service,
    feature_list_service,
    online_enable_service,
    production_ready_feature,
    online_enable_service_data_warehouse_mocks,
):
    """Test update feature"""
    online_enabled_feat = await online_enable_service.update_feature(
        feature_id=production_ready_feature.id,
        online_enabled=True,
        return_document=True,
        get_credential=Mock(),
    )
    assert isinstance(online_enabled_feat, FeatureModel)
    await check_states_after_online_enabled_change(
        feature_service=feature_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature=online_enabled_feat,
        expected_online_enabled=True,
        expected_online_enabled_feature_ids=[production_ready_feature.id],
        online_enable_service_data_warehouse_mocks=online_enable_service_data_warehouse_mocks,
    )

    online_disabled_feat = await online_enable_service.update_feature(
        feature_id=online_enabled_feat.id,
        online_enabled=False,
        return_document=True,
        get_credential=Mock(),
    )
    assert isinstance(online_disabled_feat, FeatureModel)
    await check_states_after_online_enabled_change(
        feature_service=feature_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature=online_disabled_feat,
        expected_online_enabled=False,
        expected_online_enabled_feature_ids=[],
        online_enable_service_data_warehouse_mocks=online_enable_service_data_warehouse_mocks,
    )
