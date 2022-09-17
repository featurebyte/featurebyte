"""
Tests for FeatureReadinessService
"""

import pytest

from featurebyte.models.feature import FeatureReadiness
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate


async def check_states_readiness_change(
    feature_readiness_service,
    feature_namespace_service,
    feature_list_service,
    feature_list_namespace_service,
    new_feature_id,
    new_feature_list,
    new_feature_next_readiness,
    expected_default_feature_id,
    expected_default_readiness,
    expected_feature_list_readiness_distribution,
    expected_default_feature_list_id,
    expected_default_feature_list_readiness_distribution,
):
    """Check states after feature readiness get changed"""
    new_feat = await feature_readiness_service.update_feature(
        feature_id=new_feature_id, readiness=new_feature_next_readiness, return_document=True
    )
    assert new_feat.feature_list_ids == [new_feature_list.id]

    # check feature namespace get updated (new feature become the default one)
    updated_feat_namespace = await feature_namespace_service.get_document(
        document_id=new_feat.feature_namespace_id
    )
    assert updated_feat_namespace.default_feature_id == expected_default_feature_id
    assert updated_feat_namespace.readiness == expected_default_readiness

    # check feature list version get updated (new feature list readiness distribution get updated)
    updated_flist = await feature_list_service.get_document(document_id=new_feature_list.id)
    assert (
        updated_flist.readiness_distribution.__root__
        == expected_feature_list_readiness_distribution
    )

    # check feature list namespace (new feature list becomes the default one)
    updated_flist_namespace = await feature_list_namespace_service.get_document(
        document_id=new_feature_list.feature_list_namespace_id,
    )
    assert updated_flist_namespace.default_feature_list_id == expected_default_feature_list_id
    assert (
        updated_flist_namespace.readiness_distribution.__root__
        == expected_default_feature_list_readiness_distribution
    )


@pytest.mark.asyncio
async def test_update_document__auto_default_version_mode(
    setup_for_feature_readiness,
    feature_namespace_service,
    feature_list_service,
    feature_list_namespace_service,
    feature_readiness_service,
    feature,
    feature_list,
    user,
):
    """Test update document (auto default version mode)"""
    new_feature_id, new_feature_list_id = setup_for_feature_readiness
    new_feature_list = await feature_list_service.get_document(document_id=new_feature_list_id)

    # upgrade new feature's readiness level to production
    await check_states_readiness_change(
        feature_readiness_service=feature_readiness_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature_list_namespace_service=feature_list_namespace_service,
        new_feature_id=new_feature_id,
        new_feature_list=new_feature_list,
        new_feature_next_readiness=FeatureReadiness.PRODUCTION_READY,
        expected_default_feature_id=new_feature_id,
        expected_default_readiness=FeatureReadiness.PRODUCTION_READY,
        expected_feature_list_readiness_distribution=[
            {"readiness": "PRODUCTION_READY", "count": 1}
        ],
        expected_default_feature_list_id=new_feature_list_id,
        expected_default_feature_list_readiness_distribution=[
            {"readiness": "PRODUCTION_READY", "count": 1}
        ],
    )

    # downgrade new feature's readiness from production ready to deprecated
    await check_states_readiness_change(
        feature_readiness_service=feature_readiness_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature_list_namespace_service=feature_list_namespace_service,
        new_feature_id=new_feature_id,
        new_feature_list=new_feature_list,
        new_feature_next_readiness=FeatureReadiness.DEPRECATED,
        expected_default_feature_id=feature.id,
        expected_default_readiness=FeatureReadiness.DRAFT,
        expected_feature_list_readiness_distribution=[{"readiness": "DEPRECATED", "count": 1}],
        expected_default_feature_list_id=feature_list.id,
        expected_default_feature_list_readiness_distribution=[{"readiness": "DRAFT", "count": 1}],
    )


@pytest.mark.asyncio
async def test_update_document__manual_default_version_mode__non_default_feature_readiness_change(
    setup_for_feature_readiness,
    feature_namespace_service,
    feature_list_service,
    feature_list_namespace_service,
    feature_readiness_service,
    feature,
    feature_list,
    user,
):
    """Test update document (manual default version mode, upgrade non-default feature's readiness)"""
    new_feature_id, new_feature_list_id = setup_for_feature_readiness
    new_feature_list = await feature_list_service.get_document(document_id=new_feature_list_id)

    # change default version mode to manual first
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(default_version_mode="MANUAL"),
    )
    assert feat_namespace.default_version_mode == "MANUAL"
    assert feat_namespace.default_feature_id == feature.id
    flist_namespace = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceServiceUpdate(default_version_mode="MANUAL"),
    )
    assert flist_namespace.default_version_mode == "MANUAL"
    assert flist_namespace.default_feature_list_id == feature_list.id

    # upgrade new feature's readiness level to production
    await check_states_readiness_change(
        feature_readiness_service=feature_readiness_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature_list_namespace_service=feature_list_namespace_service,
        new_feature_id=new_feature_id,
        new_feature_list=new_feature_list,
        new_feature_next_readiness=FeatureReadiness.PRODUCTION_READY,
        expected_default_feature_id=feature.id,
        expected_default_readiness=FeatureReadiness.DRAFT,
        expected_feature_list_readiness_distribution=[
            {"readiness": "PRODUCTION_READY", "count": 1}
        ],
        expected_default_feature_list_id=feature_list.id,
        expected_default_feature_list_readiness_distribution=[{"readiness": "DRAFT", "count": 1}],
    )

    # downgrade new feature's readiness level to deprecated
    await check_states_readiness_change(
        feature_readiness_service=feature_readiness_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature_list_namespace_service=feature_list_namespace_service,
        new_feature_id=new_feature_id,
        new_feature_list=new_feature_list,
        new_feature_next_readiness=FeatureReadiness.DEPRECATED,
        expected_default_feature_id=feature.id,
        expected_default_readiness=FeatureReadiness.DRAFT,
        expected_feature_list_readiness_distribution=[{"readiness": "DEPRECATED", "count": 1}],
        expected_default_feature_list_id=feature_list.id,
        expected_default_feature_list_readiness_distribution=[{"readiness": "DRAFT", "count": 1}],
    )


@pytest.mark.asyncio
async def test_update_document__manual_default_version_mode__default_feature_readiness_change(
    setup_for_feature_readiness,
    feature_namespace_service,
    feature_list_service,
    feature_list_namespace_service,
    feature_readiness_service,
    feature,
    feature_list,
    user,
):
    """Test update document (manual default version mode, upgrade non-default feature's readiness)"""
    new_feature_id, new_feature_list_id = setup_for_feature_readiness
    new_feature_list = await feature_list_service.get_document(document_id=new_feature_list_id)
    await feature_readiness_service.update_feature(
        feature_id=new_feature_id, readiness="PRODUCTION_READY", return_document=False
    )

    # change default version mode to manual first
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(default_version_mode="MANUAL"),
    )
    assert feat_namespace.default_version_mode == "MANUAL"
    assert feat_namespace.default_feature_id == new_feature_id
    flist_namespace = await feature_list_namespace_service.update_document(
        document_id=feature_list.feature_list_namespace_id,
        data=FeatureListNamespaceServiceUpdate(default_version_mode="MANUAL"),
    )
    assert flist_namespace.default_version_mode == "MANUAL"
    assert flist_namespace.default_feature_list_id == new_feature_list_id

    # downgrade new feature's readiness level to deprecated
    await check_states_readiness_change(
        feature_readiness_service=feature_readiness_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature_list_namespace_service=feature_list_namespace_service,
        new_feature_id=new_feature_id,
        new_feature_list=new_feature_list,
        new_feature_next_readiness=FeatureReadiness.DEPRECATED,
        expected_default_feature_id=new_feature_id,
        expected_default_readiness=FeatureReadiness.DEPRECATED,
        expected_feature_list_readiness_distribution=[{"readiness": "DEPRECATED", "count": 1}],
        expected_default_feature_list_id=new_feature_list_id,
        expected_default_feature_list_readiness_distribution=[
            {"readiness": "DEPRECATED", "count": 1}
        ],
    )

    # upgrade new feature's readiness level to production ready
    await check_states_readiness_change(
        feature_readiness_service=feature_readiness_service,
        feature_namespace_service=feature_namespace_service,
        feature_list_service=feature_list_service,
        feature_list_namespace_service=feature_list_namespace_service,
        new_feature_id=new_feature_id,
        new_feature_list=new_feature_list,
        new_feature_next_readiness=FeatureReadiness.PRODUCTION_READY,
        expected_default_feature_id=new_feature_id,
        expected_default_readiness=FeatureReadiness.PRODUCTION_READY,
        expected_feature_list_readiness_distribution=[
            {"readiness": "PRODUCTION_READY", "count": 1}
        ],
        expected_default_feature_list_id=new_feature_list_id,
        expected_default_feature_list_readiness_distribution=[
            {"readiness": "PRODUCTION_READY", "count": 1}
        ],
    )
