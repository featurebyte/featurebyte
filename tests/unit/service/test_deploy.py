"""
Tests for DeployService
"""
import pytest

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature_list import FeatureListModel


@pytest.mark.asyncio
async def test_update_feature_list__feature_not_online_enabled_error(deploy_service, feature_list):
    """Test update feature list (not all features are online enabled validation error)"""
    with pytest.raises(DocumentUpdateError) as exc:
        await deploy_service.update_feature_list(feature_list_id=feature_list.id, deployed=True)
    expected_msg = "Only FeatureList object of all production ready features can be deployed."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_update_feature_list__no_update(deploy_service, feature_list):
    """Test update feature list when deployed status is the same"""
    updated_feature_list = await deploy_service.update_feature_list(
        feature_list_id=feature_list.id, deployed=feature_list.deployed
    )
    assert updated_feature_list == feature_list


@pytest.mark.asyncio
async def test_update_feature_list_namespace__no_update_except_updated_at(
    deploy_service, feature_list, feature_list_namespace
):
    """Test update feature list namespace when deployed status is the same"""
    updated_namespace = await deploy_service._update_feature_list_namespace(
        feature_list_namespace_id=feature_list.feature_list_namespace_id,
        feature_list=feature_list,
    )
    assert updated_namespace.dict(exclude={"updated_at": True}) == feature_list_namespace.dict(
        exclude={"updated_at": True}
    )


@pytest.mark.asyncio
async def test_update_feature__no_update_except_updated_at(
    deploy_service, feature_service, feature, feature_list
):
    """Test update feature when deployed status is the same"""
    feature = await feature_service.get_document(document_id=feature.id)
    updated_feature = await deploy_service._update_feature(
        feature_id=feature.id,
        feature_list=feature_list,
    )
    assert updated_feature.dict(exclude={"updated_at": True}) == feature.dict(
        exclude={"updated_at": True}
    )


async def check_states_after_deployed_change(
    feature_service,
    feature_list_namespace_service,
    feature_list_service,
    feature_list,
    expected_deployed,
    expected_deployed_feature_list_ids,
):
    """Check states after deployed get changed"""
    updated_feature_list = await feature_list_service.get_document(document_id=feature_list.id)
    assert updated_feature_list.deployed == expected_deployed

    namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert namespace.deployed_feature_list_ids == expected_deployed_feature_list_ids

    for feature_id in feature_list.feature_ids:
        feature = await feature_service.get_document(document_id=feature_id)
        assert feature.deployed_feature_list_ids == expected_deployed_feature_list_ids


@pytest.mark.asyncio
async def test_update_feature_list(
    feature_service,
    feature_list_namespace_service,
    feature_list_service,
    feature_list,
    feature_readiness_service,
    deploy_service,
):
    """Test update feature list"""
    for feature_id in feature_list.feature_ids:
        await feature_readiness_service.update_feature(
            feature_id=feature_id, readiness="PRODUCTION_READY"
        )

    assert feature_list.online_enabled_feature_ids == []
    deployed_feature_list = await deploy_service.update_feature_list(
        feature_list_id=feature_list.id, deployed=True, return_document=True
    )
    assert deployed_feature_list.online_enabled_feature_ids == deployed_feature_list.feature_ids
    assert isinstance(deployed_feature_list, FeatureListModel)
    await check_states_after_deployed_change(
        feature_service=feature_service,
        feature_list_namespace_service=feature_list_namespace_service,
        feature_list_service=feature_list_service,
        feature_list=deployed_feature_list,
        expected_deployed=True,
        expected_deployed_feature_list_ids=[feature_list.id],
    )

    deployed_disabled_feature_list = await deploy_service.update_feature_list(
        feature_list_id=feature_list.id, deployed=False, return_document=True
    )
    assert deployed_disabled_feature_list.online_enabled_feature_ids == []
    assert isinstance(deployed_disabled_feature_list, FeatureListModel)
    await check_states_after_deployed_change(
        feature_service=feature_service,
        feature_list_namespace_service=feature_list_namespace_service,
        feature_list_service=feature_list_service,
        feature_list=deployed_disabled_feature_list,
        expected_deployed=False,
        expected_deployed_feature_list_ids=[],
    )
