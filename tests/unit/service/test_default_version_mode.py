"""
Test for DefaultVersionModeService
"""
import pytest


@pytest.mark.asyncio
async def test_update_feature_namespace__no_update(
    default_version_mode_service, feature, feature_namespace
):
    """Test when default version mode is the same"""
    updated_namespace = await default_version_mode_service.update_feature_namespace(
        feature_namespace_id=feature.feature_namespace_id, default_version_mode="AUTO"
    )
    assert feature_namespace == updated_namespace


@pytest.mark.asyncio
async def test_update_feature_list_namespace__no_update(
    default_version_mode_service, feature_list, feature_list_namespace
):
    """Test when default version mode is the same"""
    updated_namespace = await default_version_mode_service.update_feature_list_namespace(
        feature_list_namespace_id=feature_list.feature_list_namespace_id,
        default_version_mode="AUTO",
    )
    assert feature_list_namespace == updated_namespace


@pytest.mark.asyncio
async def test_update_feature_namespace(
    setup_for_feature_readiness,
    default_version_mode_service,
    feature_list_namespace_service,
    feature_list_service,
    feature_namespace_service,
    feature_service,
    feature_readiness_service,
    feature,
):
    """Test update_feature_default_version_mode"""
    new_feature_id, new_feature_list_id = setup_for_feature_readiness
    new_feature = await feature_service.get_document(document_id=new_feature_id)
    assert new_feature.feature_list_ids == [new_feature_list_id]

    # change default version mode to manual first
    namespace = await default_version_mode_service.update_feature_namespace(
        feature_namespace_id=feature.feature_namespace_id,
        default_version_mode="MANUAL",
        return_document=True,
    )
    assert namespace.default_version_mode == "MANUAL"
    assert namespace.default_feature_id == feature.id
    assert namespace.readiness == "DRAFT"

    # upgrade readiness to production ready & check default feature ID (should be the same)
    await feature_readiness_service.update_feature(
        feature_id=new_feature_id,
        readiness="PRODUCTION_READY",
        return_document=False,
    )
    namespace = await feature_namespace_service.get_document(
        document_id=feature.feature_namespace_id
    )
    assert namespace.default_feature_id == feature.id
    assert namespace.readiness == "DRAFT"

    # check default feature list (feature list namespace default version mode is still AUTO)
    new_feature_list = await feature_list_service.get_document(document_id=new_feature_list_id)
    fl_namespace = await feature_list_namespace_service.get_document(
        document_id=new_feature_list.feature_list_namespace_id
    )
    assert fl_namespace.default_version_mode == "AUTO"
    assert fl_namespace.default_feature_list_id == new_feature_list_id

    # change default version mode to auto, production ready feature should be the default feature
    namespace = await default_version_mode_service.update_feature_namespace(
        feature_namespace_id=feature.feature_namespace_id,
        default_version_mode="AUTO",
        return_document=True,
    )
    assert namespace.default_version_mode == "AUTO"
    assert namespace.default_feature_id == new_feature_id
    assert namespace.readiness == "PRODUCTION_READY"


@pytest.mark.asyncio
async def test_update_feature_list_namespace(
    setup_for_feature_readiness,
    default_version_mode_service,
    feature_list_namespace_service,
    feature_list_service,
    feature_namespace_service,
    feature_service,
    feature_readiness_service,
    feature_list,
):
    """Test update_feature_list_default_version_mode"""
    new_feature_id, new_feature_list_id = setup_for_feature_readiness

    # change default version mode to manual first
    namespace = await default_version_mode_service.update_feature_list_namespace(
        feature_list_namespace_id=feature_list.feature_list_namespace_id,
        default_version_mode="MANUAL",
        return_document=True,
    )
    assert namespace.default_version_mode == "MANUAL"
    assert namespace.default_feature_list_id == feature_list.id
    assert namespace.readiness_distribution.__root__ == [{"readiness": "DRAFT", "count": 1}]

    # upgrade readiness to production ready & check default feature list ID (should be the same)
    await feature_readiness_service.update_feature(
        feature_id=new_feature_id,
        readiness="PRODUCTION_READY",
        return_document=False,
    )
    namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert namespace.default_feature_list_id == feature_list.id
    assert namespace.readiness_distribution.__root__ == [{"readiness": "DRAFT", "count": 1}]

    # change default version mode to auto, production ready feature list should be the default feature list
    namespace = await default_version_mode_service.update_feature_list_namespace(
        feature_list_namespace_id=feature_list.feature_list_namespace_id,
        default_version_mode="AUTO",
        return_document=True,
    )
    assert namespace.default_version_mode == "AUTO"
    assert namespace.default_feature_list_id == new_feature_list_id
    assert namespace.readiness_distribution.__root__ == [
        {"readiness": "PRODUCTION_READY", "count": 1}
    ]
