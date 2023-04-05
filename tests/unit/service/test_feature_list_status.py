"""
Test the feature list status service.
"""
from unittest.mock import Mock

import pytest
import pytest_asyncio

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature_list import FeatureListStatus
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate


@pytest.fixture(name="feature_list_status_service")
def feature_list_status_service_fixture(app_container):
    """Feature list status service fixture"""
    return app_container.feature_list_status_service


@pytest_asyncio.fixture(name="feature_list_namespace_deprecated")
async def feature_list_namespace_deprecated_fixture(
    feature_list_namespace_service, feature_list_namespace
):
    """Feature list namespace deprecated fixture"""
    await feature_list_namespace_service.update_document(
        document_id=feature_list_namespace.id,
        data=FeatureListNamespaceServiceUpdate(status=FeatureListStatus.DEPRECATED),
    )
    return await feature_list_namespace_service.get_document(document_id=feature_list_namespace.id)


@pytest_asyncio.fixture(name="feature_list_namespace_deployed")
async def feature_list_namespace_deployed_fixture(
    deploy_service,
    feature_readiness_service,
    feature_list_namespace_service,
    feature_list,
    mock_update_data_warehouse,
):
    """Feature list namespace deployed fixture"""
    _ = mock_update_data_warehouse
    namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert namespace.status == FeatureListStatus.DRAFT
    for feature_id in feature_list.feature_ids:
        await feature_readiness_service.update_feature(
            feature_id=feature_id,
            readiness="PRODUCTION_READY",
        )

    updated_feature_list = await deploy_service.update_feature_list(
        feature_list_id=feature_list.id,
        deployed=True,
        get_credential=Mock(),
    )
    namespace = await feature_list_namespace_service.get_document(
        document_id=updated_feature_list.feature_list_namespace_id
    )
    assert namespace.status == FeatureListStatus.DEPLOYED
    return namespace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "feature_list_status",
    [
        FeatureListStatus.DRAFT,
        FeatureListStatus.PUBLIC_DRAFT,
        FeatureListStatus.TEMPLATE,
        FeatureListStatus.DEPLOYED,
    ],
)
async def test_feature_list_status__deprecated_feature_list_cannot_update(
    feature_list_status_service, feature_list_namespace_deprecated, feature_list_status
):
    """Test that a deprecated feature list cannot be updated to any other status."""
    with pytest.raises(DocumentUpdateError) as exc:
        await feature_list_status_service.update_feature_list_namespace_status(
            feature_list_namespace_id=feature_list_namespace_deprecated.id,
            feature_list_status=feature_list_status,
        )

    expected_msg = (
        f'FeatureList (name: "{feature_list_namespace_deprecated.name}") is deprecated. '
        "It cannot be updated to any other status."
    )
    assert expected_msg in str(exc.value)


async def check_transit_to_draft_is_not_allow(feature_list_status_service, feature_list_namespace):
    """Test that a feature list namespace cannot be updated to draft status."""
    with pytest.raises(DocumentUpdateError) as exc:
        await feature_list_status_service.update_feature_list_namespace_status(
            feature_list_namespace_id=feature_list_namespace.id,
            feature_list_status=FeatureListStatus.DRAFT,
        )

    expected_msg = (
        f'Not allowed to update status of FeatureList (name: "{feature_list_namespace.name}") '
        "to draft status."
    )
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "feature_list_status",
    [
        FeatureListStatus.DRAFT,
        FeatureListStatus.PUBLIC_DRAFT,
        FeatureListStatus.TEMPLATE,
        FeatureListStatus.DEPRECATED,
    ],
)
async def test_feature_list_status__deployed_feature_list_transition_check(
    feature_list_status_service, feature_list_namespace_deployed, feature_list_status
):
    """Test that a deployed feature list cannot be updated to any other status."""
    if feature_list_status == FeatureListStatus.DRAFT:
        await check_transit_to_draft_is_not_allow(
            feature_list_status_service, feature_list_namespace_deployed
        )
    else:
        with pytest.raises(DocumentUpdateError) as exc:
            await feature_list_status_service.update_feature_list_namespace_status(
                feature_list_namespace_id=feature_list_namespace_deployed.id,
                feature_list_status=feature_list_status,
            )

        assert len(feature_list_namespace_deployed.deployed_feature_list_ids) == 1
        feature_list_id = feature_list_namespace_deployed.feature_list_ids[0]
        expected_msg = (
            f'Not allowed to update status of FeatureList (name: "{feature_list_namespace_deployed.name}") '
            f'with deployed feature list ids: ["{feature_list_id}"].'
        )
        assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_feature_list_status__deployed_feature_namespace_transit_to_public_draft(
    deploy_service,
    feature_list_namespace_service,
    feature_list_namespace_deployed,
    mock_update_data_warehouse,
):
    """
    Test that a deployed feature list namespace is updated to public draft status when
    all deployed feature lists are undeployed.
    """
    _ = mock_update_data_warehouse
    assert feature_list_namespace_deployed.status == FeatureListStatus.DEPLOYED
    feature_list_id = feature_list_namespace_deployed.feature_list_ids[0]
    updated_feature_list = await deploy_service.update_feature_list(
        feature_list_id=feature_list_id,
        deployed=False,
        get_credential=Mock(),
    )
    namespace = await feature_list_namespace_service.get_document(
        document_id=updated_feature_list.feature_list_namespace_id
    )
    assert namespace.deployed_feature_list_ids == []
    assert namespace.status == FeatureListStatus.PUBLIC_DRAFT


@pytest.mark.asyncio
async def test_feature_list_status__allowed_status_transition(
    feature_list_status_service,
    feature_list_namespace_service,
    feature_list_namespace,
):
    """Test that allowed status transition is correct."""
    assert feature_list_namespace.status == FeatureListStatus.DRAFT

    # check transit from draft to public draft
    await feature_list_status_service.update_feature_list_namespace_status(
        feature_list_namespace_id=feature_list_namespace.id,
        feature_list_status=FeatureListStatus.PUBLIC_DRAFT,
    )
    namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list_namespace.id
    )
    assert namespace.status == FeatureListStatus.PUBLIC_DRAFT
    await check_transit_to_draft_is_not_allow(feature_list_status_service, namespace)

    # check transit from public draft to template
    await feature_list_status_service.update_feature_list_namespace_status(
        feature_list_namespace_id=namespace.id,
        feature_list_status=FeatureListStatus.TEMPLATE,
    )
    namespace = await feature_list_namespace_service.get_document(document_id=namespace.id)
    assert namespace.status == FeatureListStatus.TEMPLATE
    await check_transit_to_draft_is_not_allow(feature_list_status_service, namespace)

    # check transit from template to deprecated
    await feature_list_status_service.update_feature_list_namespace_status(
        feature_list_namespace_id=namespace.id,
        feature_list_status=FeatureListStatus.DEPRECATED,
    )
    namespace = await feature_list_namespace_service.get_document(document_id=namespace.id)
    assert namespace.status == FeatureListStatus.DEPRECATED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "feature_list_status",
    [FeatureListStatus.TEMPLATE, FeatureListStatus.DEPRECATED],
)
async def test_feature_list_status__deployed_can_only_transit_to_public_draft(
    feature_list_namespace_service,
    feature_list_status_service,
    feature_list_namespace,
    feature_list_status,
):
    """Test that a deployed feature list namespace can only be updated to public draft status."""
    # check a deployed status manually (not through the deploy service)
    namespace = await feature_list_namespace_service.update_document(
        document_id=feature_list_namespace.id,
        data=FeatureListNamespaceServiceUpdate(status=FeatureListStatus.DEPLOYED),
    )
    assert namespace.status == FeatureListStatus.DEPLOYED

    with pytest.raises(DocumentUpdateError) as exc:
        await feature_list_status_service.update_feature_list_namespace_status(
            feature_list_namespace_id=namespace.id,
            feature_list_status=feature_list_status,
        )

    expected_msg = f'Deployed FeatureList (name: "{feature_list_namespace.name}") can only be updated to public draft status.'
    assert expected_msg in str(exc.value)
