"""
Tests for DeployService
"""
import traceback

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.exception import DocumentError, DocumentNotFoundError, DocumentUpdateError
from featurebyte.models.feature_list import FeatureListModel


@pytest.fixture(name="deploy_service")
def deploy_service_fixture(app_container):
    """Deploy service fixture"""
    return app_container.deploy_service


@pytest.fixture(name="deployment_service")
def deployment_service_fixture(app_container):
    """Deployment service fixture"""
    return app_container.deployment_service


@pytest_asyncio.fixture(name="disabled_deployment")
async def disabled_deployment_fixture(deploy_service, deployment_service, feature_list):
    """Disabled deployment fixture"""
    deployment_id = ObjectId()
    await deploy_service.create_deployment(
        feature_list_id=feature_list.id,
        deployment_id=deployment_id,
        deployment_name=None,
        to_enable_deployment=False,
    )
    deployment = await deployment_service.get_document(document_id=deployment_id)
    assert deployment.enabled is False
    yield deployment


@pytest.mark.asyncio
async def test_create_disabled_deployment(feature_list_service, disabled_deployment):
    """Test create deployment (disabled)"""
    updated_feature_list = await feature_list_service.get_document(
        document_id=disabled_deployment.feature_list_id
    )
    assert updated_feature_list.deployed is False


@pytest.mark.asyncio
async def test_create_enabled_deployment__not_all_features_are_online_enabled(
    deploy_service, deployment_service, feature_list
):
    """Test create deployment (not all features are online enabled validation error)"""
    deployment_id = ObjectId()
    try:
        await deploy_service.create_deployment(
            feature_list_id=feature_list.id,
            deployment_id=deployment_id,
            deployment_name=None,
            to_enable_deployment=True,
        )
    except DocumentError:
        # Capture the traceback information
        exc_traceback = traceback.format_exc()

        expected_msg = "Only FeatureList object of all production ready features can be deployed."
        assert expected_msg in exc_traceback

    # check that deployment is not created
    with pytest.raises(DocumentNotFoundError):
        await deployment_service.get_document(document_id=deployment_id)


@pytest.mark.asyncio
async def test_update_deployment__not_all_features_are_online_enabled(
    deploy_service, deployment_service, disabled_deployment
):
    """Test update deployment (not all features are online enabled validation error)"""
    try:
        await deploy_service.update_deployment(
            deployment_id=disabled_deployment.id, to_enable_deployment=True
        )
    except DocumentError:
        # Capture the traceback information
        exc_traceback = traceback.format_exc()

        expected_msg = "Only FeatureList object of all production ready features can be deployed."
        assert expected_msg in exc_traceback

    # check that deployment is not updated
    updated_deployment = await deployment_service.get_document(document_id=disabled_deployment.id)
    assert updated_deployment.enabled is False


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


@pytest_asyncio.fixture(name="production_ready_feature_list")
async def production_ready_feature_list_fixture(feature_list, feature_readiness_service):
    """Feature list consists of production ready features"""
    for feature_id in feature_list.feature_ids:
        await feature_readiness_service.update_feature(
            feature_id=feature_id, readiness="PRODUCTION_READY"
        )
    yield feature_list


@pytest_asyncio.fixture(name="disabled_deployment_with_production_ready_features")
async def disabled_deployment_with_prod_ready_features(
    deploy_service, deployment_service, production_ready_feature_list
):
    """Disabled deployment fixture"""
    deployment_id = ObjectId()
    await deploy_service.create_deployment(
        feature_list_id=production_ready_feature_list.id,
        deployment_id=deployment_id,
        deployment_name=None,
        to_enable_deployment=False,
    )
    deployment = await deployment_service.get_document(document_id=deployment_id)
    assert deployment.enabled is False
    yield deployment


@pytest.mark.asyncio
async def test_update_deployment(
    deploy_service,
    deployment_service,
    feature_list_namespace_service,
    feature_list_service,
    feature_service,
    feature_list_status_service,
    production_ready_feature_list,
    mock_update_data_warehouse,
):
    """Test update feature list"""
    feature_list = production_ready_feature_list

    deployment_id = ObjectId()
    await deploy_service.create_deployment(
        feature_list_id=production_ready_feature_list.id,
        deployment_id=deployment_id,
        deployment_name=None,
        to_enable_deployment=True,
    )
    deployment = await deployment_service.get_document(document_id=deployment_id)
    deployed_feature_list = await feature_list_service.get_document(
        document_id=deployment.feature_list_id
    )
    mock_update_data_warehouse.assert_called_once()
    assert mock_update_data_warehouse.call_args[1]["updated_feature"].online_enabled is True

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

    await deploy_service.update_deployment(
        deployment_id=deployment_id,
        to_enable_deployment=False,
    )
    deployed_disabled_feature_list = await feature_list_service.get_document(
        document_id=deployment.feature_list_id
    )
    assert mock_update_data_warehouse.call_count == 2
    assert mock_update_data_warehouse.call_args[1]["updated_feature"].online_enabled is False

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

    # update feature list namespace status to deprecated and then deploy the feature list
    # should raise exception
    await feature_list_status_service.update_feature_list_namespace_status(
        feature_list_namespace_id=feature_list.feature_list_namespace_id,
        target_feature_list_status="DEPRECATED",
    )
    namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert namespace.status == "DEPRECATED"
    try:
        await deploy_service.update_deployment(
            deployment_id=deployment.id,
            to_enable_deployment=True,
        )
    except DocumentUpdateError:
        # Capture the traceback information
        exc_traceback = traceback.format_exc()

        expected_msg = "Deprecated feature list cannot be deployed."
        assert expected_msg in exc_traceback


@pytest.mark.asyncio
async def test_update_deployment__deployment_is_disabled_when_exception_raised(
    deploy_service,
    deployment_service,
    feature_list_service,
    feature_service,
    disabled_deployment_with_production_ready_features,
    mock_update_data_warehouse,
):
    """Test update deployment exception raised"""
    random_error_msg = "random error when calling update_data_warehouse!!"
    mock_update_data_warehouse.side_effect = Exception(random_error_msg)
    try:
        await deploy_service.update_deployment(
            deployment_id=disabled_deployment_with_production_ready_features.id,
            to_enable_deployment=True,
        )
    except Exception:  # pylint: disable=broad-except
        # Capture the traceback information
        exc_traceback = traceback.format_exc()

        assert random_error_msg in exc_traceback

        # check post condition
        updated_deployment = await deployment_service.get_document(
            document_id=disabled_deployment_with_production_ready_features.id
        )
        assert updated_deployment.enabled is False
        feature_list = await feature_list_service.get_document(
            document_id=disabled_deployment_with_production_ready_features.feature_list_id
        )
        assert feature_list.deployed is False
        for feature_id in feature_list.feature_ids:
            feature = await feature_service.get_document(document_id=feature_id)
            assert feature.online_enabled is False
