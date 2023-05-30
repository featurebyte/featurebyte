"""
Tests for DeployService
"""
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.exception import (
    DocumentCreationError,
    DocumentError,
    DocumentNotFoundError,
    DocumentUpdateError,
)
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_list import FeatureListModel


@pytest_asyncio.fixture(name="disabled_deployment")
async def disabled_deployment_fixture(app_container, feature_list):
    """Disabled deployment fixture"""
    deployment_id = ObjectId()
    await app_container.deploy_service.create_deployment(
        feature_list_id=feature_list.id,
        deployment_id=deployment_id,
        deployment_name=None,
        to_enable_deployment=False,
        get_credential=Mock(),
    )
    deployment = await app_container.deployment_service.get_document(document_id=deployment_id)
    assert deployment.enabled is False
    yield deployment


@pytest.mark.asyncio
async def test_create_disabled_deployment(app_container, disabled_deployment):
    """Test create deployment (disabled)"""
    updated_feature_list = await app_container.feature_list_service.get_document(
        document_id=disabled_deployment.feature_list_id
    )
    assert updated_feature_list.deployed is False


@pytest.mark.asyncio
async def test_create_enabled_deployment__not_all_features_are_online_enabled(
    app_container, feature_list
):
    """Test create deployment (not all features are online enabled validation error)"""
    deployment_id = ObjectId()
    with pytest.raises(DocumentError) as exc:
        await app_container.deploy_service.create_deployment(
            feature_list_id=feature_list.id,
            deployment_id=deployment_id,
            deployment_name=None,
            to_enable_deployment=True,
            get_credential=Mock(),
        )

    expected_msg = "Only FeatureList object of all production ready features can be deployed."
    assert expected_msg in str(exc.value)

    # check that deployment is not created
    with pytest.raises(DocumentNotFoundError):
        await app_container.deployment_service.get_document(document_id=deployment_id)


@pytest.mark.asyncio
async def test_update_deployment__not_all_features_are_online_enabled(
    app_container, disabled_deployment
):
    """Test update deployment (not all features are online enabled validation error)"""
    with pytest.raises(DocumentError) as exc:
        await app_container.deploy_service.update_deployment(
            deployment_id=disabled_deployment.id, enabled=True, get_credential=Mock()
        )

    expected_msg = "Only FeatureList object of all production ready features can be deployed."
    assert expected_msg in str(exc.value)

    # check that deployment is not updated
    updated_deployment = await app_container.deployment_service.get_document(
        document_id=disabled_deployment.id
    )
    assert updated_deployment.enabled is False


@pytest.mark.asyncio
async def test_update_deployment__no_update(app_container, disabled_deployment):
    """Test update feature list when deployed status is the same"""
    await app_container.deploy_service.update_deployment(
        deployment_id=disabled_deployment.id, enabled=False, get_credential=Mock()
    )

    # check that deployment is not updated
    updated_deployment = await app_container.deployment_service.get_document(
        document_id=disabled_deployment.id
    )
    assert updated_deployment == disabled_deployment


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
    app_container, production_ready_feature_list
):
    """Disabled deployment fixture"""
    deployment_id = ObjectId()
    await app_container.deploy_service.create_deployment(
        feature_list_id=production_ready_feature_list.id,
        deployment_id=deployment_id,
        deployment_name=None,
        to_enable_deployment=False,
        get_credential=Mock(),
    )
    deployment = await app_container.deployment_service.get_document(document_id=deployment_id)
    assert deployment.enabled is False
    yield deployment


@pytest.mark.asyncio
async def test_update_deployment(
    app_container,
    production_ready_feature_list,
    mock_update_data_warehouse,
):
    """Test update feature list"""
    feature_list = production_ready_feature_list

    deployment_id = ObjectId()
    await app_container.deploy_service.create_deployment(
        feature_list_id=production_ready_feature_list.id,
        deployment_id=deployment_id,
        deployment_name=None,
        to_enable_deployment=True,
        get_credential=Mock(),
    )
    deployment = await app_container.deployment_service.get_document(document_id=deployment_id)
    deployed_feature_list = await app_container.feature_list_service.get_document(
        document_id=deployment.feature_list_id
    )
    mock_update_data_warehouse.assert_called_once()
    assert mock_update_data_warehouse.call_args[1]["updated_feature"].online_enabled is True

    assert deployed_feature_list.online_enabled_feature_ids == deployed_feature_list.feature_ids
    assert isinstance(deployed_feature_list, FeatureListModel)
    await check_states_after_deployed_change(
        feature_service=app_container.feature_service,
        feature_list_namespace_service=app_container.feature_list_namespace_service,
        feature_list_service=app_container.feature_list_service,
        feature_list=deployed_feature_list,
        expected_deployed=True,
        expected_deployed_feature_list_ids=[feature_list.id],
    )

    await app_container.deploy_service.update_deployment(
        deployment_id=deployment_id,
        enabled=False,
        get_credential=Mock(),
    )
    deployed_disabled_feature_list = await app_container.feature_list_service.get_document(
        document_id=deployment.feature_list_id
    )
    assert mock_update_data_warehouse.call_count == 2
    assert mock_update_data_warehouse.call_args[1]["updated_feature"].online_enabled is False

    assert deployed_disabled_feature_list.online_enabled_feature_ids == []
    assert isinstance(deployed_disabled_feature_list, FeatureListModel)
    await check_states_after_deployed_change(
        feature_service=app_container.feature_service,
        feature_list_namespace_service=app_container.feature_list_namespace_service,
        feature_list_service=app_container.feature_list_service,
        feature_list=deployed_disabled_feature_list,
        expected_deployed=False,
        expected_deployed_feature_list_ids=[],
    )

    # update feature list namespace status to deprecated and then deploy the feature list
    # should raise exception
    await app_container.feature_list_status_service.update_feature_list_namespace_status(
        feature_list_namespace_id=feature_list.feature_list_namespace_id,
        target_feature_list_status="DEPRECATED",
    )
    namespace = await app_container.feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert namespace.status == "DEPRECATED"
    with pytest.raises(DocumentUpdateError) as exc:
        await app_container.deploy_service.update_deployment(
            deployment_id=deployment.id,
            enabled=True,
            get_credential=Mock(),
        )

    expected_msg = "Deprecated feature list cannot be deployed."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_update_deployment_error__state_is_reverted_when_update_feature_list_namespace_is_failed(
    app_container,
    disabled_deployment_with_production_ready_features,
    mock_update_data_warehouse,
):
    """Test update feature list exception happens when updating feature list namespace"""
    _ = mock_update_data_warehouse

    with patch.object(
        app_container.deploy_service, "_update_feature_list_namespace"
    ) as mock_update_fl_namespace:
        error_msg = "random error when calling _update_feature_list_namespace!!"
        mock_update_fl_namespace.side_effect = Exception(error_msg)
        with pytest.raises(DocumentError) as exc:
            _ = await app_container.deploy_service.update_deployment(
                deployment_id=disabled_deployment_with_production_ready_features.id,
                enabled=True,
                get_credential=Mock(),
            )

        # check exception message
        assert "Failed to update deployment" in str(exc.value)
        assert error_msg in str(exc.value.__context__)

    # check the mocked method is called once
    assert mock_update_fl_namespace.call_count == 2

    # check feature's online_enabled status & feature list deployed status
    feature_list = await app_container.feature_list_service.get_document(
        document_id=disabled_deployment_with_production_ready_features.feature_list_id
    )
    feature_list_namespace = await app_container.feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert feature_list.deployed is False
    assert feature_list.online_enabled_feature_ids == []
    assert feature_list_namespace.deployed_feature_list_ids == []


@pytest.mark.asyncio
async def test_update_deployment_error__state_is_reverted_when_update_feature_is_failed(
    app_container,
    disabled_deployment_with_production_ready_features,
    mock_update_data_warehouse,
):
    """Test update feature list exception happens when updating feature"""
    _ = mock_update_data_warehouse

    with patch.object(
        app_container.deploy_service, "_update_feature", new_callable=AsyncMock
    ) as mock_update_feature:
        error_msg = "random error when calling _update_feature!!"
        mock_update_feature.side_effect = Exception(error_msg)
        with pytest.raises(DocumentError) as exc:
            _ = await app_container.deploy_service.update_deployment(
                deployment_id=disabled_deployment_with_production_ready_features.id,
                enabled=True,
                get_credential=Mock(),
            )

        # check exception message
        assert "Failed to update deployment" in str(exc.value)
        assert error_msg in str(exc.value.__context__)

    # check the mocked method is called once
    mock_update_feature.assert_called_once()

    # check feature's online_enabled status & feature list deployed status
    feature_list = await app_container.feature_list_service.get_document(
        document_id=disabled_deployment_with_production_ready_features.feature_list_id
    )
    feature_list_namespace = await app_container.feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert feature_list.deployed is False
    assert feature_list.online_enabled_feature_ids == []
    assert feature_list_namespace.deployed_feature_list_ids == []


@pytest.mark.asyncio
async def test_update_feature_list_error__state_is_reverted_after_feature_list_namespace_updated(
    app_container,
    feature_list_namespace_service,
    feature_list_service,
    production_ready_feature_list,
    deploy_service,
    mock_update_data_warehouse,
):
    """Test update feature list exception happens after feature list namespace updated"""
    _ = mock_update_data_warehouse
    feature_list = production_ready_feature_list
    assert feature_list.deployed is False
    assert feature_list.online_enabled_feature_ids == []

    class ReturnDocument:
        """
        ReturnDocument class to trigger error when attempting to convert it to boolean value
        This is to simulate the case when the exception after feature_list_namespace get updated
        """

        def __bool__(self):
            raise ValueError("return_document throws error!!")

    with pytest.raises(ValueError) as exc:
        _ = await deploy_service._update_feature_list(
            feature_list_id=feature_list.id,
            return_document=ReturnDocument(),
            get_credential=AsyncMock(),
        )

    assert "return_document throws error!!" in str(exc.value)

    # check feature's online_enabled status & feature list deployed status
    feature_list = await feature_list_service.get_document(document_id=feature_list.id)
    feature_list_namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert feature_list.deployed is False
    assert feature_list.online_enabled_feature_ids == []
    assert feature_list_namespace.deployed_feature_list_ids == []

    # test another exception raised during revert changes
    # create a enabled deployment first so that feature_list.deployed != target_deployed
    await app_container.deployment_service.create_document(
        data=DeploymentModel(
            name="test deployment",
            feature_list_id=feature_list.id,
            enabled=True,
        )
    )
    with pytest.raises(Exception) as exc:
        with patch.object(
            deploy_service, "_revert_changes", new_callable=AsyncMock
        ) as mock_update_feature:
            mock_update_feature.side_effect = Exception("Error during revert changes")
            _ = await deploy_service._update_feature_list(
                feature_list_id=feature_list.id,
                return_document=ReturnDocument(),
                get_credential=AsyncMock(),
            )

    assert "Error during revert changes" in str(exc.value)
    assert "return_document throws error!!" in str(exc.value.__context__)
