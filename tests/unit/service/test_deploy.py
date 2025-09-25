"""
Tests for DeployService
"""

import os
import traceback
from unittest.mock import patch

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.enum import DBVarType
from featurebyte.exception import DocumentError, DocumentNotFoundError, DocumentUpdateError
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.worker.task.deployment_create_update import CreateDeploymentPayload


@pytest.fixture(name="mock_warehouse_update_for_deployment", autouse=True)
def mock_warehouse_update_for_deployment_fixture(
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Mocks the warehouse update for deployment
    """
    _ = mock_update_data_warehouse, mock_offline_store_feature_manager_dependencies
    yield


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
        deployment_id=deployment_id,
        payload=CreateDeploymentPayload(
            feature_list_id=feature_list.id,
            enabled=False,
        ),
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
            deployment_id=deployment_id,
            payload=CreateDeploymentPayload(
                feature_list_id=feature_list.id,
                enabled=True,
            ),
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
        deployment_id=deployment_id,
        payload=CreateDeploymentPayload(
            feature_list_id=production_ready_feature_list.id,
            enabled=False,
        ),
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
        deployment_id=deployment_id,
        payload=CreateDeploymentPayload(
            feature_list_id=production_ready_feature_list.id,
            enabled=True,
        ),
    )
    deployment = await deployment_service.get_document(document_id=deployment_id)
    deployed_feature_list = await feature_list_service.get_document(
        document_id=deployment.feature_list_id
    )
    mock_update_data_warehouse.assert_called_once()
    assert mock_update_data_warehouse.call_args[1]["feature"].online_enabled is False
    assert mock_update_data_warehouse.call_args[1]["target_online_enabled"] is True

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
    assert mock_update_data_warehouse.call_args[1]["feature"].online_enabled is True
    assert mock_update_data_warehouse.call_args[1]["target_online_enabled"] is False

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
    except Exception:
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


@pytest_asyncio.fixture(name="production_ready_features")
async def production_ready_features_fixture(app_container, feature, feature_item_event):
    """Production ready features fixture"""
    features = [feature, feature_item_event]
    for feat in features:
        feat = await app_container.feature_readiness_service.update_feature(
            feature_id=feat.id, readiness="PRODUCTION_READY", ignore_guardrails=True
        )
        assert feat.readiness == "PRODUCTION_READY"
        assert feat.online_enabled is False
    return features


@pytest.mark.asyncio
async def test_deploy_service__check_asset_status_update(
    production_ready_features,
    app_container,
    mock_update_data_warehouse,
):
    """Test deploy service"""
    _ = mock_update_data_warehouse

    # create feature lists
    features = production_ready_features
    feature_list_controller = app_container.feature_list_controller
    feature_list1 = await feature_list_controller.create_feature_list(
        data=FeatureListCreate(name="feature_list1", feature_ids=[features[0].id])
    )
    feature_list2 = await feature_list_controller.create_feature_list(
        data=FeatureListCreate(name="feature_list2", feature_ids=[feat.id for feat in features])
    )

    # create deployments
    deployment_id1, deployment_id2, deployment_id3 = ObjectId(), ObjectId(), ObjectId()
    await app_container.deploy_service.create_deployment(
        deployment_id=deployment_id1,
        payload=CreateDeploymentPayload(
            name="deployment1",
            feature_list_id=feature_list1.id,
            enabled=False,
        ),
    )
    await app_container.deploy_service.create_deployment(
        deployment_id=deployment_id2,
        payload=CreateDeploymentPayload(
            name="deployment2",
            feature_list_id=feature_list1.id,
            enabled=False,
        ),
    )
    await app_container.deploy_service.create_deployment(
        deployment_id=deployment_id3,
        payload=CreateDeploymentPayload(
            name="deployment3",
            feature_list_id=feature_list2.id,
            enabled=False,
        ),
    )

    async def check_asset_state(
        deployment_id,
        feats,
        expected_deployment_enabled,
        expected_feature_list_deployed,
        expected_features_online_enabled,
    ):
        """Check asset state"""
        deployment = await app_container.deployment_service.get_document(document_id=deployment_id)
        assert deployment.enabled == expected_deployment_enabled

        flist = await app_container.feature_list_service.get_document(
            document_id=deployment.feature_list_id
        )
        assert flist.deployed == expected_feature_list_deployed

        enabled_flags = []
        for feat in feats:
            feat = await app_container.feature_service.get_document(document_id=feat.id)
            enabled_flags.append(feat.online_enabled)
        assert enabled_flags == expected_features_online_enabled

    # enable deployment1
    await app_container.deploy_service.update_deployment(
        deployment_id=deployment_id1,
        to_enable_deployment=True,
    )
    await check_asset_state(
        deployment_id1,
        features,
        True,
        True,
        [True, False],
    )

    # enable deployment2
    await app_container.deploy_service.update_deployment(
        deployment_id=deployment_id2,
        to_enable_deployment=True,
    )
    await check_asset_state(
        deployment_id2,
        features,
        True,
        True,
        [True, False],
    )

    # enable deployment3
    await app_container.deploy_service.update_deployment(
        deployment_id=deployment_id3,
        to_enable_deployment=True,
    )
    await check_asset_state(
        deployment_id3,
        features,
        True,
        True,
        [True, True],
    )

    # disable deployment1
    await app_container.deploy_service.update_deployment(
        deployment_id=deployment_id1,
        to_enable_deployment=False,
    )
    await check_asset_state(
        deployment_id1,
        features,
        False,
        True,  # feature list is still deployed
        [True, True],  # feature1 is still enabled
    )

    # disable deployment2
    await app_container.deploy_service.update_deployment(
        deployment_id=deployment_id2,
        to_enable_deployment=False,
    )
    await check_asset_state(
        deployment_id2,
        features,
        False,
        False,
        [True, True],  # feature1 is still enabled
    )

    # disable deployment3
    await app_container.deploy_service.update_deployment(
        deployment_id=deployment_id3,
        to_enable_deployment=False,
    )
    await check_asset_state(
        deployment_id3,
        features,
        False,
        False,  # feature list is still deployed
        [False, False],  # feature1 is still enabled
    )


@pytest.mark.asyncio
async def test_deployment_enable__feast_enable_backward_compatibility(
    production_ready_features,
    app_container,
    mock_update_data_warehouse,
):
    """
    Test deploy service handle feast enable backward compatibility.

    When the feature list is deployed without feast integration enabled,
    all the deployments created using the feature list do not have Feast integration enabled.

    When the feature list is deployed with feast integration enabled,
    all the deployments created using the feature list have Feast integration enabled.
    """
    _ = mock_update_data_warehouse

    # create feature lists
    features = production_ready_features
    feature_list_controller = app_container.feature_list_controller
    feature_list1 = await feature_list_controller.create_feature_list(
        data=FeatureListCreate(name="feature_list1", feature_ids=[features[0].id])
    )
    feature_list2 = await feature_list_controller.create_feature_list(
        data=FeatureListCreate(name="feature_list2", feature_ids=[feat.id for feat in features])
    )

    # create deployments
    deployment_id1, deployment_id2, deployment_id3 = ObjectId(), ObjectId(), ObjectId()

    # disable feast integration to simulate old behavior
    with patch.dict(os.environ, {"FEATUREBYTE_FEAST_INTEGRATION_ENABLED": "False"}):
        await app_container.deploy_service.create_deployment(
            deployment_id=deployment_id1,
            payload=CreateDeploymentPayload(
                name="deployment1",
                feature_list_id=feature_list1.id,
                enabled=True,
            ),
        )

    # check deployment is enabled & feature list store info
    deployment1 = await app_container.deployment_service.get_document(document_id=deployment_id1)
    assert deployment1.enabled
    feature_list1 = await app_container.feature_list_service.get_document(
        document_id=feature_list1.id
    )
    assert not feature_list1.feast_enabled

    # create another deployment using the same feature list when feast integration is enabled
    with patch.dict(os.environ, {"FEATUREBYTE_FEAST_INTEGRATION_ENABLED": "False"}):
        await app_container.deploy_service.create_deployment(
            deployment_id=deployment_id2,
            payload=CreateDeploymentPayload(
                name="deployment2",
                feature_list_id=feature_list1.id,
                enabled=True,
            ),
        )

    # check deployment is enabled & feature list store info again
    deployment2 = await app_container.deployment_service.get_document(document_id=deployment_id2)
    assert deployment2.enabled
    feature_list1 = await app_container.feature_list_service.get_document(
        document_id=feature_list1.id
    )
    assert not feature_list1.feast_enabled

    # create another deployment using a new feature list when feast integration is enabled
    with patch.dict(os.environ, {"FEATUREBYTE_FEAST_INTEGRATION_ENABLED": "True"}):
        await app_container.deploy_service.create_deployment(
            deployment_id=deployment_id3,
            payload=CreateDeploymentPayload(
                name="deployment3",
                feature_list_id=feature_list2.id,
                enabled=True,
            ),
        )

    # check deployment is enabled & feature list store info
    deployment3 = await app_container.deployment_service.get_document(document_id=deployment_id3)
    assert deployment3.enabled
    feature_list2 = await app_container.feature_list_service.get_document(
        document_id=feature_list2.id
    )
    assert feature_list2.feast_enabled

    # disable deployment1 & deployment2, check feature list store info
    with patch.dict(os.environ, {"FEATUREBYTE_FEAST_INTEGRATION_ENABLED": "True"}):
        for deployment_id in [deployment_id1, deployment_id2]:
            await app_container.deploy_service.update_deployment(
                deployment_id=deployment_id,
                to_enable_deployment=False,
            )
            deployment = await app_container.deployment_service.get_document(
                document_id=deployment_id
            )
            assert not deployment.enabled

    feature_list = await app_container.feature_list_service.get_document(
        document_id=deployment.feature_list_id
    )
    assert not feature_list.feast_enabled

    # enable deployment1 again & check feature list store info
    with patch.dict(os.environ, {"FEATUREBYTE_FEAST_INTEGRATION_ENABLED": "True"}):
        await app_container.deploy_service.update_deployment(
            deployment_id=deployment_id1,
            to_enable_deployment=True,
        )

    deployment = await app_container.deployment_service.get_document(document_id=deployment_id1)
    assert deployment.enabled

    feature_list = await app_container.feature_list_service.get_document(
        document_id=deployment.feature_list_id
    )
    assert feature_list.feast_enabled


@pytest.mark.asyncio
async def test_get_serving_entity_specs(
    app_container,
    deployed_feature_list_requiring_parent_serving_composite_entity,
    group_entity,
    another_entity,
):
    """Test get serving entity specs"""
    feature_list = deployed_feature_list_requiring_parent_serving_composite_entity
    serving_entity_ids = feature_list.primary_entity_ids
    serving_entity_specs = (
        await app_container.deployment_serving_entity_service.get_serving_entity_specs(
            serving_entity_ids=serving_entity_ids
        )
    )
    expected = {
        group_entity.id: ColumnSpec(name=group_entity.serving_names[0], dtype=DBVarType.INT),
        another_entity.id: ColumnSpec(name=another_entity.serving_names[0], dtype=DBVarType.BINARY),
    }
    assert serving_entity_specs == [expected[entity_id] for entity_id in serving_entity_ids]
