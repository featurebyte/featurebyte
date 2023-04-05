"""
Tests for DeployService
"""
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature_list import FeatureListModel


@pytest.mark.asyncio
async def test_update_feature_list__feature_not_online_enabled_error(deploy_service, feature_list):
    """Test update feature list (not all features are online enabled validation error)"""
    with pytest.raises(DocumentUpdateError) as exc:
        await deploy_service.update_feature_list(
            feature_list_id=feature_list.id, deployed=True, get_credential=Mock()
        )
    expected_msg = "Only FeatureList object of all production ready features can be deployed."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_update_feature_list__no_update(deploy_service, feature_list):
    """Test update feature list when deployed status is the same"""
    updated_feature_list = await deploy_service.update_feature_list(
        feature_list_id=feature_list.id,
        deployed=feature_list.deployed,
        get_credential=Mock(),
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


@pytest_asyncio.fixture(name="production_ready_feature_list")
async def production_ready_feature_list_fixture(feature_list, feature_readiness_service):
    """Feature list consists of production ready features"""
    for feature_id in feature_list.feature_ids:
        await feature_readiness_service.update_feature(
            feature_id=feature_id, readiness="PRODUCTION_READY"
        )
    yield feature_list


@pytest.mark.asyncio
async def test_update_feature_list(
    feature_service,
    feature_list_namespace_service,
    feature_list_service,
    production_ready_feature_list,
    deploy_service,
    mock_update_data_warehouse,
):
    """Test update feature list"""
    feature_list = production_ready_feature_list
    assert feature_list.online_enabled_feature_ids == []
    deployed_feature_list = await deploy_service.update_feature_list(
        feature_list_id=feature_list.id,
        deployed=True,
        return_document=True,
        get_credential=Mock(),
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

    deployed_disabled_feature_list = await deploy_service.update_feature_list(
        feature_list_id=feature_list.id,
        deployed=False,
        return_document=True,
        get_credential=Mock(),
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


@pytest.mark.asyncio
async def test_update_feature_list_error__state_is_reverted_when_update_feature_list_namespace_is_failed(
    feature_list_namespace_service,
    feature_list_service,
    production_ready_feature_list,
    deploy_service,
    mock_update_data_warehouse,
):
    """Test update feature list exception happens when updating feature list namespace"""
    _ = mock_update_data_warehouse
    feature_list = production_ready_feature_list
    assert feature_list.online_enabled_feature_ids == []

    with patch.object(deploy_service, "_update_feature_list_namespace") as mock_update_fl_namespace:
        error_msg = "random error when calling _update_feature_list_namespace!!"
        mock_update_fl_namespace.side_effect = Exception(error_msg)
        with pytest.raises(Exception) as exc:
            _ = await deploy_service.update_feature_list(
                feature_list_id=feature_list.id,
                deployed=True,
                return_document=True,
                get_credential=Mock(),
            )

        # check exception message
        assert error_msg in str(exc.value)

    # check the mocked method is called once
    assert mock_update_fl_namespace.call_count == 2

    # check feature's online_enabled status & feature list deployed status
    feature_list = await feature_list_service.get_document(document_id=feature_list.id)
    feature_list_namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert feature_list.deployed is False
    assert feature_list.online_enabled_feature_ids == []
    assert feature_list_namespace.deployed_feature_list_ids == []


@pytest.mark.asyncio
async def test_update_feature_list_error__state_is_reverted_when_update_feature_is_failed(
    feature_list_namespace_service,
    feature_list_service,
    production_ready_feature_list,
    deploy_service,
    mock_update_data_warehouse,
):
    """Test update feature list exception happens when updating feature"""
    _ = mock_update_data_warehouse
    feature_list = production_ready_feature_list
    assert feature_list.deployed is False
    assert feature_list.online_enabled_feature_ids == []

    with patch.object(
        deploy_service, "_update_feature", new_callable=AsyncMock
    ) as mock_update_feature:
        error_msg = "random error when calling _update_feature!!"
        mock_update_feature.side_effect = Exception(error_msg)
        with pytest.raises(Exception) as exc:
            _ = await deploy_service.update_feature_list(
                feature_list_id=feature_list.id,
                deployed=True,
                return_document=True,
                get_credential=AsyncMock(),
            )

        # check exception message
        assert error_msg in str(exc.value)

    # check the mocked method is called once
    mock_update_feature.assert_called_once()

    # check feature's online_enabled status & feature list deployed status
    feature_list = await feature_list_service.get_document(document_id=feature_list.id)
    feature_list_namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert feature_list.deployed is False
    assert feature_list.online_enabled_feature_ids == []
    assert feature_list_namespace.deployed_feature_list_ids == []


@pytest.mark.asyncio
async def test_update_feature_list_error__state_is_reverted_after_feature_list_namespace_updated(
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
        _ = await deploy_service.update_feature_list(
            feature_list_id=feature_list.id,
            deployed=True,
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
    with pytest.raises(Exception) as exc:
        with patch.object(
            deploy_service, "_revert_changes", new_callable=AsyncMock
        ) as mock_update_feature:
            mock_update_feature.side_effect = Exception("Error during revert changes")
            _ = await deploy_service.update_feature_list(
                feature_list_id=feature_list.id,
                deployed=True,
                return_document=ReturnDocument(),
                get_credential=AsyncMock(),
            )

    assert "Error during revert changes" in str(exc.value)
    assert "return_document throws error!!" in str(exc.value.__context__)
