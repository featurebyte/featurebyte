"""
Tests for BaseUpdateService class
"""
import pytest

from featurebyte.models.feature import FeatureNamespaceModel
from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.service.base_update import BaseUpdateService


@pytest.fixture(name="base_update_service")
def base_update_service_fixture(user, persistent):
    """BaseUpdateService fixture"""
    return BaseUpdateService(user=user, persistent=persistent)


@pytest.mark.asyncio
async def test_get_feature_document(base_update_service, feature):
    """Test get_feature_document method"""
    # check that actual retrieval happens when document is None
    document = await base_update_service.get_feature_document(document_id=feature.id, document=None)
    assert document == feature

    # check non-retrieval happens when document is not None
    document = await base_update_service.get_feature_document(document_id=feature.id, document=True)
    assert document is True


@pytest.mark.asyncio
async def test_get_feature_namespace_document(base_update_service, feature):
    """Test get_feature_namespace_document method"""
    # check that actual retrieval happens when document is None
    document = await base_update_service.get_feature_namespace_document(
        document_id=feature.feature_namespace_id, document=None
    )
    assert isinstance(document, FeatureNamespaceModel)

    # check non-retrieval happens when document is not None
    document = await base_update_service.get_feature_namespace_document(
        document_id=feature.id, document=True
    )
    assert document is True


@pytest.mark.asyncio
async def test_get_feature_list_document(base_update_service, feature_list):
    """Test get_feature_list_document method"""
    # check that actual retrieval happens when document is None
    document = await base_update_service.get_feature_list_document(
        document_id=feature_list.id, document=None
    )
    assert document == feature_list

    # check non-retrieval happens when document is not None
    document = await base_update_service.get_feature_list_document(
        document_id=feature_list.id, document=True
    )
    assert document is True


@pytest.mark.asyncio
async def test_get_feature_list_namespace_document(base_update_service, feature_list):
    """Test get_feature_list_namespace_document method"""
    # check that actual retrieval happens when document is None
    document = await base_update_service.get_feature_list_namespace_document(
        document_id=feature_list.feature_list_namespace_id, document=None
    )
    assert isinstance(document, FeatureListNamespaceModel)

    # check non-retrieval happens when document is not None
    document = await base_update_service.get_feature_list_namespace_document(
        document_id=feature_list.feature_list_namespace_id, document=True
    )
    assert document is True
