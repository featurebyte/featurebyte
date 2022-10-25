"""
Tests for BaseService class
"""
import pytest

from featurebyte.service.base_service import BaseService, DocServiceName


@pytest.fixture(name="base_update_service")
def base_update_service_fixture(user, persistent):
    """BaseUpdateService fixture"""
    return BaseService(user=user, persistent=persistent)


@pytest.mark.asyncio
async def test_get_document(base_update_service, feature):
    """Test get_document method"""
    # check that actual retrieval happens when document is None
    document = await base_update_service.get_document(
        DocServiceName.FEATURE, feature.id, document=None
    )
    assert document == feature

    # check non-retrieval happens when document is not None
    document = await base_update_service.get_document(
        DocServiceName.FEATURE, feature.id, document=True
    )
    assert document is True
