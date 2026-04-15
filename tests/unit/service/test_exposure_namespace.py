"""
Test for ExposureNamespaceService
"""

import pytest
from bson import ObjectId

from featurebyte.models.exposure_namespace import ExposureNamespaceModel
from featurebyte.schema.exposure_namespace import ExposureNamespaceCreate


@pytest.mark.asyncio
async def test_exposure_namespace_service_create(app_container):
    """
    Test creating an exposure namespace via the service
    """
    exposure_namespace_service = app_container.exposure_namespace_service
    entity_id = ObjectId()
    data = ExposureNamespaceCreate(
        name="test_exposure_ns",
        dtype="FLOAT",
        entity_ids=[entity_id],
        exposure_ids=[],
        window="7d",
    )
    result = await exposure_namespace_service.create_document(data)
    assert isinstance(result, ExposureNamespaceModel)
    assert result.name == "test_exposure_ns"
    assert result.dtype == "FLOAT"
    assert result.window == "7d"


@pytest.mark.asyncio
async def test_exposure_namespace_service_get(app_container):
    """
    Test getting an exposure namespace by ID
    """
    exposure_namespace_service = app_container.exposure_namespace_service
    data = ExposureNamespaceCreate(
        name="test_exposure_ns_get",
        dtype="INT",
        entity_ids=[ObjectId()],
        exposure_ids=[],
    )
    created = await exposure_namespace_service.create_document(data)
    retrieved = await exposure_namespace_service.get_document(document_id=created.id)
    assert retrieved.id == created.id
    assert retrieved.name == "test_exposure_ns_get"
    assert retrieved.dtype == "INT"
    assert retrieved.window is None
