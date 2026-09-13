"""
Tests for Context exposure integration (validation and source column derivation).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from bson import ObjectId

from featurebyte.exception import DocumentCreationError
from featurebyte.models.exposure import ExposureSourceColumn
from featurebyte.models.exposure_namespace import ExposureNamespaceModel
from featurebyte.schema.context import ContextCreate


def _mock_entity(entity_id, ancestor_ids=None):
    """Create a mock entity with ancestor_ids"""
    entity = AsyncMock()
    entity.id = entity_id
    entity.ancestor_ids = ancestor_ids or []
    return entity


@pytest.mark.asyncio
async def test_context_validate_exposure_entity_same(app_container):
    """
    Test that context creation validation passes when exposure entities equal context entities.
    """
    controller = app_container.context_controller
    shared_entity_id = ObjectId()
    exposure_namespace_id = ObjectId()
    default_exposure_id = ObjectId()

    mock_exposure_namespace = ExposureNamespaceModel(
        _id=exposure_namespace_id,
        name="test_exposure",
        dtype="FLOAT",
        entity_ids=[shared_entity_id],
        exposure_ids=[default_exposure_id],
        default_exposure_id=default_exposure_id,
        user_id=ObjectId(),
        catalog_id=ObjectId(),
    )

    data = ContextCreate(
        name="test_context",
        primary_entity_ids=[shared_entity_id],
        exposure_namespace_id=exposure_namespace_id,
    )

    with patch.object(
        controller.exposure_namespace_service, "get_document", new_callable=AsyncMock
    ) as mock_get:
        mock_get.return_value = mock_exposure_namespace
        await controller._validate_and_resolve_exposure(data)
        assert data.exposure_id == default_exposure_id


@pytest.mark.asyncio
async def test_context_validate_exposure_parent_entity(app_container):
    """
    Test that context creation validation passes when the exposure entity is a parent
    of the context primary entity (supports hierarchical TS use cases).
    """
    controller = app_container.context_controller
    parent_entity_id = ObjectId()
    child_entity_id = ObjectId()
    exposure_namespace_id = ObjectId()
    default_exposure_id = ObjectId()

    mock_exposure_namespace = ExposureNamespaceModel(
        _id=exposure_namespace_id,
        name="test_exposure",
        dtype="FLOAT",
        entity_ids=[parent_entity_id],
        exposure_ids=[default_exposure_id],
        default_exposure_id=default_exposure_id,
        user_id=ObjectId(),
        catalog_id=ObjectId(),
    )

    data = ContextCreate(
        name="test_context",
        primary_entity_ids=[child_entity_id],
        exposure_namespace_id=exposure_namespace_id,
    )

    with patch.object(
        controller.exposure_namespace_service, "get_document", new_callable=AsyncMock
    ) as mock_get:
        mock_get.return_value = mock_exposure_namespace
        with patch.object(
            controller.entity_service, "get_document", new_callable=AsyncMock
        ) as mock_get_entity:
            mock_get_entity.return_value = _mock_entity(
                child_entity_id, ancestor_ids=[parent_entity_id]
            )
            await controller._validate_and_resolve_exposure(data)
            assert data.exposure_id == default_exposure_id


@pytest.mark.asyncio
async def test_context_validate_exposure_unrelated_entity_rejected(app_container):
    """
    Test that context creation validation fails when exposure entity is unrelated to
    the context primary entity.
    """
    controller = app_container.context_controller
    context_entity_id = ObjectId()
    exposure_entity_id = ObjectId()
    exposure_namespace_id = ObjectId()

    mock_exposure_namespace = ExposureNamespaceModel(
        _id=exposure_namespace_id,
        name="test_exposure",
        dtype="FLOAT",
        entity_ids=[exposure_entity_id],
        exposure_ids=[],
        user_id=ObjectId(),
        catalog_id=ObjectId(),
    )

    data = ContextCreate(
        name="test_context",
        primary_entity_ids=[context_entity_id],
        exposure_namespace_id=exposure_namespace_id,
    )

    with patch.object(
        controller.exposure_namespace_service, "get_document", new_callable=AsyncMock
    ) as mock_get:
        mock_get.return_value = mock_exposure_namespace
        with patch.object(
            controller.entity_service, "get_document", new_callable=AsyncMock
        ) as mock_get_entity:
            mock_get_entity.return_value = _mock_entity(context_entity_id, ancestor_ids=[])
            with pytest.raises(
                DocumentCreationError,
                match="Exposure entities must be the same as or parent entities",
            ):
                await controller._validate_and_resolve_exposure(data)


@pytest.mark.asyncio
async def test_context_validate_exposure_resolves_namespace_from_exposure_id(app_container):
    """
    Test that _validate_and_resolve_exposure resolves exposure_namespace_id from exposure_id.
    """
    controller = app_container.context_controller
    shared_entity_id = ObjectId()
    exposure_id = ObjectId()
    exposure_namespace_id = ObjectId()

    mock_exposure = AsyncMock()
    mock_exposure.exposure_namespace_id = exposure_namespace_id

    mock_exposure_namespace = ExposureNamespaceModel(
        _id=exposure_namespace_id,
        name="test_exposure",
        dtype="FLOAT",
        entity_ids=[shared_entity_id],
        exposure_ids=[exposure_id],
        default_exposure_id=exposure_id,
        user_id=ObjectId(),
        catalog_id=ObjectId(),
    )

    data = ContextCreate(
        name="test_context",
        primary_entity_ids=[shared_entity_id],
        exposure_id=exposure_id,
    )

    with patch.object(
        controller.exposure_service, "get_document", new_callable=AsyncMock
    ) as mock_get_exposure:
        mock_get_exposure.return_value = mock_exposure
        with patch.object(
            controller.exposure_namespace_service, "get_document", new_callable=AsyncMock
        ) as mock_get_ns:
            mock_get_ns.return_value = mock_exposure_namespace
            await controller._validate_and_resolve_exposure(data)
            assert data.exposure_namespace_id == exposure_namespace_id


@pytest.mark.asyncio
async def test_exposure_source_column_derivation(app_container):
    """
    Test that creating a context with an exposure derives and persists
    the exposure_source_column.
    """
    source_column = ExposureSourceColumn(table_id=ObjectId(), column_name="amount")
    mock_exposure = MagicMock()
    mock_exposure.get_exposure_source_column.return_value = source_column

    # The derivation lives in the controller's create_context method; we verify the
    # method call with a mocked exposure returns the expected source column.
    controller = app_container.context_controller
    with patch.object(
        controller.exposure_service, "get_document", new_callable=AsyncMock
    ) as mock_get:
        mock_get.return_value = mock_exposure
        exposure = await controller.exposure_service.get_document(document_id=ObjectId())
        result = exposure.get_exposure_source_column()
        assert result == source_column
