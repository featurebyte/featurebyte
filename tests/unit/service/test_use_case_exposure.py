"""
Test for UseCase exposure integration (exposure now lives on Context).
"""

from unittest.mock import AsyncMock, patch

import pytest
from bson import ObjectId

from featurebyte.exception import DocumentCreationError
from featurebyte.schema.use_case import UseCaseCreate


def _mock_entity(entity_id, ancestor_ids=None):
    """Create a mock entity with ancestor_ids"""
    entity = AsyncMock()
    entity.id = entity_id
    entity.ancestor_ids = ancestor_ids or []
    return entity


@pytest.mark.asyncio
async def test_use_case_exposure_rejected_for_classification_target(app_container):
    """
    Test that creating a UseCase fails when the context has an exposure but the
    target type is not regression.
    """
    use_case_service = app_container.use_case_service

    shared_entity_id = ObjectId()
    mock_context = AsyncMock()
    mock_context.primary_entity_ids = [shared_entity_id]
    mock_context.treatment_id = None
    mock_context.forecast_point_schema = None
    mock_context.exposure_id = ObjectId()  # context has an exposure

    mock_target_namespace = AsyncMock()
    mock_target_namespace.name = "classification_target"
    mock_target_namespace.target_type = "classification"
    mock_target_namespace.positive_label = "positive"
    mock_target_namespace.entity_ids = [shared_entity_id]
    mock_target_namespace.default_target_id = ObjectId()

    data = UseCaseCreate(
        name="test_use_case_classification",
        target_namespace_id=ObjectId(),
        context_id=ObjectId(),
    )
    data.target_id = mock_target_namespace.default_target_id

    with patch.object(
        use_case_service.context_service, "get_document", new_callable=AsyncMock
    ) as mock_get_ctx:
        mock_get_ctx.return_value = mock_context
        with patch.object(
            use_case_service.target_namespace_service, "get_document", new_callable=AsyncMock
        ) as mock_get_tns:
            mock_get_tns.return_value = mock_target_namespace
            with pytest.raises(
                DocumentCreationError,
                match="Exposure is only supported for regression targets",
            ):
                await use_case_service.create_use_case(data)


@pytest.mark.asyncio
async def test_use_case_validate_entity_compatibility_same_entities(app_container):
    """
    Test _validate_entity_compatibility passes for exact entity match
    """
    use_case_service = app_container.use_case_service
    entity_id = ObjectId()
    await use_case_service._validate_entity_compatibility(
        entity_ids=[entity_id],
        context_entity_ids=[entity_id],
        object_name="Target",
    )


@pytest.mark.asyncio
async def test_use_case_validate_entity_compatibility_parent_entity(app_container):
    """
    Test _validate_entity_compatibility passes when target entity is a parent of context entity
    """
    use_case_service = app_container.use_case_service
    parent_entity_id = ObjectId()
    child_entity_id = ObjectId()

    with patch.object(
        use_case_service.entity_service, "get_document", new_callable=AsyncMock
    ) as mock_get_entity:
        mock_get_entity.return_value = _mock_entity(
            child_entity_id, ancestor_ids=[parent_entity_id]
        )
        await use_case_service._validate_entity_compatibility(
            entity_ids=[parent_entity_id],
            context_entity_ids=[child_entity_id],
            object_name="Target",
        )


@pytest.mark.asyncio
async def test_use_case_validate_entity_compatibility_unrelated_entity(app_container):
    """
    Test _validate_entity_compatibility fails for unrelated entities
    """
    use_case_service = app_container.use_case_service
    entity_a = ObjectId()
    entity_b = ObjectId()

    with patch.object(
        use_case_service.entity_service, "get_document", new_callable=AsyncMock
    ) as mock_get_entity:
        mock_get_entity.return_value = _mock_entity(entity_b, ancestor_ids=[])
        with pytest.raises(DocumentCreationError, match="must be parent entities"):
            await use_case_service._validate_entity_compatibility(
                entity_ids=[entity_a],
                context_entity_ids=[entity_b],
                object_name="Target",
            )
