"""
Tests for TargetNamespaceService
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.enum import DBVarType, TargetType
from featurebyte.exception import DocumentUpdateError
from featurebyte.models.target_namespace import TargetNamespaceModel


@pytest_asyncio.fixture
async def target_namespace_service(app_container):
    """Fixture for TargetNamespaceService"""
    return app_container.target_namespace_service


@pytest_asyncio.fixture
async def classification_target_namespace(target_namespace_service):
    """Fixture for a classification target namespace with positive label"""
    target_namespace = TargetNamespaceModel(
        _id=ObjectId(),
        name="test_classification_target",
        dtype=DBVarType.VARCHAR,
        entity_ids=[ObjectId()],
        target_type=TargetType.CLASSIFICATION,
        positive_label="positive",
        target_ids=[],  # Required field
        user_id=ObjectId(),
        catalog_id=ObjectId(),
    )
    # Mock the service to return this target namespace
    return target_namespace


@pytest.mark.asyncio
async def test_update_positive_label_candidates_positive_label_not_found(
    target_namespace_service, classification_target_namespace
):
    """
    Test that update_target_namespace_classification_metadata raises DocumentUpdateError when
    positive label is not found in target values
    """
    # Create a mock observation table - we just need the id attribute
    observation_table = MagicMock()
    observation_table.id = ObjectId()

    # Create a mock database session
    db_session = MagicMock()

    # Mock get_document to return the classification target namespace
    with patch.object(
        target_namespace_service, "get_document", new_callable=AsyncMock
    ) as mock_get_document:
        mock_get_document.return_value = classification_target_namespace

        # Mock _get_unique_target_values to return values that don't include the positive label
        with patch.object(
            target_namespace_service, "_get_unique_target_values", new_callable=AsyncMock
        ) as mock_get_unique:
            # Simulate target values that don't contain "positive"
            mock_get_unique.return_value = ["negative", "neutral", "unknown"]

            # Call the method and expect it to raise DocumentUpdateError
            with pytest.raises(DocumentUpdateError) as exc_info:
                await target_namespace_service.update_target_namespace_classification_metadata(
                    target_namespace_id=classification_target_namespace.id,
                    observation_table=observation_table,
                    db_session=db_session,
                )

            # Verify the error message
            assert "Positive label positive not found in target values" in str(exc_info.value)
            assert "['negative', 'neutral', 'unknown']" in str(exc_info.value)
