"""
Tests for TreatmentService
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.enum import DBVarType, TreatmentType
from featurebyte.exception import DocumentUpdateError
from featurebyte.models.treatment import AssignmentDesign, AssignmentSource, TreatmentModel


@pytest_asyncio.fixture
async def treatment_service(app_container):
    """Fixture for TreatmentService"""
    return app_container.treatment_service


@pytest_asyncio.fixture
async def binary_treatment(treatment_service):
    """Fixture for a binary treatment with control label"""
    treatment = TreatmentModel(
        _id=ObjectId(),
        name="test_binary_treatment",
        dtype=DBVarType.VARCHAR,
        treatment_type=TreatmentType.BINARY,
        source=AssignmentSource.RANDOMIZED,
        design=AssignmentDesign.RANDOMIZED,
        treatment_labels=["control", "campaign"],
        control_label="control",
        user_id=ObjectId(),
        catalog_id=ObjectId(),
    )
    # Mock the service to return this treatment
    return treatment


@pytest.mark.asyncio
async def test_treatment_labels_not_found(treatment_service, binary_treatment):
    """
    Test that validate_treatment_labels raises DocumentUpdateError when
    treatment labels are not found in treatment values
    """
    # Create a mock observation table - we just need the id attribute
    observation_table = MagicMock()
    observation_table.id = ObjectId()

    # Create a mock database session
    db_session = MagicMock()

    # Mock get_document to return the binary treatment
    with patch.object(
        treatment_service, "get_document", new_callable=AsyncMock
    ) as mock_get_document:
        mock_get_document.return_value = binary_treatment

        # Mock _get_unique_treatment_values to return values that don't include the treatment labels
        with patch.object(
            treatment_service, "_get_unique_treatment_values", new_callable=AsyncMock
        ) as mock_get_unique:
            mock_get_unique.return_value = ["negative", "neutral", "unknown"]

            # Call the method and expect it to raise DocumentUpdateError
            with pytest.raises(DocumentUpdateError) as exc_info:
                await treatment_service.update_treatment_binary_metadata(
                    treatment_id=binary_treatment.id,
                    observation_table=observation_table,
                    db_session=db_session,
                )

            # Verify the error message
            assert (
                "Treatment labels ['control', 'campaign' are different from treatment values"
                in str(exc_info.value)
            )
            assert "['negative', 'neutral', 'unknown']" in str(exc_info.value)
