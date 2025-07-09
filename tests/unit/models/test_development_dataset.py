"""
Tests for the DevelopmentDataset model
"""

import pytest
from bson import ObjectId
from pydantic import ValidationError

from featurebyte.models.development_dataset import DevelopmentDatasetModel, DevelopmentTable
from featurebyte.query_graph.model.common_table import TabularSource


def test_empty_development_tables():
    """
    Test that an empty development dataset raises a validation error
    """

    with pytest.raises(ValidationError) as exc_info:
        DevelopmentDatasetModel(
            name="test_dataset",
            sample_from_timestamp="2023-01-01T00:00:00Z",
            sample_to_timestamp="2023-01-02T00:00:00Z",
            development_tables=[],
        )
    assert "At least one development source table is required" in str(exc_info.value)


def test_duplicate_table_ids_development_tables():
    """
    Test that an duplicate table ids indevelopment dataset raises a validation error
    """

    with pytest.raises(ValidationError) as exc_info:
        DevelopmentDatasetModel(
            name="test_dataset",
            sample_from_timestamp="2023-01-01T00:00:00Z",
            sample_to_timestamp="2023-01-02T00:00:00Z",
            development_tables=[
                DevelopmentTable(
                    table_id=ObjectId("686ba610cce016bd2fe62f57"),
                    location=TabularSource(
                        feature_store_id=ObjectId("686ba610cce016bd2fe62f58"),
                        table_details={
                            "database_name": "test_db",
                            "schema_name": "test_schema",
                            "table_name": "test_table_1",
                        },
                    ),
                ),
                DevelopmentTable(
                    table_id=ObjectId("686ba610cce016bd2fe62f57"),
                    location=TabularSource(
                        feature_store_id=ObjectId("686ba610cce016bd2fe62f58"),
                        table_details={
                            "database_name": "test_db",
                            "schema_name": "test_schema",
                            "table_name": "test_table_2",
                        },
                    ),
                ),
            ],
        )
    assert "Duplicate table IDs found in development tables" in str(exc_info.value)
