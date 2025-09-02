"""
Tests for the DevelopmentDataset model
"""

import pytest
from bson import ObjectId
from pydantic import ValidationError

from featurebyte.models.development_dataset import (
    DevelopmentDatasetModel,
    DevelopmentDatasetSourceType,
    DevelopmentDatasetStatus,
    DevelopmentTable,
)
from featurebyte.query_graph.model.common_table import TabularSource


def test_empty_development_tables():
    """
    Test that an empty development dataset can't have an active status
    """

    empty_dev = DevelopmentDatasetModel(
        name="test_dataset",
        sample_from_timestamp="2023-01-01T00:00:00Z",
        sample_to_timestamp="2023-01-02T00:00:00Z",
        development_tables=[],
    )
    assert empty_dev.status == DevelopmentDatasetStatus.EMPTY


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


def _one_table():
    return [
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
        )
    ]


def test_source_tables_requires_ids_null_raises():
    """
    If source_type is SOURCE_TABLES and either/both IDs are set, validation should fail.
    """
    with pytest.raises(ValidationError) as exc:
        DevelopmentDatasetModel(
            name="test_dataset",
            sample_from_timestamp="2023-01-01T00:00:00Z",
            sample_to_timestamp="2023-01-02T00:00:00Z",
            source_type=DevelopmentDatasetSourceType.SOURCE_TABLES,
            development_tables=_one_table(),
            development_plan_id=ObjectId("686ba610cce016bd2fe62f99"),
            observation_table_id=ObjectId("686ba610cce016bd2fe62faa"),
        )
    assert "should be null" in str(exc.value)


def test_source_tables_with_both_ids_none_is_valid():
    """
    If source_type is SOURCE_TABLES, both IDs must be None — this should pass.
    """
    obj = DevelopmentDatasetModel(
        name="test_dataset",
        sample_from_timestamp="2023-01-01T00:00:00Z",
        sample_to_timestamp="2023-01-02T00:00:00Z",
        source_type=DevelopmentDatasetSourceType.SOURCE_TABLES,
        development_tables=_one_table(),
        development_plan_id=None,
        observation_table_id=None,
    )
    assert obj.development_plan_id is None
    assert obj.observation_table_id is None


def test_observation_table_missing_ids_raises():
    """
    If source_type is OBSERVATION_TABLE and either ID is missing, validation should fail.
    """
    with pytest.raises(ValidationError) as exc:
        DevelopmentDatasetModel(
            name="test_dataset",
            sample_from_timestamp="2023-01-01T00:00:00Z",
            sample_to_timestamp="2023-01-02T00:00:00Z",
            source_type=DevelopmentDatasetSourceType.OBSERVATION_TABLE,
            development_tables=_one_table(),
            # omit IDs on purpose
        )
    assert "must be specified" in str(exc.value)


def test_observation_table_with_both_ids_valid():
    """
    If source_type is OBSERVATION_TABLE and both IDs are provided, validation should pass.
    """
    obj = DevelopmentDatasetModel(
        name="test_dataset",
        sample_from_timestamp="2023-01-01T00:00:00Z",
        sample_to_timestamp="2023-01-02T00:00:00Z",
        source_type=DevelopmentDatasetSourceType.OBSERVATION_TABLE,
        development_tables=_one_table(),
        development_plan_id=ObjectId("686ba610cce016bd2fe62f99"),
        observation_table_id=ObjectId("686ba610cce016bd2fe62faa"),
    )
    assert obj.development_plan_id is not None
    assert obj.observation_table_id is not None
