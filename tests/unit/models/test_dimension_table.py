"""
Tests for DimensionTable models
"""

import datetime

import pytest
from pydantic import ValidationError

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.models import DimensionTableModel
from featurebyte.models.feature_store import TableStatus
from featurebyte.query_graph.node.schema import TableDetails

arbitrary_test_date_time = datetime.datetime(2022, 2, 1)


@pytest.fixture(name="dimension_columns_info")
def get_dimension_columns_info():
    """Fixture to get a some dimension table columns info"""
    return [
        {
            "name": "col",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
            "partition_metadata": None,
        },
        {
            "name": "dimension_id",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
            "partition_metadata": None,
        },
        {
            "name": "created_at",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
            "partition_metadata": None,
        },
    ]


@pytest.fixture(name="dimension_table_model")
def get_dimension_table_model_fixture(snowflake_feature_store, dimension_columns_info):
    """Fixture to get a base dimension table model"""
    return DimensionTableModel(
        name="my_dimension_table",
        tabular_source={
            "feature_store_id": snowflake_feature_store.id,
            "table_details": TableDetails(
                database_name="database", schema_name="schema", table_name="table"
            ),
        },
        columns_info=dimension_columns_info,
        record_creation_timestamp_column="created_at",
        created_at=arbitrary_test_date_time,
        status=TableStatus.PUBLISHED,
        dimension_id_column="dimension_id",
    )


@pytest.fixture(name="expected_dimension_table_model")
def get_base_expected_dimension_table_model(dimension_table_model, dimension_columns_info):
    """Fixture to get a base expected dimension table JSON"""
    return {
        "type": "dimension_table",
        "user_id": None,
        "created_at": arbitrary_test_date_time,
        "updated_at": None,
        "columns_info": dimension_columns_info,
        "id": dimension_table_model.id,
        "name": "my_dimension_table",
        "record_creation_timestamp_column": "created_at",
        "status": "PUBLISHED",
        "tabular_source": {
            "feature_store_id": dimension_table_model.tabular_source.feature_store_id,
            "table_details": {
                "database_name": "database",
                "schema_name": "schema",
                "table_name": "table",
            },
        },
        "dimension_id_column": "dimension_id",
        "catalog_id": DEFAULT_CATALOG_ID,
        "block_modification_by": [],
        "description": None,
        "is_deleted": False,
        "managed_view_id": None,
        "validation": None,
    }


def test_dimension_table_model(dimension_table_model, expected_dimension_table_model):
    """Test creation, serialization and deserialization of DimensionTable"""
    assert dimension_table_model.model_dump() == expected_dimension_table_model
    dimension_table_json = dimension_table_model.model_dump_json(by_alias=True)
    dimension_table_loaded = DimensionTableModel.model_validate_json(dimension_table_json)
    assert dimension_table_loaded == dimension_table_model


def test_missing_dimension_table_id_column_errors(expected_dimension_table_model):
    """Test missing column validation on dimension table id column"""
    # Remove the "dimension_table_id_column" so that we can test the missing column validation
    expected_dimension_table_model.pop("dimension_id_column")
    with pytest.raises(ValidationError) as exc_info:
        DimensionTableModel.model_validate(expected_dimension_table_model)

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["msg"] == "Field required"
    assert errors[0]["type"] == "missing"


def test_incorrect_dimension_table_id_type_errors(expected_dimension_table_model):
    """Test type validation on dimension table id column"""
    # Update type to non str
    expected_dimension_table_model["dimension_id_column"] = arbitrary_test_date_time
    with pytest.raises(ValidationError) as exc_info:
        DimensionTableModel.model_validate(expected_dimension_table_model)

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["msg"] == "Input should be a valid string"
    assert errors[0]["type"] == "string_type"
