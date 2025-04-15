"""
Tests for SCDTable models
"""

import datetime

import pytest
from pydantic import ValidationError

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.models.feature_store import TableStatus
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.node.schema import TableDetails

arbitrary_test_date_time = datetime.datetime(2022, 2, 1)


@pytest.fixture(name="scd_columns_info")
def get_scd_columns_info():
    """Fixture to get a some SCDTable columns info"""
    return [
        {
            "name": "col",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
        },
        {
            "name": "natural_id",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
        },
        {
            "name": "surrogate_id",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
        },
        {
            "name": "created_at",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
        },
        {
            "name": "effective_at",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
        },
        {
            "name": "end_at",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
        },
        {
            "name": "enabled",
            "dtype": "BOOL",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
            "description": None,
            "dtype_metadata": None,
        },
    ]


@pytest.fixture(name="scd_table_model")
def get_scd_table_model_fixture(snowflake_feature_store, scd_columns_info):
    """Fixture to get a base SCDTable model"""
    return SCDTableModel(
        name="my_scd_table",
        tabular_source={
            "feature_store_id": snowflake_feature_store.id,
            "table_details": TableDetails(
                database_name="database", schema_name="schema", table_name="table"
            ),
        },
        columns_info=scd_columns_info,
        record_creation_timestamp_column="created_at",
        created_at=arbitrary_test_date_time,
        status=TableStatus.PUBLISHED,
        natural_key_column="natural_id",
        surrogate_key_column="surrogate_id",
        effective_timestamp_column="effective_at",
        end_timestamp_column="end_at",
        current_flag_column="enabled",
    )


@pytest.fixture(name="expected_scd_table_model")
def get_base_expected_scd_table_model(scd_table_model, scd_columns_info):
    """Fixture to get a base expected SCDTable JSON"""
    return {
        "type": "scd_table",
        "user_id": None,
        "created_at": arbitrary_test_date_time,
        "updated_at": None,
        "columns_info": scd_columns_info,
        "id": scd_table_model.id,
        "name": "my_scd_table",
        "record_creation_timestamp_column": "created_at",
        "status": "PUBLISHED",
        "tabular_source": {
            "feature_store_id": scd_table_model.tabular_source.feature_store_id,
            "table_details": {
                "database_name": "database",
                "schema_name": "schema",
                "table_name": "table",
            },
        },
        "natural_key_column": "natural_id",
        "surrogate_key_column": "surrogate_id",
        "effective_timestamp_column": "effective_at",
        "end_timestamp_column": "end_at",
        "current_flag_column": "enabled",
        "default_feature_job_setting": None,
        "catalog_id": DEFAULT_CATALOG_ID,
        "block_modification_by": [],
        "description": None,
        "is_deleted": False,
        "validation": None,
        "effective_timestamp_schema": None,
        "end_timestamp_schema": None,
    }


def test_scd_table_model(scd_table_model, expected_scd_table_model):
    """Test creation, serialization and deserialization of SCDTable"""
    # rename current_flag to current_flag_column (check the model handle it properly)
    assert scd_table_model.model_dump() == expected_scd_table_model
    scd_table_json = scd_table_model.model_dump_json(by_alias=True)
    scd_table_loaded = SCDTableModel.model_validate_json(scd_table_json)
    assert scd_table_loaded == scd_table_model


@pytest.mark.parametrize("column", ["natural_key_column"])
def test_missing_scd_table_id_column_errors(expected_scd_table_model, column):
    """Test missing column validation on SCDTable models"""
    # Remove the `column` so that we can test the missing column validation
    expected_scd_table_model.pop(column)
    with pytest.raises(ValidationError) as exc_info:
        SCDTableModel.model_validate(expected_scd_table_model)

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["msg"] == "Field required"
    assert errors[0]["type"] == "missing"


def test_incorrect_scd_table_id_type_errors(expected_scd_table_model):
    """Test type validation on SCDTable id column"""
    # Update type to non str
    expected_scd_table_model["natural_key_column"] = arbitrary_test_date_time
    with pytest.raises(ValidationError) as exc_info:
        SCDTableModel.model_validate(expected_scd_table_model)

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["msg"] == "Input should be a valid string"
    assert errors[0]["type"] == "string_type"


def test_column_cleaning_operation_with_missing_imputed_value(scd_table_model):
    """Test column cleaning operation with missing imputed value"""
    data_dict = scd_table_model.model_dump(by_alias=True)
    data_dict["columns_info"][0]["critical_data_info"] = {
        "cleaning_operations": [{"type": "disguised", "disguised_values": [9999]}]
    }

    # deserialize and check the cleaning operation
    scd_table_loaded = SCDTableModel(**data_dict)
    cleaning_operation = scd_table_loaded.columns_info[0].critical_data_info.cleaning_operations[0]
    assert cleaning_operation.disguised_values == [9999]
    assert cleaning_operation.imputed_value is None
