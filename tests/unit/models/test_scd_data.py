"""
Tests for SCDData models
"""
import datetime

import pytest
from _pytest._code import ExceptionInfo
from pydantic.error_wrappers import ValidationError

from featurebyte.models.feature_store import DataStatus
from featurebyte.models.scd_data import SCDDataModel
from featurebyte.query_graph.node.schema import TableDetails

arbitrary_test_date_time = datetime.datetime(2022, 2, 1)


@pytest.fixture(name="scd_columns_info")
def get_scd_columns_info():
    """Fixture to get a some SCD data columns info"""
    return [
        {
            "name": "col",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "natural_id",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "surrogate_id",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "created_at",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "effective_at",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "end_at",
            "dtype": "TIMESTAMP",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "enabled",
            "dtype": "BOOL",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
    ]


@pytest.fixture(name="scd_data_model")
def get_scd_data_model_fixture(snowflake_feature_store, scd_columns_info):
    """Fixture to get a base SCD data model"""
    return SCDDataModel(
        name="my_scd_data",
        tabular_source={
            "feature_store_id": snowflake_feature_store.id,
            "table_details": TableDetails(
                database_name="database", schema_name="schema", table_name="table"
            ),
        },
        columns_info=scd_columns_info,
        record_creation_date_column="created_at",
        created_at=arbitrary_test_date_time,
        status=DataStatus.PUBLISHED,
        natural_key_column="natural_id",
        surrogate_key_column="surrogate_id",
        effective_timestamp_column="effective_at",
        end_timestamp_column="end_at",
        current_flag_column="enabled",
    )


@pytest.fixture(name="expected_scd_data_model")
def get_base_expected_scd_data_model(scd_data_model, scd_columns_info):
    """Fixture to get a base expected SCD data JSON"""
    return {
        "type": "scd_data",
        "user_id": None,
        "created_at": arbitrary_test_date_time,
        "updated_at": None,
        "columns_info": scd_columns_info,
        "id": scd_data_model.id,
        "name": "my_scd_data",
        "record_creation_date_column": "created_at",
        "status": "PUBLISHED",
        "tabular_source": {
            "feature_store_id": scd_data_model.tabular_source.feature_store_id,
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
    }


def test_scd_data_model(scd_data_model, expected_scd_data_model):
    """Test creation, serialization and deserialization of SCDData"""
    # rename current_flag to current_flag_column (check the model handle it properly)
    assert scd_data_model.dict() == expected_scd_data_model
    scd_data_json = scd_data_model.json(by_alias=True)
    scd_data_loaded = SCDDataModel.parse_raw(scd_data_json)
    assert scd_data_loaded == scd_data_model


def assert_missing_column(exc_info: ExceptionInfo):
    """Helper method to assert column validation given an exception"""
    errors = exc_info.value.errors()
    assert len(errors) == 1
    error = errors[0]
    assert error["msg"] == "field required"
    assert error["type"] == "value_error.missing"


@pytest.mark.parametrize("column", ["natural_key_column"])
def test_missing_scd_data_id_column_errors(expected_scd_data_model, column):
    """Test missing column validation on SCD data models"""
    # Remove the `column` so that we can test the missing column validation
    expected_scd_data_model.pop(column)
    with pytest.raises(ValidationError) as exc_info:
        SCDDataModel.parse_obj(expected_scd_data_model)
    assert_missing_column(exc_info)


def assert_type_error(exc_info: ExceptionInfo, expected_type: str):
    """Helper method to assert type validation given an exception"""
    errors = exc_info.value.errors()
    assert len(errors) == 1
    error = errors[0]
    assert error["msg"] == f"{expected_type} type expected"
    assert error["type"] == f"type_error.{expected_type}"


def test_incorrect_scd_data_id_type_errors(expected_scd_data_model):
    """Test type validation on SCD data id column"""
    # Update type to non str
    expected_scd_data_model["natural_key_column"] = arbitrary_test_date_time
    with pytest.raises(ValidationError) as exc_info:
        SCDDataModel.parse_obj(expected_scd_data_model)
    assert_type_error(exc_info, "str")
