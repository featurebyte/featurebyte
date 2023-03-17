"""
Tests for DimensionData models
"""
import datetime

import pytest
from _pytest._code import ExceptionInfo
from pydantic.error_wrappers import ValidationError

from featurebyte.models import DimensionDataModel
from featurebyte.models.base import DEFAULT_WORKSPACE_ID
from featurebyte.models.feature_store import DataStatus
from featurebyte.query_graph.node.schema import TableDetails

arbitrary_test_date_time = datetime.datetime(2022, 2, 1)


@pytest.fixture(name="dimension_columns_info")
def get_dimension_columns_info():
    """Fixture to get a some dimension data columns info"""
    return [
        {
            "name": "col",
            "dtype": "INT",
            "entity_id": None,
            "semantic_id": None,
            "critical_data_info": None,
        },
        {
            "name": "dimension_id",
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
    ]


@pytest.fixture(name="dimension_data_model")
def get_dimension_data_model_fixture(snowflake_feature_store, dimension_columns_info):
    """Fixture to get a base dimension data model"""
    return DimensionDataModel(
        name="my_dimension_data",
        tabular_source={
            "feature_store_id": snowflake_feature_store.id,
            "table_details": TableDetails(
                database_name="database", schema_name="schema", table_name="table"
            ),
        },
        columns_info=dimension_columns_info,
        record_creation_date_column="created_at",
        created_at=arbitrary_test_date_time,
        status=DataStatus.PUBLISHED,
        dimension_id_column="dimension_id",
    )


@pytest.fixture(name="expected_dimension_data_model")
def get_base_expected_dimension_data_model(dimension_data_model, dimension_columns_info):
    """Fixture to get a base expected dimension data JSON"""
    return {
        "type": "dimension_data",
        "user_id": None,
        "created_at": arbitrary_test_date_time,
        "updated_at": None,
        "columns_info": dimension_columns_info,
        "id": dimension_data_model.id,
        "name": "my_dimension_data",
        "record_creation_date_column": "created_at",
        "status": "PUBLISHED",
        "tabular_source": {
            "feature_store_id": dimension_data_model.tabular_source.feature_store_id,
            "table_details": {
                "database_name": "database",
                "schema_name": "schema",
                "table_name": "table",
            },
        },
        "dimension_id_column": "dimension_id",
        "workspace_id": DEFAULT_WORKSPACE_ID,
    }


def test_dimension_data_model(dimension_data_model, expected_dimension_data_model):
    """Test creation, serialization and deserialization of DimensionData"""
    assert dimension_data_model.dict() == expected_dimension_data_model
    dimension_data_json = dimension_data_model.json(by_alias=True)
    dimension_data_loaded = DimensionDataModel.parse_raw(dimension_data_json)
    assert dimension_data_loaded == dimension_data_model

    # DEV-556: check backward compatibility
    dimension_data_dict = dimension_data_model.dict(by_alias=True)
    dimension_data_dict["dimension_data_id_column"] = dimension_data_dict.pop("dimension_id_column")
    assert dimension_data_model == DimensionDataModel(**dimension_data_dict)


def assert_missing_column(exc_info: ExceptionInfo):
    """Helper method to assert column validation given an exception"""
    errors = exc_info.value.errors()
    assert len(errors) == 1
    error = errors[0]
    assert error["msg"] == "field required"
    assert error["type"] == "value_error.missing"


def test_missing_dimension_data_id_column_errors(expected_dimension_data_model):
    """Test missing column validation on dimension data id column"""
    # Remove the "dimension_data_id_column" so that we can test the missing column validation
    expected_dimension_data_model.pop("dimension_id_column")
    with pytest.raises(ValidationError) as exc_info:
        DimensionDataModel.parse_obj(expected_dimension_data_model)
    assert_missing_column(exc_info)


def assert_type_error(exc_info: ExceptionInfo, expected_type: str):
    """Helper method to assert type validation given an exception"""
    errors = exc_info.value.errors()
    assert len(errors) == 1
    error = errors[0]
    assert error["msg"] == f"{expected_type} type expected"
    assert error["type"] == f"type_error.{expected_type}"


def test_incorrect_dimension_data_id_type_errors(expected_dimension_data_model):
    """Test type validation on dimension data id column"""
    # Update type to non str
    expected_dimension_data_model["dimension_id_column"] = arbitrary_test_date_time
    with pytest.raises(ValidationError) as exc_info:
        DimensionDataModel.parse_obj(expected_dimension_data_model)
    assert_type_error(exc_info, "str")
