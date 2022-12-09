"""
Test dimension data API object
"""
import datetime
from unittest.mock import patch

import pytest
from bson import ObjectId

from featurebyte.api.dimension_data import DimensionData
from featurebyte.enum import TableDataType
from featurebyte.exception import DuplicatedRecordException, RecordRetrievalException
from featurebyte.models.feature_store import DataStatus, TableDetails, TabularSource
from tests.unit.api.base_data_test import BaseDataTestSuite, DataType


class TestEventDataTestSuite(BaseDataTestSuite):

    data_type = DataType.DIMENSION_DATA
    col = "col_int"
    expected_columns = {
        "col_char",
        "col_float",
        "col_boolean",
        "event_timestamp",
        "col_text",
        "created_at",
        "col_binary",
        "col_int",
        "cust_id",
    }


@pytest.fixture(name="dimension_data_dict")
def dimension_data_dict_fixture(snowflake_database_table):
    """DimensionData in serialized dictionary format"""
    return {
        "type": TableDataType.DIMENSION_DATA,
        "name": "sf_dimension_data",
        "tabular_source": {
            "feature_store_id": snowflake_database_table.feature_store.id,
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        },
        "columns_info": [
            {"entity_id": None, "name": "col_int", "dtype": "INT", "semantic_id": None},
            {"entity_id": None, "name": "col_float", "dtype": "FLOAT", "semantic_id": None},
            {"entity_id": None, "name": "col_char", "dtype": "CHAR", "semantic_id": None},
            {"entity_id": None, "name": "col_text", "dtype": "VARCHAR", "semantic_id": None},
            {"entity_id": None, "name": "col_binary", "dtype": "BINARY", "semantic_id": None},
            {"entity_id": None, "name": "col_boolean", "dtype": "BOOL", "semantic_id": None},
            {
                "entity_id": None,
                "name": "event_timestamp",
                "dtype": "TIMESTAMP",
                "semantic_id": None,
            },
            {"entity_id": None, "name": "created_at", "dtype": "TIMESTAMP", "semantic_id": None},
            {"entity_id": None, "name": "cust_id", "dtype": "INT", "semantic_id": None},
        ],
        "dimension_data_id_column": "col_int",
        "record_creation_date_column": "created_at",
        "created_at": None,
        "updated_at": None,
        "user_id": None,
        "status": DataStatus.DRAFT,
    }


def test_from_tabular_source(snowflake_database_table, dimension_data_dict):
    """
    Test DimensionData creation using tabular source
    """
    dimension_data = DimensionData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="sf_dimension_data",
        dimension_data_id_column="col_int",
        record_creation_date_column="created_at",
    )

    # check that node parameter is set properly
    node_params = dimension_data.node.parameters
    assert node_params.id == dimension_data.id
    assert node_params.type == TableDataType.DIMENSION_DATA

    # check that dimension data columns for autocompletion
    assert set(dimension_data.columns).issubset(dir(dimension_data))
    assert dimension_data._ipython_key_completions_() == set(dimension_data.columns)

    dimension_data_dict["id"] = dimension_data.id
    assert dimension_data.dict() == dimension_data_dict

    # user input validation
    with pytest.raises(TypeError) as exc:
        DimensionData.from_tabular_source(
            tabular_source=snowflake_database_table,
            name=123,
            dimension_data_id_column="col_int",
            record_creation_date_column=345,
        )
    assert 'type of argument "name" must be str; got int instead' in str(exc.value)


def test_from_tabular_source__duplicated_record(saved_dimension_data, snowflake_database_table):
    """
    Test DimensionData creation failure due to duplicated dimension data name
    """
    _ = saved_dimension_data
    with pytest.raises(DuplicatedRecordException) as exc:
        DimensionData.from_tabular_source(
            tabular_source=snowflake_database_table,
            name="sf_dimension_data",
            dimension_data_id_column="col_int",
            record_creation_date_column="created_at",
        )
    assert (
        'DimensionData (dimension_data.name: "sf_dimension_data") exists in saved record.'
        in str(exc.value)
    )


def test_from_tabular_source__retrieval_exception(snowflake_database_table):
    """
    Test DimensionData creation failure due to retrieval exception
    """
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.data.Configurations"):
            DimensionData.from_tabular_source(
                tabular_source=snowflake_database_table,
                name="sf_dimension_data",
                dimension_data_id_column="col_int",
                record_creation_date_column="created_at",
            )


def assert_info_helper(dimension_data_info):
    """
    Helper function to assert info from dimension data.
    """
    assert dimension_data_info["dimension_data_id_column"] == "col_int"
    assert dimension_data_info["entities"] == []
    assert dimension_data_info["name"] == "sf_dimension_data"
    assert dimension_data_info["record_creation_date_column"] == "created_at"
    assert dimension_data_info["status"] == "DRAFT"


def test_info(saved_dimension_data):
    """
    Test info
    """
    info = saved_dimension_data.info()
    assert_info_helper(info)

    # setting verbose = true is a no-op for now
    info = saved_dimension_data.info(verbose=True)
    assert_info_helper(info)
