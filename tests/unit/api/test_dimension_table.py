"""
Test dimension table API object
"""
from unittest.mock import patch

import pytest

from featurebyte.api.dimension_table import DimensionTable
from featurebyte.enum import TableDataType
from featurebyte.exception import DuplicatedRecordException, RecordRetrievalException
from featurebyte.models import DimensionTableModel
from tests.unit.api.base_data_test import BaseTableTestSuite, DataType
from tests.util.helper import check_sdk_code_generation


class TestDimensionTableTestSuite(BaseTableTestSuite):

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
    expected_data_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      "col_text" AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("event_timestamp" AS STRING) AS "event_timestamp",
      CAST("created_at" AS STRING) AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."dimension_table"
    LIMIT 10
    """
    expected_data_column_sql = """
    SELECT
      "col_int" AS "col_int"
    FROM "sf_database"."sf_schema"."dimension_table"
    LIMIT 10
    """
    expected_clean_data_sql = """
    SELECT
      CAST(CASE WHEN (
        "col_int" IS NULL
      ) THEN 0 ELSE "col_int" END AS BIGINT) AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      "col_text" AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("event_timestamp" AS STRING) AS "event_timestamp",
      CAST("created_at" AS STRING) AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."dimension_table"
    LIMIT 10
    """


@pytest.fixture(name="dimension_table_dict")
def dimension_table_dict_fixture(snowflake_database_table):
    """DimensionTable in serialized dictionary format"""
    return {
        "type": TableDataType.DIMENSION_TABLE,
        "name": "sf_dimension_table",
        "tabular_source": {
            "feature_store_id": snowflake_database_table.feature_store.id,
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        },
        "columns_info": [
            {
                "entity_id": None,
                "name": "col_int",
                "dtype": "INT",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "col_float",
                "dtype": "FLOAT",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "col_char",
                "dtype": "CHAR",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "col_text",
                "dtype": "VARCHAR",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "col_binary",
                "dtype": "BINARY",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "col_boolean",
                "dtype": "BOOL",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "event_timestamp",
                "dtype": "TIMESTAMP_TZ",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "created_at",
                "dtype": "TIMESTAMP_TZ",
                "semantic_id": None,
                "critical_data_info": None,
            },
            {
                "entity_id": None,
                "name": "cust_id",
                "dtype": "INT",
                "semantic_id": None,
                "critical_data_info": None,
            },
        ],
        "dimension_id_column": "col_int",
        "record_creation_timestamp_column": "created_at",
        "created_at": None,
        "updated_at": None,
        "user_id": None,
    }


def test_create_dimension_table(snowflake_database_table, dimension_table_dict):
    """
    Test DimensionTable creation using tabular source
    """
    dimension_table = snowflake_database_table.create_dimension_table(
        name="sf_dimension_table",
        dimension_id_column="col_int",
        record_creation_timestamp_column="created_at",
    )

    # check that node parameter is set properly
    node_params = dimension_table.frame.node.parameters
    assert node_params.id == dimension_table.id
    assert node_params.type == TableDataType.DIMENSION_TABLE

    # check that dimension table columns for autocompletion
    assert set(dimension_table.columns).issubset(dir(dimension_table))
    assert dimension_table._ipython_key_completions_() == set(dimension_table.columns)

    output = dimension_table.dict(by_alias=True)
    dimension_table_dict["_id"] = dimension_table.id
    dimension_table_dict["created_at"] = dimension_table.created_at
    dimension_table_dict["updated_at"] = dimension_table.updated_at
    dimension_table_dict["columns_info"][0]["semantic_id"] = dimension_table.columns_info[
        0
    ].semantic_id
    assert output == dimension_table_dict

    # user input validation
    with pytest.raises(TypeError) as exc:
        snowflake_database_table.create_dimension_table(
            name=123,
            dimension_id_column="col_int",
            record_creation_timestamp_column=345,
        )
    assert 'type of argument "name" must be str; got int instead' in str(exc.value)


def test_create_dimension_table__duplicated_record(saved_dimension_table, snowflake_database_table):
    """
    Test DimensionTable creation failure due to duplicated dimension table name
    """
    _ = saved_dimension_table
    with pytest.raises(DuplicatedRecordException) as exc:
        snowflake_database_table.create_dimension_table(
            name="sf_dimension_table",
            dimension_id_column="col_int",
            record_creation_timestamp_column="created_at",
        )
    assert (
        'DimensionTable (dimension_table.name: "sf_dimension_table") exists in saved record.'
        in str(exc.value)
    )


def test_create_dimension_table__retrieval_exception(snowflake_database_table):
    """
    Test DimensionTable creation failure due to retrieval exception
    """
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.base_table.Configurations"):
            snowflake_database_table.create_dimension_table(
                name="sf_dimension_table",
                dimension_id_column="col_int",
                record_creation_timestamp_column="created_at",
            )


def assert_info_helper(dimension_table_info):
    """
    Helper function to assert info from dimension table.
    """
    assert dimension_table_info["dimension_id_column"] == "col_int"
    assert dimension_table_info["entities"] == []
    assert dimension_table_info["name"] == "sf_dimension_table"
    assert dimension_table_info["record_creation_timestamp_column"] == "created_at"
    assert dimension_table_info["status"] == "PUBLIC_DRAFT"


def test_info(saved_dimension_table):
    """
    Test info
    """
    info = saved_dimension_table.info()
    assert_info_helper(info)

    # setting verbose = true is a no-op for now
    info = saved_dimension_table.info(verbose=True)
    assert_info_helper(info)


def test_accessing_dimension_table_attributes(snowflake_dimension_table):
    """Test accessing event table object attributes"""
    assert snowflake_dimension_table.saved is True
    assert snowflake_dimension_table.record_creation_timestamp_column == "created_at"
    assert snowflake_dimension_table.dimension_id_column == "col_int"


def test_accessing_saved_dimension_table_attributes(saved_dimension_table):
    """Test accessing event table object attributes"""
    assert saved_dimension_table.saved
    assert isinstance(saved_dimension_table.cached_model, DimensionTableModel)
    assert saved_dimension_table.record_creation_timestamp_column == "created_at"
    assert saved_dimension_table.dimension_id_column == "col_int"

    # check synchronization
    cloned = DimensionTable.get_by_id(id=saved_dimension_table.id)
    assert cloned.record_creation_timestamp_column == "created_at"
    saved_dimension_table.update_record_creation_timestamp_column(
        record_creation_timestamp_column="event_timestamp"
    )
    assert saved_dimension_table.record_creation_timestamp_column == "event_timestamp"
    assert cloned.record_creation_timestamp_column == "event_timestamp"


def test_sdk_code_generation(snowflake_database_table, update_fixtures):
    """Check SDK code generation for unsaved table"""
    dimension_table = snowflake_database_table.create_dimension_table(
        name="sf_dimension_table",
        dimension_id_column="col_int",
        record_creation_timestamp_column="created_at",
    )
    check_sdk_code_generation(
        dimension_table.frame,
        to_use_saved_data=False,
        fixture_path="tests/fixtures/sdk_code/dimension_table.py",
        update_fixtures=update_fixtures,
        table_id=dimension_table.id,
    )


def test_sdk_code_generation_on_saved_data(saved_dimension_table, update_fixtures):
    """Check SDK code generation for saved table"""
    check_sdk_code_generation(
        saved_dimension_table.frame,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/saved_dimension_table.py",
        update_fixtures=update_fixtures,
        table_id=saved_dimension_table.id,
    )
