"""
Test SCD table API object
"""

from unittest.mock import patch

import pytest
from typeguard import TypeCheckError

from featurebyte.api.entity import Entity
from featurebyte.api.scd_table import SCDTable
from featurebyte.enum import TableDataType
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from tests.unit.api.base_table_test import BaseTableTestSuite, DataType
from tests.util.helper import check_sdk_code_generation, compare_pydantic_obj


class TestSCDTableTestSuite(BaseTableTestSuite):
    """Test SCDTable"""

    data_type = DataType.SCD_DATA
    col = "col_int"
    expected_columns = {
        "is_active",
        "col_float",
        "col_boolean",
        "effective_timestamp",
        "end_timestamp",
        "col_text",
        "created_at",
        "col_binary",
        "col_int",
        "cust_id",
        "date_of_birth",
    }
    expected_table_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "is_active" AS "is_active",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("effective_timestamp" AS VARCHAR) AS "effective_timestamp",
      CAST("end_timestamp" AS VARCHAR) AS "end_timestamp",
      "date_of_birth" AS "date_of_birth",
      CAST("created_at" AS VARCHAR) AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."scd_table"
    LIMIT 10
    """
    expected_table_column_sql = """
    SELECT
      "col_int" AS "col_int"
    FROM "sf_database"."sf_schema"."scd_table"
    LIMIT 10
    """
    expected_clean_table_sql = """
    SELECT
      CAST(CASE WHEN (
        "col_int" IS NULL
      ) THEN 0 ELSE "col_int" END AS BIGINT) AS "col_int",
      "col_float" AS "col_float",
      "is_active" AS "is_active",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("effective_timestamp" AS VARCHAR) AS "effective_timestamp",
      CAST("end_timestamp" AS VARCHAR) AS "end_timestamp",
      "date_of_birth" AS "date_of_birth",
      CAST("created_at" AS VARCHAR) AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."scd_table"
    LIMIT 10
    """
    expected_clean_table_column_sql = """
    SELECT
      CAST(CASE WHEN (
        "col_int" IS NULL
      ) THEN 0 ELSE "col_int" END AS BIGINT) AS "col_int"
    FROM "sf_database"."sf_schema"."scd_table"
    LIMIT 10
    """
    expected_timestamp_column = "effective_timestamp"
    expected_special_columns = [
        "col_text",
        "col_int",
        "effective_timestamp",
        "end_timestamp",
        "is_active",
    ]


@pytest.fixture(name="scd_table_dict")
def scd_table_dict_fixture(snowflake_database_table_scd_table, user_id):
    """SCDTable in serialized dictionary format"""
    return {
        "type": TableDataType.SCD_TABLE,
        "name": "sf_scd_table",
        "description": "SCD table",
        "tabular_source": {
            "feature_store_id": snowflake_database_table_scd_table.feature_store.id,
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "scd_table",
            },
        },
        "default_feature_job_setting": None,
        "columns_info": [
            {
                "entity_id": None,
                "name": "col_int",
                "dtype": "INT",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_float",
                "dtype": "FLOAT",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "is_active",
                "dtype": "BOOL",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_text",
                "dtype": "VARCHAR",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_binary",
                "dtype": "BINARY",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_boolean",
                "dtype": "BOOL",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "effective_timestamp",
                "dtype": "TIMESTAMP_TZ",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "end_timestamp",
                "dtype": "TIMESTAMP_TZ",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "critical_data_info": None,
                "description": None,
                "dtype": "TIMESTAMP",
                "entity_id": None,
                "name": "date_of_birth",
                "semantic_id": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "created_at",
                "dtype": "TIMESTAMP_TZ",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "entity_id": None,
                "name": "cust_id",
                "dtype": "INT",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
        ],
        "natural_key_column": "col_text",
        "surrogate_key_column": "col_int",
        "effective_timestamp_column": "effective_timestamp",
        "effective_timestamp_schema": None,
        "end_timestamp_column": "end_timestamp",
        "end_timestamp_schema": None,
        "current_flag": "is_active",
        "record_creation_timestamp_column": "created_at",
        "created_at": None,
        "updated_at": None,
        "user_id": user_id,
        "is_deleted": False,
    }


def test_create_scd_table(snowflake_database_table_scd_table, scd_table_dict, catalog):
    """
    Test SCDTable creation using tabular source
    """
    _ = catalog

    scd_table = snowflake_database_table_scd_table.create_scd_table(
        name="sf_scd_table",
        natural_key_column="col_text",
        surrogate_key_column="col_int",
        effective_timestamp_column="effective_timestamp",
        end_timestamp_column="end_timestamp",
        current_flag_column="is_active",
        record_creation_timestamp_column="created_at",
    )

    # check that node parameter is set properly
    node_params = scd_table.frame.node.parameters
    assert node_params.id == scd_table.id
    assert node_params.type == TableDataType.SCD_TABLE

    # check that dimension table columns for autocompletion
    assert set(scd_table.columns).issubset(dir(scd_table))
    assert scd_table._ipython_key_completions_() == set(scd_table.columns)

    output = scd_table.model_dump(by_alias=True)
    scd_table_dict["_id"] = scd_table.id
    scd_table_dict["current_flag_column"] = scd_table_dict.pop("current_flag")  # DEV-556
    scd_table_dict["created_at"] = scd_table.created_at
    scd_table_dict["updated_at"] = scd_table.updated_at
    scd_table_dict["block_modification_by"] = []
    scd_table_dict["default_feature_job_setting"] = {
        "blind_spot": "0s",
        "offset": "0s",
        "period": "86400s",
        "execution_buffer": "0s",
    }
    for column_idx in [0, 2, 3, 6, 7, 9]:
        scd_table_dict["columns_info"][column_idx]["semantic_id"] = scd_table.columns_info[
            column_idx
        ].semantic_id
    assert output == scd_table_dict

    # user input validation
    with pytest.raises(TypeCheckError) as exc:
        snowflake_database_table_scd_table.create_scd_table(
            name=123,
            natural_key_column="col_text",
            surrogate_key_column="col_int",
            effective_timestamp_column="effective_timestamp",
            end_timestamp_column="end_timestamp",
            current_flag_column="is_current",
            record_creation_timestamp_column=345,
        )
    assert 'argument "name" (int) is not an instance of str' in str(exc.value)


@pytest.mark.usefixtures("saved_scd_table")
def test_create_scd_table__duplicated_record(snowflake_database_table_scd_table):
    """
    Test SCDTable creation failure due to duplicated dimension table name
    """
    with pytest.raises(DuplicatedRecordException) as exc:
        snowflake_database_table_scd_table.create_scd_table(
            name="sf_scd_table",
            natural_key_column="col_text",
            surrogate_key_column="col_int",
            effective_timestamp_column="effective_timestamp",
            end_timestamp_column="end_timestamp",
            current_flag_column="is_active",
            record_creation_timestamp_column="created_at",
        )
    assert 'SCDTable (scd_table.name: "sf_scd_table") exists in saved record.' in str(exc.value)


def test_create_scd_table__retrieval_exception(snowflake_database_table_scd_table):
    """
    Test SCDTable creation failure due to retrieval exception
    """
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.base_table.Configurations"):
            snowflake_database_table_scd_table.create_scd_table(
                name="sf_scd_table",
                natural_key_column="col_text",
                surrogate_key_column="col_int",
                effective_timestamp_column="effective_timestamp",
                end_timestamp_column="end_timestamp",
                current_flag_column="is_active",
                record_creation_timestamp_column="created_at",
            )


def test_create_scd_table__duplicated_column_name_in_different_fields(
    snowflake_database_table_scd_table,
):
    """Test SCDTable creation failure due to duplicated column name"""
    with pytest.raises(ValueError) as exc:
        snowflake_database_table_scd_table.create_scd_table(
            name="sf_scd_table",
            natural_key_column="col_text",
            surrogate_key_column="col_int",
            effective_timestamp_column="effective_timestamp",
            end_timestamp_column="effective_timestamp",
            current_flag_column="is_active",
            record_creation_timestamp_column="created_at",
        )

    expected_error_message = (
        "end_timestamp_column and effective_timestamp_column have to be different columns in the table but "
        '"effective_timestamp" is specified for both.'
    )
    assert expected_error_message in str(exc.value)


def assert_info_helper(scd_table_info):
    """
    Helper function to assert info from SCD table.
    """
    assert scd_table_info["entities"] == []
    assert scd_table_info["name"] == "sf_scd_table"
    assert scd_table_info["status"] == "PUBLIC_DRAFT"
    assert scd_table_info["natural_key_column"] == "col_text"
    assert scd_table_info["surrogate_key_column"] == "col_int"
    assert scd_table_info["effective_timestamp_column"] == "effective_timestamp"
    assert scd_table_info["end_timestamp_column"] == "end_timestamp"
    assert scd_table_info["current_flag_column"] == "is_active"


def test_info(saved_scd_table):
    """
    Test info
    """
    info = saved_scd_table.info()
    assert_info_helper(info)

    # setting verbose = true is a no-op for now
    info = saved_scd_table.info(verbose=True)
    assert_info_helper(info)


def test_scd_table__entity_relation_auto_tagging(saved_scd_table, mock_api_object_cache):
    """Test scd table update: entity relation will be created automatically"""
    _ = mock_api_object_cache

    entity_a = Entity(name="a", serving_names=["a_id"])
    entity_a.save()

    entity_b = Entity(name="b", serving_names=["b_id"])
    entity_b.save()

    # add entities to scd table
    assert saved_scd_table.natural_key_column == "col_text"
    saved_scd_table.col_text.as_entity("a")
    saved_scd_table.cust_id.as_entity("b")

    updated_entity_a = Entity.get_by_id(id=entity_a.id)
    compare_pydantic_obj(
        updated_entity_a.parents,
        expected=[{"id": entity_b.id, "table_type": "scd_table", "table_id": saved_scd_table.id}],
    )
    updated_entity_b = Entity.get_by_id(id=entity_b.id)
    assert updated_entity_b.parents == []

    # remove primary id column's entity
    saved_scd_table.col_text.as_entity(None)
    updated_entity_a = Entity.get_by_id(id=entity_a.id)
    assert updated_entity_a.parents == []


def test_accessing_scd_table_attributes(snowflake_scd_table):
    """Test accessing event table object attributes"""
    assert snowflake_scd_table.saved
    assert snowflake_scd_table.record_creation_timestamp_column is None
    assert snowflake_scd_table.natural_key_column == "col_text"
    assert snowflake_scd_table.effective_timestamp_column == "effective_timestamp"
    assert snowflake_scd_table.surrogate_key_column == "col_int"
    assert snowflake_scd_table.end_timestamp_column == "end_timestamp"
    assert snowflake_scd_table.current_flag_column == "is_active"
    assert snowflake_scd_table.timestamp_column == "effective_timestamp"


def test_accessing_saved_scd_table_attributes(saved_scd_table):
    """Test accessing event table object attributes"""
    assert saved_scd_table.saved
    assert isinstance(saved_scd_table.cached_model, SCDTableModel)
    assert saved_scd_table.record_creation_timestamp_column is None
    assert saved_scd_table.natural_key_column == "col_text"
    assert saved_scd_table.effective_timestamp_column == "effective_timestamp"
    assert saved_scd_table.surrogate_key_column == "col_int"
    assert saved_scd_table.end_timestamp_column == "end_timestamp"
    assert saved_scd_table.current_flag_column == "is_active"
    assert saved_scd_table.timestamp_column == "effective_timestamp"

    # check synchronization
    cloned = SCDTable.get_by_id(id=saved_scd_table.id)
    assert cloned.record_creation_timestamp_column is None
    saved_scd_table.update_record_creation_timestamp_column(
        record_creation_timestamp_column="created_at"
    )
    assert saved_scd_table.record_creation_timestamp_column == "created_at"
    assert cloned.record_creation_timestamp_column == "created_at"


def test_sdk_code_generation(snowflake_database_table_scd_table, update_fixtures, catalog):
    """Check SDK code generation for unsaved table"""
    _ = catalog

    scd_table = snowflake_database_table_scd_table.create_scd_table(
        name="sf_scd_table",
        natural_key_column="col_text",
        surrogate_key_column="col_int",
        effective_timestamp_column="effective_timestamp",
        end_timestamp_column="end_timestamp",
        current_flag_column="is_active",
        record_creation_timestamp_column="created_at",
    )
    check_sdk_code_generation(
        scd_table.frame,
        to_use_saved_data=False,
        fixture_path="tests/fixtures/sdk_code/scd_table.py",
        update_fixtures=update_fixtures,
        table_id=scd_table.id,
    )


def test_sdk_code_generation_on_saved_data(saved_scd_table, update_fixtures):
    """Check SDK code generation for saved table"""
    check_sdk_code_generation(
        saved_scd_table.frame,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/saved_scd_table.py",
        update_fixtures=update_fixtures,
        table_id=saved_scd_table.id,
    )


def test_update_default_feature_job_setting(saved_scd_table, feature_group_feature_job_setting):
    """Test update feature job setting"""
    assert saved_scd_table.default_feature_job_setting == FeatureJobSetting(
        blind_spot="0h", offset="0h", period="24h"
    )
    saved_scd_table.update_default_feature_job_setting(
        feature_job_setting=feature_group_feature_job_setting
    )
    assert saved_scd_table.default_feature_job_setting == feature_group_feature_job_setting


def test_create_scd_table_without_natural_key_column(
    snowflake_database_table_scd_table, snowflake_database_table, scd_table_dict, catalog
):
    """
    Test SCDTable creation using tabular source without natural_key_column
    """
    _ = catalog

    # create SCD table without both natural key column and end_timestamp_column and expect to fail
    with pytest.raises(ValueError) as exc:
        snowflake_database_table_scd_table.get_or_create_scd_table(
            name="sf_scd_table",
            natural_key_column=None,
            effective_timestamp_column="effective_timestamp",
            current_flag_column="is_active",
            record_creation_timestamp_column="created_at",
        )
    assert "Either natural_key_column or end_timestamp_column must be specified" in str(exc.value)

    # create SCD table without natural key column and with end_timestamp_column and expect to work
    scd_table = snowflake_database_table_scd_table.create_scd_table(
        name="sf_scd_table",
        natural_key_column=None,
        effective_timestamp_column="effective_timestamp",
        end_timestamp_column="end_timestamp",
        current_flag_column="is_active",
        record_creation_timestamp_column="created_at",
    )

    entity_a = Entity(name="a", serving_names=["a_id"])
    entity_a.save()
    scd_table.cust_id.as_entity("a")

    # check scd table info
    scd_table_info = scd_table.info()
    assert scd_table_info["natural_key_column"] is None

    # check that node parameter is set properly
    node_params = scd_table.frame.node.parameters
    assert node_params.id == scd_table.id
    assert node_params.type == TableDataType.SCD_TABLE

    output = scd_table.model_dump(by_alias=True)
    assert output["natural_key_column"] is None

    event_table = snowflake_database_table.create_event_table(
        name="sf_event_table",
        event_timestamp_column="event_timestamp",
        event_id_column=None,
        record_creation_timestamp_column="created_at",
        description="Some description",
    )
    event_view = event_table.get_view()
    scd_view = scd_table.get_view()

    # expect join to be unsuccessful
    with pytest.raises(AssertionError) as exc:
        event_view.join(scd_view)
        assert "Natural key column is not available." in str(exc.value)

    # expect lookup feature to be unsuccessful
    with pytest.raises(AssertionError) as exc:
        scd_view.col_text.as_feature("some feature")
    assert "Natural key column is not available." in str(exc.value)

    # expect aggregate_asat to be successful
    scd_view.groupby("cust_id").aggregate_asat(
        value_column=None,
        method="count",
        feature_name="count_col_int",
    )

    # expect subset to work
    _ = scd_view[["col_float", "col_text"]]


def test_timestamp_schema__effective_timestamp_column(snowflake_database_table_scd_table, catalog):
    """
    Test SCDTable creation with timestamp schema
    """
    _ = catalog

    scd_table = snowflake_database_table_scd_table.get_or_create_scd_table(
        name="sf_scd_table",
        natural_key_column="col_int",
        effective_timestamp_column="col_text",
        current_flag_column="is_active",
        record_creation_timestamp_column="created_at",
        effective_timestamp_schema=TimestampSchema(
            format_string="%Y-%m-%d",
            timezone="Etc/UTC",
        ),
    )

    # Check columns info set up correctly
    assert scd_table.effective_timestamp_column == "col_text"
    assert scd_table.effective_timestamp_schema == TimestampSchema(
        format_string="%Y-%m-%d",
        timezone="Etc/UTC",
    )
    for column_info in scd_table.columns_info:
        column_info_dict = column_info.model_dump()
        if column_info.name == "col_text":
            assert column_info_dict["dtype_metadata"] == {
                "timestamp_schema": {
                    "format_string": "%Y-%m-%d",
                    "is_utc_time": None,
                    "timezone": "Etc/UTC",
                },
                "timestamp_tuple_schema": None,
            }
        else:
            assert column_info_dict["dtype_metadata"] is None


def test_timestamp_schema__end_timestamp_column(snowflake_database_table_scd_table, catalog):
    """
    Test SCDTable creation with timestamp schema
    """
    _ = catalog

    scd_table = snowflake_database_table_scd_table.get_or_create_scd_table(
        name="sf_scd_table",
        natural_key_column="col_int",
        effective_timestamp_column="effective_timestamp",
        end_timestamp_column="col_text",
        record_creation_timestamp_column="created_at",
        end_timestamp_schema=TimestampSchema(
            format_string="%Y-%m-%d",
            timezone="Etc/UTC",
        ),
    )

    # Check columns info set up correctly
    assert scd_table.end_timestamp_column == "col_text"
    assert scd_table.end_timestamp_schema == TimestampSchema(
        format_string="%Y-%m-%d",
        timezone="Etc/UTC",
    )
    for column_info in scd_table.columns_info:
        column_info_dict = column_info.model_dump()
        if column_info.name == "col_text":
            assert column_info_dict["dtype_metadata"] == {
                "timestamp_schema": {
                    "format_string": "%Y-%m-%d",
                    "is_utc_time": None,
                    "timezone": "Etc/UTC",
                },
                "timestamp_tuple_schema": None,
            }
        else:
            assert column_info_dict["dtype_metadata"] is None


def test_timestamp_schema__mandatory_if_not_timestamp(snowflake_database_table_scd_table, catalog):
    """
    Test timestamp_schema
    """
    _ = catalog
    with pytest.raises(RecordCreationException) as exc:
        snowflake_database_table_scd_table.get_or_create_scd_table(
            name="sf_scd_table",
            natural_key_column="col_int",
            effective_timestamp_column="col_text",
            current_flag_column="is_active",
            record_creation_timestamp_column="created_at",
        )
    assert (
        " timestamp_schema is required for col_text with ambiguous timestamp type VARCHAR"
        in str(exc.value)
    )


def test_timestamp_schema__format_string_mandatory_for_varchar(
    snowflake_database_table_scd_table, catalog
):
    """
    Test timestamp_schema validation at ColumnSpec level
    """
    _ = catalog

    with pytest.raises(RecordCreationException) as exc:
        snowflake_database_table_scd_table.get_or_create_scd_table(
            name="sf_scd_table",
            natural_key_column="col_int",
            effective_timestamp_column="col_text",
            current_flag_column="is_active",
            record_creation_timestamp_column="created_at",
            effective_timestamp_schema=TimestampSchema(
                timezone="Etc/UTC",
            ),
        )
    assert "format_string is required in the timestamp_schema for column col_text" in str(exc.value)

    with pytest.raises(RecordCreationException) as exc:
        snowflake_database_table_scd_table.get_or_create_scd_table(
            name="sf_scd_table",
            natural_key_column="col_int",
            effective_timestamp_column="col_text",
            current_flag_column="is_active",
            record_creation_timestamp_column="created_at",
            effective_timestamp_schema=TimestampSchema(
                timezone="Etc/UTC",
                format_string="YYYY-MM-DD HH24:MI:SSTZH:TZM",
            ),
        )
    expected = (
        "Timestamp column 'col_text' has timezone information in the data and in the schema. "
        "Please remove timezone information from the data or from the schema."
    )
    assert expected in str(exc.value)
