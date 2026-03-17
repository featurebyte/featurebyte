"""
Unit test for CalendarTable class
"""

from __future__ import annotations

import textwrap
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from typeguard import TypeCheckError

from featurebyte import CalendarTable
from featurebyte.api.entity import Entity
from featurebyte.enum import TableDataType
from featurebyte.exception import (
    DuplicatedRecordException,
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.calendar_table import CalendarTableModel
from featurebyte.query_graph.model.timestamp_schema import (
    TimestampSchema,
    TimeZoneColumn,
)
from featurebyte.query_graph.node.cleaning_operation import (
    MissingValueImputation,
)
from tests.unit.api.base_table_test import BaseTableTestSuite, DataType
from tests.util.helper import check_sdk_code_generation


@pytest.fixture(name="calendar_table_dict")
def calendar_table_dict_fixture(snowflake_database_calendar_table, user_id):
    """CalendarTable in serialized dictionary format"""
    ts_schema = {
        "format_string": "YYYY-MM-DD HH24:MI:SS",
        "timezone": "Etc/UTC",
        "is_utc_time": None,
    }
    return {
        "type": "calendar_table",
        "name": "sf_calendar_table",
        "description": "Some description",
        "tabular_source": {
            "feature_store_id": snowflake_database_calendar_table.feature_store.id,
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "calendar_table",
            },
        },
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
                "nested_field_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_float",
                "dtype": "FLOAT",
                "semantic_id": None,
                "critical_data_info": None,
                "description": "Float column",
                "dtype_metadata": None,
                "partition_metadata": None,
                "nested_field_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_char",
                "dtype": "CHAR",
                "semantic_id": None,
                "critical_data_info": None,
                "description": "Char column",
                "dtype_metadata": None,
                "partition_metadata": None,
                "nested_field_metadata": None,
            },
            {
                "entity_id": None,
                "name": "col_text",
                "dtype": "VARCHAR",
                "semantic_id": None,
                "critical_data_info": None,
                "description": "Text column",
                "dtype_metadata": None,
                "partition_metadata": None,
                "nested_field_metadata": None,
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
                "nested_field_metadata": None,
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
                "nested_field_metadata": None,
            },
            {
                "entity_id": None,
                "name": "date",
                "dtype": "VARCHAR",
                "semantic_id": None,
                "critical_data_info": None,
                "description": "Date column",
                "dtype_metadata": {"timestamp_schema": ts_schema, "timestamp_tuple_schema": None},
                "partition_metadata": None,
                "nested_field_metadata": None,
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
                "nested_field_metadata": None,
            },
            {
                "entity_id": None,
                "name": "store_id",
                "dtype": "INT",
                "semantic_id": None,
                "critical_data_info": None,
                "description": None,
                "dtype_metadata": None,
                "partition_metadata": None,
                "nested_field_metadata": None,
            },
        ],
        "series_id_column": "store_id",
        "calendar_datetime_column": "date",
        "calendar_datetime_schema": ts_schema,
        "datetime_partition_column": None,
        "datetime_partition_schema": None,
        "record_creation_timestamp_column": "created_at",
        "created_at": None,
        "updated_at": None,
        "user_id": user_id,
        "is_deleted": False,
    }


def test_create_calendar_table(snowflake_database_calendar_table, calendar_table_dict, catalog):
    """
    Test CalendarTable creation using tabular source
    """
    _ = catalog

    calendar_table = snowflake_database_calendar_table.create_calendar_table(
        name="sf_calendar_table",
        series_id_column="store_id",
        calendar_datetime_column="date",
        calendar_datetime_schema=TimestampSchema(
            format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
        ),
        record_creation_timestamp_column="created_at",
        description="Some description",
    )

    # check that node parameter is set properly
    node_params = calendar_table.frame.node.parameters
    assert node_params.id == calendar_table.id
    assert node_params.type == TableDataType.CALENDAR_TABLE

    # check that calendar table columns for autocompletion
    assert set(calendar_table.columns).issubset(dir(calendar_table))
    assert calendar_table._ipython_key_completions_() == set(calendar_table.columns)

    output = calendar_table.model_dump(by_alias=True)
    calendar_table_dict["_id"] = calendar_table.id
    calendar_table_dict["created_at"] = calendar_table.created_at
    calendar_table_dict["updated_at"] = calendar_table.updated_at
    calendar_table_dict["block_modification_by"] = []
    for column_idx in [6, 7, 8]:
        calendar_table_dict["columns_info"][column_idx]["semantic_id"] = (
            calendar_table.columns_info[column_idx].semantic_id
        )
    assert output == calendar_table_dict

    # user input validation
    with pytest.raises(TypeCheckError) as exc:
        snowflake_database_calendar_table.create_calendar_table(
            name=123,
            series_id_column="store_id",
            calendar_datetime_column=234,
            calendar_datetime_schema=TimestampSchema(
                format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
            ),
            record_creation_timestamp_column=345,
        )
    assert 'argument "name" (int) is not an instance of str' in str(exc.value)


def test_create_calendar_table__without_series_id(snowflake_database_calendar_table, catalog):
    """
    Test CalendarTable creation without series_id_column
    """
    _ = catalog

    calendar_table = snowflake_database_calendar_table.create_calendar_table(
        name="sf_calendar_table",
        calendar_datetime_column="date",
        calendar_datetime_schema=TimestampSchema(
            format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
        ),
    )
    assert calendar_table.series_id_column is None
    assert calendar_table.calendar_datetime_column == "date"


def test_create_calendar_table__duplicated_record(
    saved_calendar_table, snowflake_database_calendar_table
):
    """
    Test CalendarTable creation failure due to duplicated calendar table name
    """
    _ = saved_calendar_table
    with pytest.raises(DuplicatedRecordException) as exc:
        snowflake_database_calendar_table.create_calendar_table(
            name="sf_calendar_table",
            series_id_column="store_id",
            calendar_datetime_column="date",
            calendar_datetime_schema=TimestampSchema(
                format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
            ),
            record_creation_timestamp_column="created_at",
        )
    assert (
        'CalendarTable (calendar_table.name: "sf_calendar_table") exists in saved record.'
        in str(exc.value)
    )


def test_create_calendar_table__retrieval_exception(snowflake_database_calendar_table):
    """
    Test CalendarTable creation failure due to retrieval exception
    """
    with pytest.raises(RecordRetrievalException):
        with patch("featurebyte.api.base_table.Configurations"):
            snowflake_database_calendar_table.create_calendar_table(
                name="sf_calendar_table",
                series_id_column="store_id",
                calendar_datetime_column="date",
                calendar_datetime_schema=TimestampSchema(
                    format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
                ),
                record_creation_timestamp_column="created_at",
            )


def test_deserialization(
    calendar_table_dict,
    snowflake_feature_store,
    snowflake_execute_query,
    expected_calendar_table_preview_query,
):
    """
    Test deserialize calendar table dictionary
    """
    _ = snowflake_execute_query
    # setup proper configuration to deserialize the calendar table object
    calendar_table_dict["feature_store"] = snowflake_feature_store
    calendar_table = CalendarTable.model_validate(calendar_table_dict)
    assert calendar_table.preview_sql() == expected_calendar_table_preview_query


def test_deserialization__column_name_not_found(
    calendar_table_dict, snowflake_feature_store, snowflake_execute_query
):
    """
    Test column not found during deserialize calendar table
    """
    _ = snowflake_execute_query
    calendar_table_dict["feature_store"] = snowflake_feature_store
    calendar_table_dict["record_creation_timestamp_column"] = "some_random_name"
    with pytest.raises(ValueError) as exc:
        CalendarTable.model_validate(calendar_table_dict)
    assert 'Column "some_random_name" not found in the table!' in str(exc.value)

    calendar_table_dict["record_creation_timestamp_column"] = "created_at"
    calendar_table_dict["calendar_datetime_column"] = "some_timestamp_column"
    with pytest.raises(ValueError) as exc:
        CalendarTable.model_validate(calendar_table_dict)
    assert 'Column "some_timestamp_column" not found in the table!' in str(exc.value)


class TestCalendarTableTestSuite(BaseTableTestSuite):
    """Test CalendarTable"""

    data_type = DataType.CALENDAR_DATA
    col = "col_int"
    expected_columns = {
        "col_char",
        "col_float",
        "col_boolean",
        "date",
        "col_text",
        "created_at",
        "col_binary",
        "col_int",
        "store_id",
    }
    expected_table_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("date" AS VARCHAR) AS "date",
      CAST("created_at" AS VARCHAR) AS "created_at",
      "store_id" AS "store_id"
    FROM "sf_database"."sf_schema"."calendar_table"
    LIMIT 10
    """
    expected_table_column_sql = """
    SELECT
      "col_int" AS "col_int"
    FROM "sf_database"."sf_schema"."calendar_table"
    LIMIT 10
    """
    expected_clean_table_sql = """
    SELECT
      CAST(CASE WHEN (
        "col_int" IS NULL
      ) THEN 0 ELSE "col_int" END AS BIGINT) AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("date" AS VARCHAR) AS "date",
      CAST("created_at" AS VARCHAR) AS "created_at",
      "store_id" AS "store_id"
    FROM "sf_database"."sf_schema"."calendar_table"
    LIMIT 10
    """
    expected_clean_table_column_sql = """
    SELECT
      CAST(CASE WHEN (
        "col_int" IS NULL
      ) THEN 0 ELSE "col_int" END AS BIGINT) AS "col_int"
    FROM "sf_database"."sf_schema"."calendar_table"
    LIMIT 10
    """
    expected_timestamp_column = "date"
    expected_special_columns = ["date", "store_id", "created_at"]


def test_info__calendar_table_without_record_creation_date(
    snowflake_database_calendar_table, catalog
):
    """Test info on calendar table with record creation timestamp is None"""
    _ = catalog

    calendar_table = snowflake_database_calendar_table.create_calendar_table(
        name="sf_calendar_table",
        series_id_column="store_id",
        calendar_datetime_column="date",
        calendar_datetime_schema=TimestampSchema(
            format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
        ),
    )

    # make sure .info() can be executed without throwing any error
    _ = calendar_table.info()


def test_info(saved_calendar_table, cust_id_entity):
    """
    Test info
    """
    _ = cust_id_entity
    saved_calendar_table.store_id.as_entity("customer")
    info_dict = saved_calendar_table.info()
    expected_info = {
        "name": "sf_calendar_table",
        "calendar_datetime_column": "date",
        "record_creation_timestamp_column": "created_at",
        "status": "PUBLIC_DRAFT",
        "entities": [{"name": "customer", "serving_names": ["cust_id"], "catalog_name": "catalog"}],
        "column_count": 9,
        "table_details": {
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "table_name": "calendar_table",
        },
        "catalog_name": "catalog",
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert info_dict["updated_at"] is not None, info_dict["updated_at"]
    assert "created_at" in info_dict, info_dict

    # update critical data info
    saved_calendar_table.col_int.update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value=0)]
    )

    # update column description
    saved_calendar_table.col_int.update_description("new description")
    assert saved_calendar_table.col_int.description == "new description"

    verbose_info_dict = saved_calendar_table.info(verbose=True)
    assert verbose_info_dict.items() > expected_info.items(), info_dict
    assert verbose_info_dict["updated_at"] is not None, verbose_info_dict["updated_at"]
    assert "created_at" in verbose_info_dict, verbose_info_dict
    assert verbose_info_dict["columns_info"] == [
        {
            "name": "col_int",
            "dtype": "INT",
            "entity": None,
            "semantic": None,
            "critical_data_info": {
                "cleaning_operations": [{"type": "missing", "imputed_value": 0}]
            },
            "description": "new description",
        },
        {
            "name": "col_float",
            "dtype": "FLOAT",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
            "description": "Float column",
        },
        {
            "name": "col_char",
            "dtype": "CHAR",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
            "description": "Char column",
        },
        {
            "name": "col_text",
            "dtype": "VARCHAR",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
            "description": "Text column",
        },
        {
            "name": "col_binary",
            "dtype": "BINARY",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
            "description": None,
        },
        {
            "name": "col_boolean",
            "dtype": "BOOL",
            "entity": None,
            "semantic": None,
            "critical_data_info": None,
            "description": None,
        },
        {
            "name": "date",
            "dtype": "VARCHAR",
            "entity": None,
            "semantic": "calendar_date",
            "critical_data_info": None,
            "description": "Date column",
        },
        {
            "name": "created_at",
            "dtype": "TIMESTAMP_TZ",
            "entity": None,
            "semantic": "record_creation_timestamp",
            "critical_data_info": None,
            "description": None,
        },
        {
            "name": "store_id",
            "dtype": "INT",
            "entity": "customer",
            "semantic": "series_id",
            "critical_data_info": None,
            "description": None,
        },
    ]


def test_calendar_table__save__exceptions(saved_calendar_table):
    """
    Test save calendar table failure due to conflict
    """
    # test duplicated record exception when record exists
    with pytest.raises(ObjectHasBeenSavedError) as exc:
        saved_calendar_table.save()
    expected_msg = f'CalendarTable (id: "{saved_calendar_table.id}") has been saved before.'
    assert expected_msg in str(exc.value)


def test_calendar_table__record_creation_exception(
    snowflake_database_calendar_table, snowflake_calendar_table_id, catalog
):
    """
    Test save calendar table failure due to conflict
    """
    # check unhandled response status code
    _ = catalog
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.savable_api_object.Configurations"):
            snowflake_database_calendar_table.create_calendar_table(
                name="sf_calendar_table",
                series_id_column="store_id",
                calendar_datetime_column="date",
                calendar_datetime_schema=TimestampSchema(
                    format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
                ),
                record_creation_timestamp_column="created_at",
                _id=snowflake_calendar_table_id,
            )


def test_update_record_creation_timestamp_column__unsaved_object(
    snowflake_database_calendar_table, catalog
):
    """Test update record creation timestamp column (unsaved calendar table)"""
    _ = catalog

    calendar_table = snowflake_database_calendar_table.create_calendar_table(
        name="calendar_table",
        series_id_column="store_id",
        calendar_datetime_column="date",
        calendar_datetime_schema=TimestampSchema(
            format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
        ),
    )
    assert calendar_table.record_creation_timestamp_column is None
    calendar_table.update_record_creation_timestamp_column("created_at")
    assert calendar_table.record_creation_timestamp_column == "created_at"


def test_update_record_creation_timestamp_column__saved_object(saved_calendar_table):
    """Test update record creation timestamp column (saved calendar table)"""
    saved_calendar_table.update_record_creation_timestamp_column("created_at")
    assert saved_calendar_table.record_creation_timestamp_column == "created_at"

    # check that validation logic works
    with pytest.raises(RecordUpdateException) as exc:
        saved_calendar_table.update_record_creation_timestamp_column("random_column_name")
    expected_msg = 'Column "random_column_name" not found in the table!'
    assert expected_msg in str(exc.value)

    with pytest.raises(RecordUpdateException) as exc:
        saved_calendar_table.update_record_creation_timestamp_column("col_float")
    expected_msg = "Column \"col_float\" is expected to have type(s): ['TIMESTAMP', 'TIMESTAMP_TZ']"
    assert expected_msg in str(exc.value)


def test_get_calendar_table(snowflake_calendar_table, mock_config_path_env):
    """
    Test CalendarTable.get function
    """
    _ = mock_config_path_env

    # load the calendar table from the persistent
    loaded_calendar_table = CalendarTable.get(snowflake_calendar_table.name)
    assert loaded_calendar_table.saved is True
    assert loaded_calendar_table == snowflake_calendar_table
    assert CalendarTable.get_by_id(id=snowflake_calendar_table.id) == snowflake_calendar_table

    with pytest.raises(RecordRetrievalException) as exc:
        CalendarTable.get("unknown_calendar_table")

    expected_msg = (
        'CalendarTable (name: "unknown_calendar_table") not found. '
        "Please save the CalendarTable object first."
    )
    assert expected_msg in str(exc.value)


def test_calendar_table__entity_relation_auto_tagging(saved_calendar_table, mock_api_object_cache):
    """Test calendar table update: entity relation will be created automatically"""
    _ = mock_api_object_cache

    transaction_entity = Entity(name="transaction", serving_names=["transaction_id"])
    transaction_entity.save()

    customer = Entity(name="customer", serving_names=["customer_id"])
    customer.save()

    # add entities to calendar table
    assert saved_calendar_table.series_id_column == "store_id"
    saved_calendar_table.store_id.as_entity("transaction")
    saved_calendar_table.col_int.as_entity("customer")

    from featurebyte.models.entity import ParentEntity

    updated_transaction_entity = Entity.get_by_id(id=transaction_entity.id)
    assert updated_transaction_entity.parents == [
        ParentEntity(id=customer.id, table_type="calendar_table", table_id=saved_calendar_table.id)
    ]
    updated_customer_entity = Entity.get_by_id(id=customer.id)
    assert updated_customer_entity.parents == []

    # remove primary id column's entity
    saved_calendar_table.store_id.as_entity(None)
    updated_transaction_entity = Entity.get_by_id(id=transaction_entity.id)
    assert updated_transaction_entity.parents == []


def test_accessing_calendar_table_attributes(snowflake_calendar_table):
    """Test accessing calendar table object attributes"""
    assert snowflake_calendar_table.saved
    assert snowflake_calendar_table.record_creation_timestamp_column == "created_at"
    assert snowflake_calendar_table.calendar_datetime_column == "date"
    assert snowflake_calendar_table.series_id_column == "store_id"
    assert snowflake_calendar_table.timestamp_column == "date"


def test_accessing_saved_calendar_table_attributes(saved_calendar_table):
    """Test accessing calendar table object attributes"""
    assert saved_calendar_table.saved
    assert isinstance(saved_calendar_table.cached_model, CalendarTableModel)
    assert saved_calendar_table.record_creation_timestamp_column == "created_at"
    assert saved_calendar_table.calendar_datetime_column == "date"
    assert saved_calendar_table.series_id_column == "store_id"
    assert saved_calendar_table.timestamp_column == "date"


def test_timezone__valid(snowflake_database_calendar_table, catalog):
    """Test specifying a valid timezone"""
    _ = catalog

    calendar_table = snowflake_database_calendar_table.create_calendar_table(
        name="sf_calendar_table",
        series_id_column="store_id",
        calendar_datetime_column="date",
        calendar_datetime_schema=TimestampSchema(
            format_string="YYYY-MM-DD HH24:MI:SS", timezone="Asia/Singapore"
        ),
    )
    assert calendar_table.calendar_datetime_schema.timezone == "Asia/Singapore"

    input_node_params = calendar_table.frame.node.parameters
    assert input_node_params.calendar_datetime_schema.timezone == "Asia/Singapore"


def test_timezone__invalid(snowflake_database_calendar_table, catalog):
    """Test specifying an invalid timezone"""
    _ = catalog

    with pytest.raises(ValidationError) as exc:
        snowflake_database_calendar_table.create_calendar_table(
            name="sf_calendar_table",
            series_id_column="store_id",
            calendar_datetime_column="date",
            calendar_datetime_schema=TimestampSchema(
                format_string="YYYY-MM-DD HH24:MI:SS", timezone="Space/Time"
            ),
        )
    assert "Invalid timezone name." in str(exc.value)


def test_timezone_offset__timezone_column_not_supported(snowflake_database_calendar_table, catalog):
    """Test specifying a timezone offset using a column (not supported)"""
    _ = catalog
    with pytest.raises(RecordCreationException) as exc:
        snowflake_database_calendar_table.create_calendar_table(
            name="sf_calendar_table",
            series_id_column="store_id",
            calendar_datetime_column="date",
            calendar_datetime_schema=TimestampSchema(
                format_string="YYYY-MM-DD HH24:MI:SS",
                timezone=TimeZoneColumn(
                    column_name="col_text",
                    type="offset",
                ),
            ),
        )
    assert (
        "Timezone information in calendar_datetime_column is not supported for CalendarTable."
        in str(exc.value)
    )


def test_sdk_code_generation(saved_calendar_table, update_fixtures):
    """Check SDK code generation for unsaved table"""
    check_sdk_code_generation(
        saved_calendar_table.frame,
        to_use_saved_data=False,
        fixture_path="tests/fixtures/sdk_code/calendar_table.py",
        update_fixtures=update_fixtures,
        table_id=saved_calendar_table.id,
    )


def test_sdk_code_generation_on_saved_data(saved_calendar_table, update_fixtures):
    """Check SDK code generation for saved table"""
    check_sdk_code_generation(
        saved_calendar_table.frame,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/saved_calendar_table.py",
        update_fixtures=update_fixtures,
        table_id=saved_calendar_table.id,
    )


def test_get_view(saved_calendar_table):
    """Test get_view returns CalendarView"""
    from featurebyte.api.calendar_view import CalendarView

    calendar_view = saved_calendar_table.get_view()
    assert isinstance(calendar_view, CalendarView)
    assert calendar_view.series_id_column == "store_id"
    assert calendar_view.timestamp_column == "date"
    assert calendar_view.calendar_datetime_column == "date"


def test_get_view__auto_mode_drops_record_creation_timestamp(saved_calendar_table):
    """Test get_view in auto mode drops record_creation_timestamp_column"""
    calendar_view = saved_calendar_table.get_view()
    # record_creation_timestamp_column should be dropped in auto mode
    assert "created_at" not in calendar_view.columns


def test_get_view__manual_mode(saved_calendar_table):
    """Test get_view in manual mode"""
    from featurebyte.enum import ViewMode

    calendar_view = saved_calendar_table.get_view(view_mode=ViewMode.MANUAL)
    # In manual mode, record_creation_timestamp_column should be kept
    assert "created_at" in calendar_view.columns


def test_get_view__without_series_id(snowflake_database_calendar_table, catalog):
    """Test get_view on CalendarTable without series_id_column"""
    _ = catalog

    calendar_table = snowflake_database_calendar_table.create_calendar_table(
        name="sf_calendar_table",
        calendar_datetime_column="date",
        calendar_datetime_schema=TimestampSchema(
            format_string="YYYY-MM-DD HH24:MI:SS", timezone="Etc/UTC"
        ),
    )
    from featurebyte.api.calendar_view import CalendarView

    calendar_view = calendar_table.get_view()
    assert isinstance(calendar_view, CalendarView)
    assert calendar_view.series_id_column is None


def test_shape(snowflake_calendar_table, snowflake_query_map):
    """
    Test shape on a CalendarTable
    """
    from unittest import mock

    import pandas as pd

    def side_effect(query, timeout=None, to_log_error=True, query_metadata=None):
        _ = (
            timeout,
            to_log_error,
            query_metadata,
        )
        res = snowflake_query_map.get(query)
        if res is not None:
            return pd.DataFrame(res)
        return pd.DataFrame({"count": [1000]})

    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        assert snowflake_calendar_table.shape() == (1000, 9)
        # Check that the correct query was executed
        assert (
            mock_execute_query.call_args[0][0]
            == textwrap.dedent(
                """
                WITH data AS (
                  SELECT
                    "col_int" AS "col_int",
                    "col_float" AS "col_float",
                    "col_char" AS "col_char",
                    "col_text" AS "col_text",
                    "col_binary" AS "col_binary",
                    "col_boolean" AS "col_boolean",
                    "date" AS "date",
                    "created_at" AS "created_at",
                    "store_id" AS "store_id"
                  FROM "sf_database"."sf_schema"."calendar_table"
                )
                SELECT
                  COUNT(*) AS "count"
                FROM data
                """
            ).strip()
        )
