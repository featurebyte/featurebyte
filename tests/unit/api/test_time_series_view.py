"""
Unit test for TimeSeriesView class
"""

import textwrap
from unittest import mock
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.time_series_view import TimeSeriesView
from featurebyte.enum import DBVarType
from featurebyte.models.periodic_task import Crontab
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.node.cleaning_operation import (
    DisguisedValueImputation,
    MissingValueImputation,
)
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType
from tests.util.helper import (
    check_observation_table_creation_query,
    check_sdk_code_generation,
    compare_pydantic_obj,
)


class TestTimeSeriesView(BaseViewTestSuite):
    """
    TimeSeriesView test suite
    """

    protected_columns = ["date"]
    view_type = ViewType.TIME_SERIES_VIEW
    col = "store_id"
    view_class = TimeSeriesView
    bool_col = "col_boolean"
    expected_view_with_raw_accessor_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("date" AS VARCHAR) AS "date",
      "store_id" AS "store_id",
      (
        "store_id" + 1
      ) AS "new_col"
    FROM "sf_database"."sf_schema"."time_series_table"
    LIMIT 10
    """

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.default_feature_job_setting == view_under_test.default_feature_job_setting


def test_from_time_series_table(snowflake_time_series_table, mock_api_object_cache):
    """
    Test event table creation
    """
    _ = mock_api_object_cache
    time_series_view_first = snowflake_time_series_table.get_view()
    expected_view_columns_info = [
        col
        for col in snowflake_time_series_table.columns_info
        if col.name != snowflake_time_series_table.record_creation_timestamp_column
    ]
    assert time_series_view_first.tabular_source == snowflake_time_series_table.tabular_source
    assert (
        time_series_view_first.row_index_lineage
        == snowflake_time_series_table.frame.row_index_lineage
    )
    assert time_series_view_first.columns_info == expected_view_columns_info

    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()
    snowflake_time_series_table.store_id.as_entity("customer")
    snowflake_time_series_table.update_default_feature_job_setting(
        feature_job_setting=CronFeatureJobSetting(
            crontab=Crontab(
                minute=0,
                hour=1,
                day_of_week="*",
                day_of_month="*",
                month_of_year="*",
            ),
            timezone="Etc/UTC",
        )
    )
    time_series_view_second = snowflake_time_series_table.get_view()
    expected_view_columns_info = [
        col
        for col in snowflake_time_series_table.columns_info
        if col.name != snowflake_time_series_table.record_creation_timestamp_column
    ]
    assert time_series_view_second.columns_info == expected_view_columns_info
    assert time_series_view_second.default_feature_job_setting == CronFeatureJobSetting(
        crontab=Crontab(
            minute=0,
            hour=1,
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        ),
        timezone="Etc/UTC",
    )


def test_getitem__list_of_str_contains_protected_column(
    snowflake_time_series_table, snowflake_time_series_view
):
    """
    Test retrieving subset of the event table features
    """
    # case 1: select a non-protect column without selecting date column and entity identifier column
    time_series_view_subset1 = snowflake_time_series_view[["col_float"]]
    assert isinstance(time_series_view_subset1, TimeSeriesView)
    assert set(time_series_view_subset1.column_var_type_map) == {
        "date",
        "col_int",
        "col_float",
    }
    assert (
        time_series_view_subset1.row_index_lineage == snowflake_time_series_view.row_index_lineage
    )
    assert (
        time_series_view_subset1.default_feature_job_setting
        == snowflake_time_series_view.default_feature_job_setting
    )

    # case 2: select a non-protected column with a timestamp column
    time_series_view_subset2 = snowflake_time_series_view[["col_float", "col_int", "date"]]
    assert isinstance(time_series_view_subset2, TimeSeriesView)
    assert set(time_series_view_subset2.column_var_type_map) == {
        "date",
        "col_int",
        "col_float",
    }
    assert (
        time_series_view_subset2.row_index_lineage == snowflake_time_series_view.row_index_lineage
    )
    assert (
        time_series_view_subset2.default_feature_job_setting
        == snowflake_time_series_view.default_feature_job_setting
    )

    # both event table subsets actually point to the same node
    assert time_series_view_subset1.node == time_series_view_subset2.node


@pytest.mark.parametrize(
    "column, offset, expected_var_type",
    [
        ("date", None, DBVarType.VARCHAR),
        ("col_float", 1, DBVarType.FLOAT),
        ("col_text", 2, DBVarType.VARCHAR),
    ],
)
def test_time_series_view_column_lag(
    snowflake_time_series_view, snowflake_time_series_table, column, offset, expected_var_type
):
    """
    Test TimeSeriesViewColumn lag operation
    """
    if offset is None:
        expected_offset_param = 1
    else:
        expected_offset_param = offset
    lag_kwargs = {}
    if offset is not None:
        lag_kwargs["offset"] = offset
    lagged_column = snowflake_time_series_view[column].lag("store_id", **lag_kwargs)
    assert lagged_column.node.output_type == NodeOutputType.SERIES
    assert lagged_column.dtype == expected_var_type
    assert lagged_column.node.type == NodeType.LAG
    compare_pydantic_obj(
        lagged_column.node.parameters,
        expected={
            "timestamp_column": "date",
            "entity_columns": ["store_id"],
            "offset": expected_offset_param,
        },
    )

    # check SDK code generation
    check_sdk_code_generation(
        lagged_column,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_time_series_table.id: {
                "name": snowflake_time_series_table.name,
                "record_creation_timestamp_column": snowflake_time_series_table.record_creation_timestamp_column,
            }
        },
    )


def test_time_series_view_column_lag__invalid(snowflake_time_series_view):
    """
    Test attempting to apply lag more than once raises error
    """
    snowflake_time_series_view["prev_col_float"] = snowflake_time_series_view["col_float"].lag(
        "store_id"
    )
    with pytest.raises(ValueError) as exc:
        (snowflake_time_series_view["prev_col_float"] + 123).lag("store_id")
    assert "lag can only be applied once per column" in str(exc.value)


def test_time_series_view_copy(snowflake_time_series_view):
    """
    Test event view copy
    """
    new_snowflake_time_series_view = snowflake_time_series_view.copy()
    assert new_snowflake_time_series_view == snowflake_time_series_view
    assert new_snowflake_time_series_view.feature_store == snowflake_time_series_view.feature_store
    assert id(new_snowflake_time_series_view.graph.nodes) == id(
        snowflake_time_series_view.graph.nodes
    )

    deep_snowflake_time_series_view = snowflake_time_series_view.copy()
    assert deep_snowflake_time_series_view == snowflake_time_series_view
    assert deep_snowflake_time_series_view.feature_store == snowflake_time_series_view.feature_store
    assert id(deep_snowflake_time_series_view.graph.nodes) == id(
        snowflake_time_series_view.graph.nodes
    )

    view_column = snowflake_time_series_view["col_int"]
    new_view_column = view_column.copy()
    assert new_view_column == view_column
    assert new_view_column.parent == view_column.parent == snowflake_time_series_view
    assert id(new_view_column.graph.nodes) == id(view_column.graph.nodes)

    deep_view_column = view_column.copy(deep=True)
    assert deep_view_column == view_column
    assert deep_view_column.parent == view_column.parent
    assert id(deep_view_column.graph.nodes) == id(view_column.graph.nodes)


def test_validate_join(snowflake_dimension_view, snowflake_time_series_view):
    """
    Test validate join
    """
    # No error expected
    snowflake_time_series_view.validate_join(snowflake_dimension_view)
    snowflake_time_series_view.validate_join(snowflake_time_series_view)


@pytest.fixture(name="generic_input_node_params")
def get_generic_input_node_params_fixture():
    node_params = {
        "type": "generic",
        "columns": ["random_column"],
        "table_details": {
            "database_name": "db",
            "schema_name": "public",
            "table_name": "transaction",
        },
        "feature_store_details": {
            "type": "snowflake",
            "details": {
                "account": "sf_account",
                "warehouse": "sf_warehouse",
                "database_name": "db",
                "schema_name": "public",
                "role_name": "role",
            },
        },
    }
    return {
        "node_type": NodeType.INPUT,
        "node_params": node_params,
        "node_output_type": NodeOutputType.FRAME,
        "input_nodes": [],
    }


def test_sdk_code_generation(saved_time_series_table, update_fixtures):
    """Check SDK code generation"""
    to_use_saved_data = True
    time_series_view = saved_time_series_table.get_view()
    check_sdk_code_generation(
        time_series_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/time_series_view.py",
        update_fixtures=update_fixtures,
        table_id=saved_time_series_table.id,
    )

    # add some cleaning operations to the table before view construction
    saved_time_series_table.col_int.update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=-1),
        ]
    )
    saved_time_series_table.col_float.update_critical_data_info(
        cleaning_operations=[
            DisguisedValueImputation(disguised_values=[-99], imputed_value=-1),
        ]
    )

    time_series_view = saved_time_series_table.get_view()
    check_sdk_code_generation(
        time_series_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/time_series_view_with_column_clean_ops.py",
        update_fixtures=update_fixtures,
        table_id=saved_time_series_table.id,
    )


@pytest.mark.usefixtures("patched_observation_table_service")
def test_create_observation_table_from_time_series_view__no_sample(
    snowflake_time_series_table, snowflake_execute_query, catalog, cust_id_entity
):
    """
    Test creating ObservationTable from an TimeSeriesView
    """
    _ = catalog
    view = snowflake_time_series_table.get_view()

    observation_table = view.create_observation_table(
        "my_observation_table_from_time_series_view",
        columns=["date", "store_id"],
        columns_rename_mapping={"date": "POINT_IN_TIME", "store_id": "cust_id"},
        primary_entities=[cust_id_entity.name],
    )

    assert isinstance(observation_table, ObservationTable)
    assert observation_table.name == "my_observation_table_from_time_series_view"
    assert observation_table.primary_entity_ids == [cust_id_entity.id]
    assert observation_table.request_input.definition is not None

    # Check that the correct query was executed
    _, kwargs = snowflake_execute_query.call_args_list[-2]
    check_observation_table_creation_query(
        kwargs["query"],
        """
        CREATE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE" AS
        SELECT
          "POINT_IN_TIME",
          "cust_id"
        FROM (
          SELECT
            "date" AS "POINT_IN_TIME",
            "store_id" AS "cust_id"
          FROM (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "col_char" AS "col_char",
              "col_text" AS "col_text",
              "col_binary" AS "col_binary",
              "col_boolean" AS "col_boolean",
              "date" AS "date",
              "store_id" AS "store_id"
            FROM "sf_database"."sf_schema"."time_series_table"
          )
        )
        WHERE
            "POINT_IN_TIME" IS NOT NULL AND
            "cust_id" IS NOT NULL
        """,
    )
    _, kwargs = snowflake_execute_query.call_args_list[-1]
    check_observation_table_creation_query(
        kwargs["query"],
        """
        CREATE OR REPLACE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE" AS
        SELECT
          ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
          *
        FROM "OBSERVATION_TABLE"
        """,
    )


@pytest.mark.usefixtures("patched_observation_table_service")
def test_create_observation_table_from_time_series_view__with_sample(
    snowflake_time_series_table_with_entity,
    snowflake_execute_query,
    cust_id_entity,
):
    """
    Test creating ObservationTable from an TimeSeriesView
    """
    view = snowflake_time_series_table_with_entity.get_view()

    with patch(
        "featurebyte.models.request_input.BaseRequestInput.get_row_count",
        AsyncMock(return_value=1000),
    ):
        observation_table = view.create_observation_table(
            "my_observation_table_from_time_series_view",
            sample_rows=100,
            columns=["date", "store_id"],
            columns_rename_mapping={"date": "POINT_IN_TIME", "store_id": "cust_id"},
        )

    assert isinstance(observation_table, ObservationTable)
    assert observation_table.name == "my_observation_table_from_time_series_view"
    assert observation_table.primary_entity_ids == [cust_id_entity.id]

    # Check that the correct query was executed
    _, kwargs = snowflake_execute_query.call_args_list[-2]
    check_observation_table_creation_query(
        kwargs["query"],
        """
        CREATE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE" AS
        SELECT
          *
        FROM (
          SELECT
            "POINT_IN_TIME",
            "cust_id"
          FROM (
            SELECT
              "date" AS "POINT_IN_TIME",
              "store_id" AS "cust_id"
            FROM (
              SELECT
                "col_int" AS "col_int",
                "col_float" AS "col_float",
                "col_char" AS "col_char",
                "col_text" AS "col_text",
                "col_binary" AS "col_binary",
                "col_boolean" AS "col_boolean",
                "date" AS "date",
                "store_id" AS "store_id"
              FROM "sf_database"."sf_schema"."time_series_table"
            )
          )
          WHERE
              "POINT_IN_TIME" IS NOT NULL AND
              "cust_id" IS NOT NULL
        ) TABLESAMPLE (14)
        ORDER BY
          RANDOM()
        LIMIT 100
        """,
    )
    _, kwargs = snowflake_execute_query.call_args_list[-1]
    check_observation_table_creation_query(
        kwargs["query"],
        """
        CREATE OR REPLACE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE" AS
        SELECT
          ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
          *
        FROM "OBSERVATION_TABLE"
        """,
    )


def test_shape(snowflake_time_series_table, snowflake_query_map):
    """
    Test creating ObservationTable from an TimeSeriesView
    """
    view = snowflake_time_series_table.get_view()
    expected_call_view = textwrap.dedent(
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
            "store_id" AS "store_id"
          FROM "sf_database"."sf_schema"."time_series_table"
        )
        SELECT
          COUNT(*) AS "count"
        FROM data
        """
    ).strip()
    expected_call_view_column = textwrap.dedent(
        """
        WITH data AS (
          SELECT
            "col_int" AS "col_int"
          FROM "sf_database"."sf_schema"."time_series_table"
        )
        SELECT
          COUNT(*) AS "count"
        FROM data
        """
    ).strip()

    def side_effect(query, timeout=None):
        _ = timeout
        res = snowflake_query_map.get(query)
        if res is not None:
            return pd.DataFrame(res)
        return pd.DataFrame({"count": [1000]})

    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        assert view.shape() == (1000, 8)
        # Check that the correct query was executed
        assert mock_execute_query.call_args[0][0] == expected_call_view
        # test view colum shape
        assert view["col_int"].shape() == (1000, 1)
        # Check that the correct query was executed
        assert mock_execute_query.call_args[0][0] == expected_call_view_column
