"""
Unit test for SnapshotsView class
"""

import textwrap
from unittest import mock
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.snapshots_view import SnapshotsView
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
    get_node,
)


class TestSnapshotsView(BaseViewTestSuite):
    """
    SnapshotsView test suite
    """

    protected_columns = ["date"]
    view_type = ViewType.SNAPSHOTS_VIEW
    col = "store_id"
    view_class = SnapshotsView
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
      CAST("another_timestamp_col" AS VARCHAR) AS "another_timestamp_col",
      (
        "store_id" + 1
      ) AS "new_col"
    FROM "sf_database"."sf_schema"."snapshots_table"
    LIMIT 10
    """

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.default_feature_job_setting == view_under_test.default_feature_job_setting


def test_from_snapshots_table(snowflake_snapshots_table, mock_api_object_cache):
    """
    Test event table creation
    """
    _ = mock_api_object_cache
    snapshots_view_first = snowflake_snapshots_table.get_view()
    expected_view_columns_info = [
        col
        for col in snowflake_snapshots_table.columns_info
        if col.name != snowflake_snapshots_table.record_creation_timestamp_column
    ]
    assert snapshots_view_first.tabular_source == snowflake_snapshots_table.tabular_source
    assert (
        snapshots_view_first.row_index_lineage == snowflake_snapshots_table.frame.row_index_lineage
    )
    assert snapshots_view_first.columns_info == expected_view_columns_info

    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()
    snowflake_snapshots_table.store_id.as_entity("customer")
    snowflake_snapshots_table.update_default_feature_job_setting(
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
    snapshots_view_second = snowflake_snapshots_table.get_view()
    expected_view_columns_info = [
        col
        for col in snowflake_snapshots_table.columns_info
        if col.name != snowflake_snapshots_table.record_creation_timestamp_column
    ]
    assert snapshots_view_second.columns_info == expected_view_columns_info
    assert snapshots_view_second.default_feature_job_setting == CronFeatureJobSetting(
        crontab=Crontab(
            minute=0,
            hour=1,
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        ),
        timezone="Etc/UTC",
        reference_timezone="Etc/UTC",
    )


def test_getitem__list_of_str_contains_protected_column(
    snowflake_snapshots_table, snowflake_snapshots_view
):
    """
    Test retrieving subset of the event table features
    """
    # case 1: select a non-protect column without selecting date column and entity identifier column
    snapshots_view_subset1 = snowflake_snapshots_view[["col_float"]]
    assert isinstance(snapshots_view_subset1, SnapshotsView)
    assert set(snapshots_view_subset1.column_var_type_map) == {
        "date",
        "col_int",
        "col_float",
    }
    assert snapshots_view_subset1.row_index_lineage == snowflake_snapshots_view.row_index_lineage
    assert (
        snapshots_view_subset1.default_feature_job_setting
        == snowflake_snapshots_view.default_feature_job_setting
    )

    # case 2: select a non-protected column with a timestamp column
    snapshots_view_subset2 = snowflake_snapshots_view[["col_float", "col_int", "date"]]
    assert isinstance(snapshots_view_subset2, SnapshotsView)
    assert set(snapshots_view_subset2.column_var_type_map) == {
        "date",
        "col_int",
        "col_float",
    }
    assert snapshots_view_subset2.row_index_lineage == snowflake_snapshots_view.row_index_lineage
    assert (
        snapshots_view_subset2.default_feature_job_setting
        == snowflake_snapshots_view.default_feature_job_setting
    )

    # both event table subsets actually point to the same node
    assert snapshots_view_subset1.node == snapshots_view_subset2.node


@pytest.mark.parametrize(
    "column, offset, expected_var_type",
    [
        ("date", None, DBVarType.VARCHAR),
        ("col_float", 1, DBVarType.FLOAT),
        ("col_text", 2, DBVarType.VARCHAR),
    ],
)
def test_snapshots_view_column_lag(
    snowflake_snapshots_view, snowflake_snapshots_table, column, offset, expected_var_type
):
    """
    Test SnapshotsViewColumn lag operation
    """
    if offset is None:
        expected_offset_param = 1
    else:
        expected_offset_param = offset
    lag_kwargs = {}
    if offset is not None:
        lag_kwargs["offset"] = offset
    lagged_column = snowflake_snapshots_view[column].lag("store_id", **lag_kwargs)
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
            snowflake_snapshots_table.id: {
                "name": snowflake_snapshots_table.name,
                "record_creation_timestamp_column": snowflake_snapshots_table.record_creation_timestamp_column,
            }
        },
    )


def test_snapshots_view_column_lag__invalid(snowflake_snapshots_view):
    """
    Test attempting to apply lag more than once raises error
    """
    snowflake_snapshots_view["prev_col_float"] = snowflake_snapshots_view["col_float"].lag(
        "store_id"
    )
    with pytest.raises(ValueError) as exc:
        (snowflake_snapshots_view["prev_col_float"] + 123).lag("store_id")
    assert "lag can only be applied once per column" in str(exc.value)


def test_snapshots_view_copy(snowflake_snapshots_view):
    """
    Test event view copy
    """
    new_snowflake_snapshots_view = snowflake_snapshots_view.copy()
    assert new_snowflake_snapshots_view == snowflake_snapshots_view
    assert new_snowflake_snapshots_view.feature_store == snowflake_snapshots_view.feature_store
    assert id(new_snowflake_snapshots_view.graph.nodes) == id(snowflake_snapshots_view.graph.nodes)

    deep_snowflake_snapshots_view = snowflake_snapshots_view.copy()
    assert deep_snowflake_snapshots_view == snowflake_snapshots_view
    assert deep_snowflake_snapshots_view.feature_store == snowflake_snapshots_view.feature_store
    assert id(deep_snowflake_snapshots_view.graph.nodes) == id(snowflake_snapshots_view.graph.nodes)

    view_column = snowflake_snapshots_view["col_int"]
    new_view_column = view_column.copy()
    assert new_view_column == view_column
    assert new_view_column.parent == view_column.parent == snowflake_snapshots_view
    assert id(new_view_column.graph.nodes) == id(view_column.graph.nodes)

    deep_view_column = view_column.copy(deep=True)
    assert deep_view_column == view_column
    assert deep_view_column.parent == view_column.parent
    assert id(deep_view_column.graph.nodes) == id(view_column.graph.nodes)


def test_validate_join(snowflake_dimension_view, snowflake_snapshots_view):
    """
    Test validate join
    """
    # No error expected
    snowflake_snapshots_view.validate_join(snowflake_dimension_view)
    snowflake_snapshots_view.validate_join(snowflake_snapshots_view)


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


def test_sdk_code_generation(saved_snapshots_table, update_fixtures):
    """Check SDK code generation"""
    to_use_saved_data = True
    snapshots_view = saved_snapshots_table.get_view()
    check_sdk_code_generation(
        snapshots_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/snapshots_view.py",
        update_fixtures=update_fixtures,
        table_id=saved_snapshots_table.id,
    )

    # add some cleaning operations to the table before view construction
    saved_snapshots_table.col_int.update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=-1),
        ]
    )
    saved_snapshots_table.col_float.update_critical_data_info(
        cleaning_operations=[
            DisguisedValueImputation(disguised_values=[-99], imputed_value=-1),
        ]
    )

    snapshots_view = saved_snapshots_table.get_view()
    check_sdk_code_generation(
        snapshots_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/snapshots_view_with_column_clean_ops.py",
        update_fixtures=update_fixtures,
        table_id=saved_snapshots_table.id,
    )


@pytest.mark.usefixtures("patched_observation_table_service")
def test_create_observation_table_from_snapshots_view__no_sample(
    snowflake_snapshots_table, snowflake_execute_query, catalog, cust_id_entity
):
    """
    Test creating ObservationTable from an SnapshotsView
    """
    _ = catalog
    view = snowflake_snapshots_table.get_view()

    observation_table = view.create_observation_table(
        "my_observation_table_from_snapshots_view",
        columns=["date", "store_id"],
        columns_rename_mapping={"date": "POINT_IN_TIME", "store_id": "cust_id"},
        primary_entities=[cust_id_entity.name],
    )

    assert isinstance(observation_table, ObservationTable)
    assert observation_table.name == "my_observation_table_from_snapshots_view"
    assert observation_table.primary_entity_ids == [cust_id_entity.id]
    assert observation_table.request_input.definition is not None

    # Check that the correct query was executed
    _, kwargs = snowflake_execute_query.call_args_list[-4]
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
              "store_id" AS "store_id",
              "another_timestamp_col" AS "another_timestamp_col"
            FROM "sf_database"."sf_schema"."snapshots_table"
          )
        )
        WHERE
              "POINT_IN_TIME" IS NOT NULL AND
              "cust_id" IS NOT NULL
        """,
    )
    _, kwargs = snowflake_execute_query.call_args_list[-2]
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
def test_create_observation_table_from_snapshots_view__with_sample(
    snowflake_snapshots_table_with_entity,
    snowflake_execute_query,
    cust_id_entity,
):
    """
    Test creating ObservationTable from an SnapshotsView
    """
    view = snowflake_snapshots_table_with_entity.get_view()

    with patch(
        "featurebyte.models.request_input.BaseRequestInput.get_row_count",
        AsyncMock(return_value=1000),
    ):
        observation_table = view.create_observation_table(
            "my_observation_table_from_snapshots_view",
            sample_rows=100,
            columns=["date", "store_id"],
            columns_rename_mapping={"date": "POINT_IN_TIME", "store_id": "cust_id"},
        )

    assert isinstance(observation_table, ObservationTable)
    assert observation_table.name == "my_observation_table_from_snapshots_view"
    assert observation_table.primary_entity_ids == [cust_id_entity.id]

    # Check that the correct query was executed
    _, kwargs = snowflake_execute_query.call_args_list[-4]
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
                "store_id" AS "store_id",
                "another_timestamp_col" AS "another_timestamp_col"
              FROM "sf_database"."sf_schema"."snapshots_table"
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
    _, kwargs = snowflake_execute_query.call_args_list[-2]
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


def test_shape(snowflake_snapshots_table, snowflake_query_map):
    """
    Test creating ObservationTable from an SnapshotsView
    """
    view = snowflake_snapshots_table.get_view()
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
            "store_id" AS "store_id",
            "another_timestamp_col" AS "another_timestamp_col"
          FROM "sf_database"."sf_schema"."snapshots_table"
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
          FROM "sf_database"."sf_schema"."snapshots_table"
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
        assert view.shape() == (1000, 9)
        # Check that the correct query was executed
        assert mock_execute_query.call_args[0][0] == expected_call_view
        # test view colum shape
        assert view["col_int"].shape() == (1000, 1)
        # Check that the correct query was executed
        assert mock_execute_query.call_args[0][0] == expected_call_view_column


@pytest.mark.parametrize(
    "left_view_fixture, right_view_fixture, expected_error, expected_snapshots_datetime_join_keys",
    [
        # SnapshotsView as left
        ("snowflake_snapshots_view", "snowflake_dimension_view", None, None),
        (
            "snowflake_snapshots_view",
            "snowflake_event_view",
            None,
            {
                "left_key": {"column_name": "date", "transform": None},
                "right_key": {
                    "column_name": "event_timestamp",
                    "transform": {
                        "original_timestamp_schema": None,
                        "snapshot_timezone_name": "Etc/UTC",
                        "snapshot_time_interval": {"unit": "DAY", "value": 1},
                        "snapshot_format_string": "YYYY-MM-DD HH24:MI:SS",
                        "snapshot_feature_job_setting": {
                            "crontab": {
                                "minute": 0,
                                "hour": 1,
                                "day_of_month": "*",
                                "month_of_year": "*",
                                "day_of_week": "*",
                            },
                            "timezone": "Etc/UTC",
                            "reference_timezone": "Etc/UTC",
                            "blind_spot": None,
                        },
                    },
                },
            },
        ),
        (
            "snowflake_snapshots_view",
            "snowflake_time_series_view",
            ValueError("Cannot join a TimeSeriesView to a SnapshotsView"),
            None,
        ),
        (
            "snowflake_snapshots_view",
            "snowflake_snapshots_view",
            NotImplementedError("Joining a SnapshotsView to SnapshotsView is not supported"),
            None,
        ),
        # Other views as left with SnapshotsView as right
        (
            "snowflake_dimension_view",
            "snowflake_snapshots_view",
            NotImplementedError("Joining a SnapshotsView to DimensionView is not supported"),
            None,
        ),
        (
            "snowflake_event_view",
            "snowflake_snapshots_view",
            None,
            {
                "left_key": {
                    "column_name": "event_timestamp",
                    "transform": {
                        "original_timestamp_schema": None,
                        "snapshot_feature_job_setting": {
                            "blind_spot": None,
                            "crontab": {
                                "day_of_month": "*",
                                "day_of_week": "*",
                                "hour": 1,
                                "minute": 0,
                                "month_of_year": "*",
                            },
                            "reference_timezone": "Etc/UTC",
                            "timezone": "Etc/UTC",
                        },
                        "snapshot_format_string": "YYYY-MM-DD HH24:MI:SS",
                        "snapshot_time_interval": {"unit": "DAY", "value": 1},
                        "snapshot_timezone_name": "Etc/UTC",
                    },
                },
                "right_key": {"column_name": "date", "transform": None},
            },
        ),
        (
            "snowflake_time_series_view",
            "snowflake_snapshots_view",
            None,
            {
                "left_key": {
                    "column_name": "date",
                    "transform": {
                        "original_timestamp_schema": {
                            "format_string": "YYYY-MM-DD " "HH24:MI:SS",
                            "is_utc_time": None,
                            "timezone": "Etc/UTC",
                        },
                        "snapshot_feature_job_setting": {
                            "blind_spot": None,
                            "crontab": {
                                "day_of_month": "*",
                                "day_of_week": "*",
                                "hour": 1,
                                "minute": 0,
                                "month_of_year": "*",
                            },
                            "reference_timezone": "Etc/UTC",
                            "timezone": "Etc/UTC",
                        },
                        "snapshot_format_string": "YYYY-MM-DD HH24:MI:SS",
                        "snapshot_time_interval": {"unit": "DAY", "value": 1},
                        "snapshot_timezone_name": "Etc/UTC",
                    },
                },
                "right_key": {"column_name": "date", "transform": None},
            },
        ),
        (
            "snowflake_scd_view",
            "snowflake_snapshots_view",
            NotImplementedError("Joining a SnapshotsView to SCDView is not supported"),
            None,
        ),
    ],
)
def test_snapshots_view_join_all_combinations(
    request,
    left_view_fixture,
    right_view_fixture,
    expected_error,
    expected_snapshots_datetime_join_keys,
):
    """
    Test join combinations involving SnapshotsView with exact parameter validation
    """
    left_view = request.getfixturevalue(left_view_fixture)
    right_view = request.getfixturevalue(right_view_fixture)

    # Use minimal column subset
    left_subset = left_view[["col_int"]]
    right_subset = right_view[["col_int"]]

    if expected_error:
        with pytest.raises(
            type(expected_error), match=str(expected_error).replace("(", r"\(").replace(")", r"\)")
        ):
            left_subset.join(right_subset, rsuffix="_test")
    else:
        joined_view = left_subset.join(right_subset, rsuffix="_test")
        join_node = joined_view.node

        # Check exact join parameters
        assert join_node.type == NodeType.JOIN
        join_params = join_node.parameters

        assert join_params.left_on == "col_int"
        assert join_params.right_on == "col_int"
        assert join_params.join_type == "left"
        assert join_params.metadata.rsuffix == "_test"

        # Check snapshots_datetime_join_keys
        if expected_snapshots_datetime_join_keys is None:
            assert join_params.snapshots_datetime_join_keys is None
        else:
            actual_dict = join_params.snapshots_datetime_join_keys.model_dump()
            assert actual_dict == expected_snapshots_datetime_join_keys


def test_snapshots_view_join_scd_view(snowflake_snapshots_view, snowflake_scd_view):
    """
    Test SnapshotsView joining SCDView produces correct scd_parameters and empty snapshots_datetime_join_keys
    """
    # SnapshotsView joining SCDView should work and produce scd_parameters
    joined_view = snowflake_snapshots_view.join(snowflake_scd_view, rsuffix="_scd", on="col_text")
    join_node = joined_view.node

    # Check exact join parameters
    assert join_node.type == NodeType.JOIN
    join_params = join_node.parameters

    assert join_params.left_on == "col_text"
    assert join_params.right_on == "col_text"
    assert join_params.join_type == "left"
    assert join_params.metadata.rsuffix == "_scd"

    # Check that snapshots_datetime_join_keys is None (empty as mentioned in requirement)
    assert join_params.snapshots_datetime_join_keys is None

    # Check that scd_parameters is present and has expected structure
    assert join_params.scd_parameters is not None
    scd_params = join_params.scd_parameters.model_dump()
    expected_scd_params = {
        "effective_timestamp_column": "effective_timestamp",
        "natural_key_column": "col_text",
        "current_flag_column": "is_active",
        "end_timestamp_column": "end_timestamp",
        "effective_timestamp_metadata": None,
        "end_timestamp_metadata": None,
        "left_timestamp_column": "date",
        "left_timestamp_metadata": {
            "timestamp_schema": {
                "format_string": "YYYY-MM-DD HH24:MI:SS",
                "is_utc_time": None,
                "timezone": "Etc/UTC",
            },
            "timestamp_tuple_schema": None,
        },
    }
    assert scd_params == expected_scd_params


def test_snapshots_view_as_feature(snowflake_snapshots_table, cust_id_entity):
    """
    Test SnapshotsView as_feature configures additional parameters
    """
    snowflake_snapshots_table["col_int"].as_entity(cust_id_entity.name)
    view = snowflake_snapshots_table.get_view()
    feature = view["col_float"].as_feature("FloatFeature", offset=7)
    graph_dict = feature.model_dump()["graph"]
    lookup_node = get_node(graph_dict, "lookup_1")
    assert lookup_node == {
        "name": "lookup_1",
        "type": NodeType.LOOKUP,
        "output_type": "frame",
        "parameters": {
            "input_column_names": ["col_float"],
            "feature_names": ["FloatFeature"],
            "entity_column": "col_int",
            "serving_name": "cust_id",
            "entity_id": cust_id_entity.id,
            "scd_parameters": None,
            "event_parameters": None,
            "snapshots_parameters": {
                "snapshot_datetime_column": "date",
                "time_interval": {"unit": "DAY", "value": 1},
                "snapshot_datetime_metadata": {
                    "timestamp_schema": {
                        "format_string": "YYYY-MM-DD HH24:MI:SS",
                        "is_utc_time": None,
                        "timezone": "Etc/UTC",
                    },
                    "timestamp_tuple_schema": None,
                },
                "feature_job_setting": None,
                "offset_size": 7,
            },
        },
    }

    # check SDK code generation
    table_columns_info = snowflake_snapshots_table.model_dump(by_alias=True)["columns_info"]
    check_sdk_code_generation(
        feature,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_snapshots_table.id: {
                "name": snowflake_snapshots_table.name,
                "record_creation_timestamp_column": snowflake_snapshots_table.record_creation_timestamp_column,
                # since the table is not saved, we need to pass in the columns info
                # otherwise, entity id will be missing and code generation will fail in as_features method
                "columns_info": table_columns_info,
            }
        },
    )
    feature.save()


def test_snapshots_view_as_target(snowflake_snapshots_table, cust_id_entity):
    """
    Test SnapshotsView as_target configures additional parameters
    """
    snowflake_snapshots_table["col_int"].as_entity(cust_id_entity.name)
    view = snowflake_snapshots_table.get_view()
    target = view["col_float"].as_target(
        "FloatTarget",
        fill_value=0,
        offset=7,
    )
    graph_dict = target.model_dump()["graph"]
    lookup_node = get_node(graph_dict, "lookup_target_1")
    assert lookup_node == {
        "name": "lookup_target_1",
        "type": NodeType.LOOKUP_TARGET,
        "output_type": "frame",
        "parameters": {
            "input_column_names": ["col_float"],
            "feature_names": ["FloatTarget"],
            "entity_column": "col_int",
            "serving_name": "cust_id",
            "entity_id": cust_id_entity.id,
            "scd_parameters": None,
            "event_parameters": None,
            "snapshots_parameters": {
                "snapshot_datetime_column": "date",
                "time_interval": {"unit": "DAY", "value": 1},
                "snapshot_datetime_metadata": {
                    "timestamp_schema": {
                        "format_string": "YYYY-MM-DD HH24:MI:SS",
                        "is_utc_time": None,
                        "timezone": "Etc/UTC",
                    },
                    "timestamp_tuple_schema": None,
                },
                "feature_job_setting": None,
                "offset_size": 7,
            },
            "offset": None,
        },
    }

    # check SDK code generation
    table_columns_info = snowflake_snapshots_table.model_dump(by_alias=True)["columns_info"]
    check_sdk_code_generation(
        target,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_snapshots_table.id: {
                "name": snowflake_snapshots_table.name,
                "record_creation_timestamp_column": snowflake_snapshots_table.record_creation_timestamp_column,
                # since the table is not saved, we need to pass in the columns info
                # otherwise, entity id will be missing and code generation will fail in as_features method
                "columns_info": table_columns_info,
            }
        },
    )
    target.save()


def test_snapshots_view_offset_validation(snowflake_snapshots_table, cust_id_entity):
    """
    Test that SnapshotsView properly validates offset parameter types
    """
    snowflake_snapshots_table["col_int"].as_entity(cust_id_entity.name)
    view = snowflake_snapshots_table.get_view()

    # Test that integer offset works
    feature = view["col_float"].as_feature("FloatFeature", offset=7)
    assert feature is not None

    # Test that string offset is rejected
    with pytest.raises(ValueError, match="String offset is not supported for SnapshotsView"):
        view["col_float"].as_feature("FloatFeature", offset="7d")

    # Test that negative integer offset is rejected
    with pytest.raises(ValueError, match="Offset for SnapshotsView must be a non-negative integer"):
        view["col_float"].as_feature("FloatFeature", offset=-1)

    # Test as_target with similar validations
    target = view["col_float"].as_target("FloatTarget", offset=5, fill_value=0)
    assert target is not None

    with pytest.raises(ValueError, match="String offset is not supported for SnapshotsView"):
        view["col_float"].as_target("FloatTarget", offset="5d", fill_value=0)

    with pytest.raises(ValueError, match="Offset for SnapshotsView must be a non-negative integer"):
        view["col_float"].as_target("FloatTarget", offset=-1, fill_value=0)
