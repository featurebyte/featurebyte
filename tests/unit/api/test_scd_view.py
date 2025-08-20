"""
Unit tests for SCDView class
"""

import pytest

from featurebyte.api.scd_view import SCDView
from featurebyte.exception import JoinViewMismatchError
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType
from tests.util.helper import check_sdk_code_generation, deploy_features_through_api, get_node


class TestSCDView(BaseViewTestSuite):
    """
    SCDView test suite
    """

    protected_columns = ["col_int", "col_text", "effective_timestamp", "is_active"]
    view_type = ViewType.SCD_VIEW
    col = "cust_id"
    view_class = SCDView
    bool_col = "col_boolean"
    expected_view_with_raw_accessor_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("effective_timestamp" AS VARCHAR) AS "effective_timestamp",
      CAST("end_timestamp" AS VARCHAR) AS "end_timestamp",
      "date_of_birth" AS "date_of_birth",
      CAST("created_at" AS VARCHAR) AS "created_at",
      "cust_id" AS "cust_id",
      (
        "cust_id" + 1
      ) AS "new_col"
    FROM "sf_database"."sf_schema"."scd_table"
    LIMIT 10
    """
    additional_expected_drop_column_names = ["is_active"]

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.natural_key_column == view_under_test.natural_key_column
        assert row_subset.surrogate_key_column == view_under_test.surrogate_key_column
        assert row_subset.effective_timestamp_column == view_under_test.effective_timestamp_column
        assert row_subset.end_timestamp_column == view_under_test.end_timestamp_column
        assert row_subset.current_flag_column == view_under_test.current_flag_column


def test_validate_join(snowflake_scd_view, snowflake_dimension_view):
    """
    Test validate join
    """
    with pytest.raises(JoinViewMismatchError):
        snowflake_scd_view.validate_join(snowflake_scd_view)

    # assert that joining with a dimension view has no issues
    snowflake_scd_view.validate_join(snowflake_dimension_view)


def test_scd_view__create_with_minimal_params(snowflake_database_table_scd_table, catalog):
    """
    Test SCD view creation with minimal params
    """
    _ = catalog

    scd_table_with_minimal_params = snowflake_database_table_scd_table.create_scd_table(
        name="sf_scd_table",
        natural_key_column="col_text",
        effective_timestamp_column="effective_timestamp",
    )
    # Able to create view
    scd_table_with_minimal_params.get_view()


def test_get_join_column(snowflake_scd_view):
    """
    Test get join column
    """
    column = snowflake_scd_view.get_join_column()
    # col_text is the natural_key column name used when creating this view fixture
    assert column == "col_text"


def test_event_view_join_scd_view(
    snowflake_event_view, snowflake_event_table, snowflake_scd_view, snowflake_scd_table
):
    """
    Test additional join parameters are added for SCDView
    """
    joined_view = snowflake_event_view.join(snowflake_scd_view, rsuffix="_scd")
    assert joined_view.node.parameters.model_dump() == {
        "left_on": "col_text",
        "right_on": "col_text",
        "left_input_columns": [
            "col_int",
            "col_float",
            "col_char",
            "col_text",
            "col_binary",
            "col_boolean",
            "event_timestamp",
            "cust_id",
        ],
        "left_output_columns": [
            "col_int",
            "col_float",
            "col_char",
            "col_text",
            "col_binary",
            "col_boolean",
            "event_timestamp",
            "cust_id",
        ],
        "right_input_columns": [
            "col_float",
            "col_binary",
            "col_boolean",
            "date_of_birth",
            "created_at",
            "cust_id",
        ],
        "right_output_columns": [
            "col_float_scd",
            "col_binary_scd",
            "col_boolean_scd",
            "date_of_birth_scd",
            "created_at_scd",
            "cust_id_scd",
        ],
        "join_type": "left",
        "scd_parameters": {
            "effective_timestamp_column": "effective_timestamp",
            "natural_key_column": "col_text",
            "current_flag_column": "is_active",
            "end_timestamp_column": "end_timestamp",
            "left_timestamp_column": "event_timestamp",
            "effective_timestamp_metadata": None,
            "end_timestamp_metadata": None,
            "left_timestamp_metadata": None,
        },
        "metadata": {"type": "join", "rsuffix": "_scd", "rprefix": ""},
    }

    # check SDK code generation
    check_sdk_code_generation(
        snowflake_event_view,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_scd_table.id: {
                "name": snowflake_scd_table.name,
                "record_creation_timestamp_column": snowflake_scd_table.record_creation_timestamp_column,
            },
            snowflake_event_table.id: {
                "name": snowflake_event_table.name,
                "record_creation_timestamp_column": snowflake_event_table.record_creation_timestamp_column,
            },
        },
    )


def test_scd_view_as_feature(snowflake_scd_table, cust_id_entity):
    """
    Test SCDView as_feature configures additional parameters
    """
    snowflake_scd_table["col_text"].as_entity(cust_id_entity.name)
    scd_view = snowflake_scd_table.get_view()
    feature = scd_view["col_float"].as_feature("FloatFeature", offset="7d")
    graph_dict = feature.model_dump()["graph"]
    lookup_node = get_node(graph_dict, "lookup_1")
    assert lookup_node == {
        "name": "lookup_1",
        "type": "lookup",
        "output_type": "frame",
        "parameters": {
            "input_column_names": ["col_float"],
            "feature_names": ["FloatFeature"],
            "entity_column": "col_text",
            "serving_name": "cust_id",
            "entity_id": cust_id_entity.id,
            "scd_parameters": {
                "effective_timestamp_column": "effective_timestamp",
                "effective_timestamp_metadata": None,
                "natural_key_column": "col_text",
                "current_flag_column": "is_active",
                "end_timestamp_column": "end_timestamp",
                "end_timestamp_metadata": None,
                "offset": "7d",
            },
            "event_parameters": None,
            "snapshots_parameters": None,
        },
    }

    # check SDK code generation
    scd_table_columns_info = snowflake_scd_table.model_dump(by_alias=True)["columns_info"]
    check_sdk_code_generation(
        feature,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_scd_table.id: {
                "name": snowflake_scd_table.name,
                "record_creation_timestamp_column": snowflake_scd_table.record_creation_timestamp_column,
                # since the table is not saved, we need to pass in the columns info
                # otherwise, entity id will be missing and code generation will fail in as_features method
                "columns_info": scd_table_columns_info,
            }
        },
    )


def test_scd_view_as_feature__invalid_duration(snowflake_scd_table, cust_id_entity):
    """
    Test SCDView as_feature configures additional parameters
    """
    snowflake_scd_table["col_text"].as_entity(cust_id_entity.name)
    scd_view = snowflake_scd_table.get_view()
    with pytest.raises(ValueError) as exc:
        scd_view["col_float"].as_feature("FloatFeature", offset="something")
    assert "Failed to parse the offset parameter" in str(exc.value)


def test_scd_view_inherited__columns(snowflake_scd_view):
    """
    Test SCDView inherited columns include both natural key column and effective
    timestamp column
    """
    subset_view = snowflake_scd_view[["col_float"]]
    assert subset_view.columns == ["col_float", "col_text", "effective_timestamp", "end_timestamp"]


def test_scd_view_as_feature__special_column(
    snowflake_scd_table, cust_id_entity, mock_deployment_flow
):
    """
    Test SCDView as_feature selects a special column that is excluded by default
    """
    _ = mock_deployment_flow

    snowflake_scd_table["col_text"].as_entity(cust_id_entity.name)
    scd_view = snowflake_scd_table.get_view()
    feature = scd_view["effective_timestamp"].as_feature("Latest Record Change Date")
    lookup_node_dict = get_node(feature.model_dump()["graph"], "lookup_1")
    assert feature.name == "Latest Record Change Date"
    assert lookup_node_dict["parameters"] == {
        "input_column_names": ["effective_timestamp"],
        "feature_names": ["Latest Record Change Date"],
        "entity_column": "col_text",
        "serving_name": "cust_id",
        "entity_id": cust_id_entity.id,
        "scd_parameters": {
            "effective_timestamp_column": "effective_timestamp",
            "effective_timestamp_metadata": None,
            "current_flag_column": "is_active",
            "end_timestamp_column": "end_timestamp",
            "end_timestamp_metadata": None,
            "natural_key_column": "col_text",
            "offset": None,
        },
        "event_parameters": None,
        "snapshots_parameters": None,
    }

    # check SDK code generation
    scd_table_columns_info = snowflake_scd_table.model_dump(by_alias=True)["columns_info"]
    check_sdk_code_generation(
        feature,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_scd_table.id: {
                "name": snowflake_scd_table.name,
                "record_creation_timestamp_column": snowflake_scd_table.record_creation_timestamp_column,
                # since the table is not saved, we need to pass in the columns info
                # otherwise, entity id will be missing and code generation will fail in as_features method
                "columns_info": scd_table_columns_info,
            }
        },
    )

    # check offline store table name (should have feature job setting)
    feature.save()
    deploy_features_through_api([feature])

    offline_store_info = feature.cached_model.offline_store_info
    ingest_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_graphs) == 1
    assert ingest_graphs[0].offline_store_table_name == "cat1_cust_id_1d"


def test_sdk_code_generation(saved_scd_table, update_fixtures):
    """Check SDK code generation"""
    to_use_saved_data = True
    scd_view = saved_scd_table.get_view()
    check_sdk_code_generation(
        scd_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/scd_view.py",
        update_fixtures=update_fixtures,
        table_id=saved_scd_table.id,
    )


def test_aggregate_asat_sdk_code_generation(saved_scd_table, transaction_entity, update_fixtures):
    """Test SDK code generation for aggregate asat"""
    saved_scd_table.cust_id.as_entity(transaction_entity.name)
    snowflake_scd_view = saved_scd_table.get_view()
    feature = snowflake_scd_view.groupby(by_keys=["cust_id"]).aggregate_asat(
        value_column="col_float", method="max", feature_name="col_float_max"
    )
    check_sdk_code_generation(
        feature,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/scd_view_aggregate_asat.py",
        table_id=saved_scd_table.id,
        update_fixtures=update_fixtures,
    )


def test_feature_derived_from_multiple_scd_joins(multiple_scd_joined_feature):
    """Test saving a feature derived from multiple SCD joins"""
    # check that the feature can be saved (pruning does not fail)
    feat = multiple_scd_joined_feature
    feat.save()

    # check that table names are correct
    feat_info = feat.info()
    table_names = [table["name"] for table in feat_info["tables"]]
    assert table_names == ["scd_table_state_map", "sf_scd_table", "sf_event_table"]


def test_event_view_join_scd_view__feature_info(
    snowflake_event_view, snowflake_scd_view_with_entity
):
    """
    Test joining event view with SCD view and creating a feature
    """
    joined_view = snowflake_event_view.join(snowflake_scd_view_with_entity, rsuffix="_scd")
    joined_view["new_col"] = joined_view["col_float"].lag("col_boolean_scd")
    feature = joined_view.groupby("col_boolean_scd").aggregate_over(
        value_column="new_col", method="max", feature_names=["new_col_max_feature"], windows=["3d"]
    )["new_col_max_feature"]
    feature.save()
    info = feature.info()
    assert info["name"] == "new_col_max_feature"
    assert info["metadata"]["input_columns"] == {
        "Input0": {
            "data": "sf_event_table",
            "column_name": "event_timestamp",
            "semantic": "event_timestamp",
        },
        "Input1": {"data": "sf_event_table", "column_name": "col_text", "semantic": None},
        "Input2": {
            "data": "sf_scd_table",
            "column_name": "col_text",
            "semantic": "scd_natural_key_id",
        },
        "Input3": {"data": "sf_scd_table", "column_name": "col_boolean", "semantic": None},
        "Input4": {"data": "sf_event_table", "column_name": "col_float", "semantic": None},
    }


def test_join_scd_view_repeated_effective_timestamp_column_allowed(
    snowflake_event_view, snowflake_scd_table_v2
):
    """
    Test effective timestamp column is excluded from repeated column validation
    """
    scd_view = snowflake_scd_table_v2.get_view()
    joined_view = snowflake_event_view.join(scd_view[["date_of_birth"]])
    assert joined_view.columns == [
        "col_int",
        "col_float",
        "col_char",
        "col_text",
        "col_binary",
        "col_boolean",
        "event_timestamp",
        "cust_id",
        "date_of_birth",
    ]


def test_change_view_join_scd_view_with_rprefix(snowflake_scd_table):
    """
    Test joining change view with SCDView with rprefix same as change view column prefix
    """
    change_view = snowflake_scd_table.get_change_view("col_boolean")
    scd_view = snowflake_scd_table.get_view()[["col_float"]]
    joined_view = change_view.join(scd_view, rprefix="new_")
    assert joined_view.columns == [
        "col_text",
        "new_effective_timestamp",
        "past_effective_timestamp",
        "new_col_boolean",
        "past_col_boolean",
        "new_col_float",
    ]


def test_join_parameters_columns__should_not_trigger_graph_inconsistency(
    snowflake_scd_view, snowflake_event_view_with_entity, feature_group_feature_job_setting
):
    """Test join parameters column order"""
    # reassigned an existing column to cause a change in column order
    original_columns = snowflake_scd_view.columns
    assert "col_float" in original_columns and original_columns != "col_float"
    snowflake_scd_view["col_float"] = snowflake_scd_view["col_float"].astype(int)
    assert snowflake_scd_view.columns[-1] == "col_float"

    # create a feature without using the assigned column so that the assign operation is going to be pruned
    joined_view = snowflake_event_view_with_entity.join(snowflake_scd_view, rsuffix="_scd")
    filtered_joined_view = joined_view[joined_view["col_boolean_scd"]]
    feature = filtered_joined_view.groupby("cust_id").aggregate_over(
        value_column="col_int",
        method="max",
        feature_names=["col_int_max"],
        windows=["3d"],
        feature_job_setting=feature_group_feature_job_setting,
    )["col_int_max"]

    # check the feature is savable
    feature.save()

    # check that the col_float is not at the end of the column list
    join_node_parameters = feature.cached_model.graph.get_node_by_name("join_1").parameters
    assert join_node_parameters.right_input_columns == [
        "col_float",
        "col_binary",
        "col_boolean",
        "date_of_birth",
        "created_at",
        "cust_id",
    ]
