"""
Unit tests for SCDView class
"""
import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.scd_view import SCDView
from featurebyte.exception import JoinViewMismatchError
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType
from tests.util.helper import check_sdk_code_generation, get_node


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
      "col_text" AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("effective_timestamp" AS STRING) AS "effective_timestamp",
      CAST("end_timestamp" AS STRING) AS "end_timestamp",
      CAST("created_at" AS STRING) AS "created_at",
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


def test_scd_view__create_with_minimal_params(snowflake_database_table_scd_table):
    """
    Test SCD view creation with minimal params
    """
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
    assert joined_view.node.parameters.dict() == {
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
        "right_input_columns": ["col_float", "col_binary", "col_boolean", "created_at", "cust_id"],
        "right_output_columns": [
            "col_float_scd",
            "col_binary_scd",
            "col_boolean_scd",
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
        },
        "metadata": {"type": "join", "rsuffix": "_scd"},
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
    graph_dict = feature.dict()["graph"]
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
                "natural_key_column": "col_text",
                "current_flag_column": "is_active",
                "end_timestamp_column": "end_timestamp",
                "offset": "7d",
            },
            "event_parameters": None,
        },
    }

    # check SDK code generation
    scd_table_columns_info = snowflake_scd_table.dict(by_alias=True)["columns_info"]
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
    assert subset_view.columns == ["col_float", "col_text", "effective_timestamp"]


def test_scd_view_as_feature__special_column(snowflake_scd_table, cust_id_entity):
    """
    Test SCDView as_feature selects a special column that is excluded by default
    """
    snowflake_scd_table["col_text"].as_entity(cust_id_entity.name)
    scd_view = snowflake_scd_table.get_view()
    feature = scd_view["effective_timestamp"].as_feature("Latest Record Change Date")
    lookup_node_dict = get_node(feature.dict()["graph"], "lookup_1")
    assert feature.name == "Latest Record Change Date"
    assert lookup_node_dict["parameters"] == {
        "input_column_names": ["effective_timestamp"],
        "feature_names": ["Latest Record Change Date"],
        "entity_column": "col_text",
        "serving_name": "cust_id",
        "entity_id": cust_id_entity.id,
        "scd_parameters": {
            "effective_timestamp_column": "effective_timestamp",
            "current_flag_column": "is_active",
            "end_timestamp_column": "end_timestamp",
            "natural_key_column": "col_text",
            "offset": None,
        },
        "event_parameters": None,
    }

    # check SDK code generation
    scd_table_columns_info = snowflake_scd_table.dict(by_alias=True)["columns_info"]
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
    saved_scd_table.col_int.as_entity(transaction_entity.name)
    snowflake_scd_view = saved_scd_table.get_view()
    feature = snowflake_scd_view.groupby(by_keys=["col_int"]).aggregate_asat(
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
    # FIXME: this test is disabled because the feature is not saved
    _ = multiple_scd_joined_feature
