"""
Unit tests for DimensionView class
"""

import textwrap

import pytest

from featurebyte.api.dimension_view import DimensionView
from featurebyte.api.feature_list import FeatureList
from featurebyte.enum import DBVarType
from featurebyte.exception import JoinViewMismatchError, RepeatedColumnNamesError
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType
from tests.util.helper import check_sdk_code_generation, deploy_features_through_api, get_node


class TestDimensionView(BaseViewTestSuite):
    """
    DimensionView test suite
    """

    protected_columns = ["col_int"]
    view_type = ViewType.DIMENSION_VIEW
    col = "cust_id"
    view_class = DimensionView
    bool_col = "col_boolean"
    expected_view_with_raw_accessor_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("event_timestamp" AS VARCHAR) AS "event_timestamp",
      "cust_id" AS "cust_id",
      (
        "cust_id" + 1
      ) AS "new_col"
    FROM "sf_database"."sf_schema"."dimension_table"
    LIMIT 10
    """

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.dimension_id_column == view_under_test.dimension_id_column


def test_validate_join(snowflake_dimension_view, snowflake_scd_view):
    with pytest.raises(JoinViewMismatchError):
        snowflake_dimension_view.validate_join(snowflake_scd_view)

    # assert that joining with a dimension view has no issues
    snowflake_dimension_view.validate_join(snowflake_dimension_view)


def test_get_join_column(snowflake_dimension_view):
    column = snowflake_dimension_view.get_join_column()
    # col_int is the dimension_id column name used when creating this view fixture
    assert column == "col_int"


def test_join_same_rsuffix_multiple_times(snowflake_dimension_view, snowflake_dimension_table):
    """
    Test scenario where rsuffix didn't help to resolve repeated columns issue
    """
    original_columns = snowflake_dimension_view.columns[:]
    other_view = snowflake_dimension_view[["col_text"]]

    node_name_before = snowflake_dimension_view.node_name
    joined_view = snowflake_dimension_view.join(other_view, rsuffix="_y")
    assert snowflake_dimension_view.node_name == node_name_before
    assert joined_view.columns == original_columns + ["col_text_y"]

    with pytest.raises(RepeatedColumnNamesError) as exc:
        joined_view.join(other_view, rsuffix="_y")
    assert "Duplicate column names ['col_text_y'] found" in str(exc.value)

    joined_again_view = joined_view.join(other_view, rsuffix="_z")
    assert joined_again_view.columns == original_columns + ["col_text_y", "col_text_z"]

    # check SDK code generation
    check_sdk_code_generation(
        snowflake_dimension_view,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_dimension_table.id: {
                "name": snowflake_dimension_table.name,
                "record_creation_timestamp_column": snowflake_dimension_table.record_creation_timestamp_column,
            }
        },
    )


@pytest.fixture
def snowflake_dimension_view_with_entity(
    snowflake_dimension_table, cust_id_entity, mock_api_object_cache
):
    """
    Fixture of a DimensionView with entity tagged
    """
    _ = mock_api_object_cache
    snowflake_dimension_table["col_int"].as_entity(cust_id_entity.name)
    view = snowflake_dimension_table.get_view()
    return view


def test_as_features__invalid_column_names(snowflake_dimension_view):
    """
    Test as_features() with a missing input column name
    """
    with pytest.raises(ValueError) as exc:
        snowflake_dimension_view.as_features(["non_existing_col"], ["feature_name"])
    assert str(exc.value) == "Column 'non_existing_col' not found"


def test_as_features__empty_column_names(snowflake_dimension_view):
    """
    Test as_features() with invalid number of feature names provided
    """
    with pytest.raises(ValueError) as exc:
        snowflake_dimension_view.as_features([], [])
    assert str(exc.value) == "column_names is empty"


def test_as_features__duplicate_feature_names(snowflake_dimension_view):
    """
    Test as_features() with duplicate values in feature_names
    """
    with pytest.raises(ValueError) as exc:
        snowflake_dimension_view.as_features(
            column_names=["col_float", "col_text"],
            feature_names=["FloatFeature", "FloatFeature"],
        )
    assert str(exc.value) == "feature_names contains duplicated value(s)"


def test_as_features__primary_key_not_entity(snowflake_dimension_view, mock_api_object_cache):
    """
    Test as_features() when the primary key in not an entity
    """
    _ = mock_api_object_cache
    with pytest.raises(ValueError) as exc:
        snowflake_dimension_view.as_features(["col_float", "col_char"], ["col_float", "col_char"])
    assert str(exc.value) == 'Column "col_int" is not an entity!'


def test_as_features__with_primary_key_column(
    snowflake_dimension_view_with_entity,
    snowflake_dimension_table,
    cust_id_entity,
    mock_deployment_flow,
):
    """
    Test calling as_features() when including primary column works correctly
    """
    _ = mock_deployment_flow

    # Set entity
    view = snowflake_dimension_view_with_entity
    entity_column = "col_int"
    assert entity_column in view.entity_columns

    # Select columns for as_features
    columns = [entity_column, "col_float", "col_char"]
    feature_names = ["IntFeature", "FloatFeature", "CharFeature"]

    feature_group = view.as_features(columns, feature_names)
    assert feature_group.feature_names == ["IntFeature", "FloatFeature", "CharFeature"]
    assert feature_group["IntFeature"].dtype == DBVarType.INT
    assert feature_group["FloatFeature"].dtype == DBVarType.FLOAT
    assert feature_group["CharFeature"].dtype == DBVarType.CHAR

    float_feature_dict = feature_group["CharFeature"].model_dump()
    graph_dict = float_feature_dict["graph"]
    float_feature_node_dict = get_node(graph_dict, float_feature_dict["node_name"])
    lookup_node_dict = get_node(graph_dict, "lookup_1")
    assert graph_dict["edges"] == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "graph_1", "target": "lookup_1"},
        {"source": "lookup_1", "target": "project_1"},
    ]
    assert float_feature_node_dict == {
        "name": "project_1",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["CharFeature"]},
    }
    assert lookup_node_dict == {
        "name": "lookup_1",
        "type": "lookup",
        "output_type": "frame",
        "parameters": {
            "input_column_names": ["col_int", "col_float", "col_char"],
            "feature_names": ["IntFeature", "FloatFeature", "CharFeature"],
            "entity_column": "col_int",
            "serving_name": "cust_id",
            "entity_id": cust_id_entity.id,
            "scd_parameters": None,
            "event_parameters": None,
        },
    }

    # check SDK code generation
    dimension_table_columns_info = snowflake_dimension_table.model_dump(by_alias=True)[
        "columns_info"
    ]
    for feature_name in feature_names:
        check_sdk_code_generation(
            feature_group[feature_name],
            to_use_saved_data=False,
            table_id_to_info={
                snowflake_dimension_table.id: {
                    "name": snowflake_dimension_table.name,
                    "record_creation_timestamp_column": snowflake_dimension_table.record_creation_timestamp_column,
                    # since the table is not saved, we need to pass in the columns info
                    # otherwise, entity id will be missing and code generation will fail in as_features method
                    "columns_info": dimension_table_columns_info,
                }
            },
        )

    # check offline store info
    feature = feature_group["IntFeature"]
    feature.save()
    deploy_features_through_api([feature])

    # check offline store table name (should not have any feature job setting)
    offline_store_info = feature.cached_model.offline_store_info
    ingest_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_graphs) == 1
    assert ingest_graphs[0].offline_store_table_name == "cat1_cust_id"


def test_as_features__offset_provided_but_ignored(
    snowflake_dimension_view_with_entity, snowflake_dimension_table, cust_id_entity
):
    """
    Test as_features() when offset is provided but ignored
    """
    # offset ignored but should not have error
    view = snowflake_dimension_view_with_entity
    feature_group = view.as_features(
        ["col_float", "col_char"], ["col_float", "col_char"], offset="7d"
    )

    # check SDK code generation
    dimension_table_columns_info = snowflake_dimension_table.model_dump(by_alias=True)[
        "columns_info"
    ]
    check_sdk_code_generation(
        feature_group["col_float"],
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_dimension_table.id: {
                "name": snowflake_dimension_table.name,
                "record_creation_timestamp_column": snowflake_dimension_table.record_creation_timestamp_column,
                # since the table is not saved, we need to pass in the columns info
                # otherwise, entity id will be missing and code generation will fail in as_features method
                "columns_info": dimension_table_columns_info,
            }
        },
    )


def test_as_feature__not_supported(snowflake_dimension_view_with_entity):
    """
    Test calling as_feature() is not supported on a temporary column
    """
    view = snowflake_dimension_view_with_entity
    with pytest.raises(ValueError) as exc:
        (view["col_float"] + 123).as_feature("col_float_plus_123")
    assert "as_feature is only supported for named columns in the View object" in str(exc.value)


def test_as_feature__special_column(snowflake_dimension_view_with_entity):
    """
    Test as_feature() by specifying a special column
    """
    # col_int is not allowed in as_features(), but ok in as_feature()
    feature = snowflake_dimension_view_with_entity["col_int"].as_feature("IntFeature")
    assert feature.name == "IntFeature"


def test_as_feature_same_column_name(
    snowflake_dimension_view_with_entity,
    snowflake_scd_table,
    cust_id_entity,
    patch_initialize_entity_dtype,
):
    """
    Test lookup features with same column name
    """
    feature_a = snowflake_dimension_view_with_entity["col_float"].as_feature(
        "FloatFeatureDimensionView"
    )

    snowflake_scd_table["col_text"].as_entity(cust_id_entity.name)
    scd_view = snowflake_scd_table.get_view()
    feature_b = scd_view["col_float"].as_feature("FloatFeatureSCDView", offset="7d")

    new_feature = feature_b == feature_a
    new_feature.name = "lookup_combined_feature"


def test_as_target__from_view_column(snowflake_dimension_view_with_entity, cust_id_entity):
    """
    Test calling as_target() correctly
    """
    view = snowflake_dimension_view_with_entity
    target = view["col_float"].as_target("FloatTarget", "1d", fill_value=None)
    assert target.name == "FloatTarget"
    assert target.dtype == DBVarType.FLOAT

    target_dict = target.model_dump()
    graph_dict = target_dict["graph"]
    float_target_node_dict = get_node(graph_dict, target_dict["node_name"])
    lookup_node_dict = get_node(graph_dict, "lookup_target_1")
    assert graph_dict["edges"] == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "graph_1", "target": "lookup_target_1"},
        {"source": "lookup_target_1", "target": "project_1"},
    ]
    assert float_target_node_dict == {
        "name": "project_1",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["FloatTarget"]},
    }
    assert lookup_node_dict == {
        "name": "lookup_target_1",
        "type": NodeType.LOOKUP_TARGET,
        "output_type": NodeOutputType.FRAME,
        "parameters": {
            "input_column_names": ["col_float"],
            "feature_names": ["FloatTarget"],
            "entity_column": "col_int",
            "serving_name": "cust_id",
            "entity_id": cust_id_entity.id,
            "scd_parameters": None,
            "event_parameters": None,
            "offset": "1d",
        },
    }


def test_as_feature__from_view_column(snowflake_dimension_view_with_entity, cust_id_entity):
    """
    Test calling as_feature() correctly
    """
    view = snowflake_dimension_view_with_entity
    feature = view["col_float"].as_feature("FloatFeature")
    assert feature.name == "FloatFeature"
    assert feature.dtype == DBVarType.FLOAT

    feature_dict = feature.model_dump()
    graph_dict = feature_dict["graph"]
    float_feature_node_dict = get_node(graph_dict, feature_dict["node_name"])
    lookup_node_dict = get_node(graph_dict, "lookup_1")
    assert graph_dict["edges"] == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "graph_1", "target": "lookup_1"},
        {"source": "lookup_1", "target": "project_1"},
    ]
    assert float_feature_node_dict == {
        "name": "project_1",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["FloatFeature"]},
    }
    assert lookup_node_dict == {
        "name": "lookup_1",
        "type": "lookup",
        "output_type": "frame",
        "parameters": {
            "input_column_names": ["col_float"],
            "feature_names": ["FloatFeature"],
            "entity_column": "col_int",
            "serving_name": "cust_id",
            "entity_id": cust_id_entity.id,
            "scd_parameters": None,
            "event_parameters": None,
        },
    }


def test_multiple_as_feature__same_join(snowflake_dimension_view_with_entity):
    """
    Test features created from different as_feature call is joined together in sql
    """
    view = snowflake_dimension_view_with_entity
    feature_1 = view["col_float"].as_feature("FloatFeature")
    feature_2 = view[["col_float", "col_char"]]["col_char"].as_feature("CharFeature")
    feature_3_and_4 = view.as_features(
        ["col_binary", "col_boolean"], ["BinaryFeature", "BoolFeature"]
    )
    feature_list = FeatureList([feature_1, feature_2, feature_3_and_4], name="my_feature_list")
    feature_list_sql = feature_list.sql
    assert (
        feature_list_sql
        == textwrap.dedent(
            """
        WITH _FB_AGGREGATED AS (
          SELECT
            "T0"."_fb_internal_cust_id_lookup_col_float_project_1" AS "_fb_internal_cust_id_lookup_col_float_project_1",
            "T0"."_fb_internal_cust_id_lookup_col_char_project_1" AS "_fb_internal_cust_id_lookup_col_char_project_1",
            "T0"."_fb_internal_cust_id_lookup_col_binary_project_1" AS "_fb_internal_cust_id_lookup_col_binary_project_1",
            "T0"."_fb_internal_cust_id_lookup_col_boolean_project_1" AS "_fb_internal_cust_id_lookup_col_boolean_project_1"
          FROM REQUEST_TABLE AS REQ
          LEFT JOIN (
            SELECT
              "cust_id",
              ANY_VALUE("_fb_internal_cust_id_lookup_col_float_project_1") AS "_fb_internal_cust_id_lookup_col_float_project_1",
              ANY_VALUE("_fb_internal_cust_id_lookup_col_char_project_1") AS "_fb_internal_cust_id_lookup_col_char_project_1",
              ANY_VALUE("_fb_internal_cust_id_lookup_col_binary_project_1") AS "_fb_internal_cust_id_lookup_col_binary_project_1",
              ANY_VALUE("_fb_internal_cust_id_lookup_col_boolean_project_1") AS "_fb_internal_cust_id_lookup_col_boolean_project_1"
            FROM (
              SELECT
                "col_int" AS "cust_id",
                "col_float" AS "_fb_internal_cust_id_lookup_col_float_project_1",
                "col_char" AS "_fb_internal_cust_id_lookup_col_char_project_1",
                "col_binary" AS "_fb_internal_cust_id_lookup_col_binary_project_1",
                "col_boolean" AS "_fb_internal_cust_id_lookup_col_boolean_project_1"
              FROM (
                SELECT
                  "col_int" AS "col_int",
                  "col_float" AS "col_float",
                  "col_char" AS "col_char",
                  "col_text" AS "col_text",
                  "col_binary" AS "col_binary",
                  "col_boolean" AS "col_boolean",
                  "event_timestamp" AS "event_timestamp",
                  "cust_id" AS "cust_id"
                FROM "sf_database"."sf_schema"."dimension_table"
              )
            )
            GROUP BY
              "cust_id"
          ) AS T0
            ON REQ."cust_id" = T0."cust_id"
        )
        SELECT
          CAST("_fb_internal_cust_id_lookup_col_float_project_1" AS DOUBLE) AS "FloatFeature",
          "_fb_internal_cust_id_lookup_col_char_project_1" AS "CharFeature",
          "_fb_internal_cust_id_lookup_col_binary_project_1" AS "BinaryFeature",
          "_fb_internal_cust_id_lookup_col_boolean_project_1" AS "BoolFeature"
        FROM _FB_AGGREGATED AS AGG
        """
        ).strip()
    )


def test_sdk_code_generation(saved_dimension_table, update_fixtures):
    """Check SDK code generation"""
    to_use_saved_data = True
    dimension_view = saved_dimension_table.get_view()
    check_sdk_code_generation(
        dimension_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/dimension_view.py",
        update_fixtures=update_fixtures,
        table_id=saved_dimension_table.id,
    )


def test_as_target__fill_value(snowflake_dimension_view_with_entity, cust_id_entity):
    """
    Test calling as_target() with fill_value
    """
    view = snowflake_dimension_view_with_entity
    with pytest.raises(ValueError) as exc:
        view["col_float"].as_target("FloatTarget", "1d")

    assert "fill_value must be provided" in str(exc.value)

    target = view["col_float"].as_target("FloatTarget", "1d", fill_value=0.0)
    target.save()

    partial_definition = """
    dimension_view = dimension_table.get_view(
        view_mode="manual",
        drop_column_names=["created_at"],
        column_cleaning_operations=[],
    )
    target = dimension_view["col_float"].as_target(
        target_name="FloatTarget", offset="1d", fill_value=None
    )
    target_1 = target.copy()
    target_1[target.isnull()] = 0.0
    target_1.name = "FloatTarget"
    output = target_1
    """
    assert textwrap.dedent(partial_definition).strip() in target.definition


def test_as_feature__fill_value(snowflake_dimension_view_with_entity, cust_id_entity):
    """
    Test calling as_feature() with fill_value
    """
    view = snowflake_dimension_view_with_entity
    feature = view["col_float"].as_feature("FloatFeature", "1d", fill_value=0.0)
    feature.save()

    partial_definition = """
    dimension_view = dimension_table.get_view(
        view_mode="manual",
        drop_column_names=["created_at"],
        column_cleaning_operations=[],
    )
    grouped = dimension_view.as_features(
        column_names=["col_float"], feature_names=["FloatFeature"], offset=None
    )
    feat = grouped["FloatFeature"]
    feat_1 = feat.copy()
    feat_1[feat.isnull()] = 0.0
    feat_1.name = "FloatFeature"
    output = feat_1
    """
    assert textwrap.dedent(partial_definition).strip() in feature.definition
