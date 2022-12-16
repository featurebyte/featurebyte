"""
Unit tests for DimensionView class
"""
import textwrap

import pytest

from featurebyte.api.dimension_view import DimensionView
from featurebyte.api.feature_list import FeatureList
from featurebyte.enum import DBVarType
from featurebyte.exception import JoinViewMismatchError
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType
from tests.util.helper import get_node


class TestDimensionView(BaseViewTestSuite):
    """
    DimensionView test suite
    """

    protected_columns = ["col_int"]
    view_type = ViewType.DIMENSION_VIEW
    col = "cust_id"
    factory_method = DimensionView.from_dimension_data
    view_class = DimensionView
    bool_col = "col_boolean"

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.dimension_data_id_column == view_under_test.dimension_data_id_column


def test_validate_join(snowflake_dimension_view, snowflake_scd_view):
    with pytest.raises(JoinViewMismatchError):
        snowflake_dimension_view.validate_join(snowflake_scd_view)

    # assert that joining with a dimension view has no issues
    snowflake_dimension_view.validate_join(snowflake_dimension_view)


def test_get_join_column(snowflake_dimension_view):
    column = snowflake_dimension_view.get_join_column()
    # col_int is the dimension_data_id_key column name used when creating this view fixture
    assert column == "col_int"


@pytest.fixture
def snowflake_dimension_view_with_entity(snowflake_dimension_data, cust_id_entity):
    """
    Fixture of a DimensionView with entity tagged
    """
    snowflake_dimension_data["col_int"].as_entity(cust_id_entity.name)
    view = DimensionView.from_dimension_data(snowflake_dimension_data)
    return view


def test_as_features__invalid_column_names(snowflake_dimension_view):
    """
    Test as_features() with invalid number of feature names provided
    """
    with pytest.raises(ValueError) as exc:
        snowflake_dimension_view.as_features(["col_float"])
    assert "Length of feature_names should be 8, got 1" in str(exc.value)


def test_as_features__all_special_columns(snowflake_dimension_view_with_entity):
    """
    Test as_features() when only special columns are selected
    """
    with pytest.raises(ValueError) as exc:
        # col_int is the primary key column of this view
        snowflake_dimension_view_with_entity[["col_int"]].as_features(["IntFeature"])
    assert "None of the selected columns can be converted to Features" in str(exc.value)


def test_as_features__primary_key_not_entity(snowflake_dimension_view):
    """
    Test as_features() when the primary key in not an entity
    """
    with pytest.raises(ValueError) as exc:
        snowflake_dimension_view[["col_float", "col_char"]].as_features(["col_float", "col_char"])
    assert str(exc.value) == 'Column "col_int" is not an entity!'


@pytest.mark.parametrize("include_entity", [False, True])
def test_as_features__primary_key_not_required(
    snowflake_dimension_view_with_entity, cust_id_entity, include_entity
):
    """
    Test calling as_features() on a subset with / without entity column works correctly
    """
    # Set entity
    view = snowflake_dimension_view_with_entity

    # Select columns for as_features
    columns = ["col_float", "col_char"]
    if include_entity:
        columns = ["col_int"] + columns
    feature_names = ["FloatFeature", "CharFeature"]

    feature_group = view[columns].as_features(feature_names)
    assert feature_group.feature_names == ["FloatFeature", "CharFeature"]
    assert feature_group["FloatFeature"].dtype == DBVarType.FLOAT
    assert feature_group["CharFeature"].dtype == DBVarType.CHAR

    float_feature_dict = feature_group["CharFeature"].dict()
    graph_dict = float_feature_dict["graph"]
    float_feature_node_dict = get_node(graph_dict, float_feature_dict["node_name"])
    lookup_node_dict = get_node(graph_dict, "lookup_1")
    assert graph_dict["edges"] == [
        {"source": "input_1", "target": "lookup_1"},
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
            "input_column_names": ["col_float", "col_char"],
            "feature_names": ["FloatFeature", "CharFeature"],
            "entity_column": "col_int",
            "serving_name": "cust_id",
            "entity_id": cust_id_entity.id,
            "scd_parameters": None,
        },
    }


def test_as_features__offset_provided_but_ignored(
    snowflake_dimension_view_with_entity, cust_id_entity
):
    """
    Test as_features() when offset is provided but ignored
    """
    # offset ignored but should not have error
    view = snowflake_dimension_view_with_entity
    _ = view[["col_float", "col_char"]].as_features(["col_float", "col_char"], offset="7d")


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


def test_as_feature__from_view_column(snowflake_dimension_view_with_entity, cust_id_entity):
    """
    Test calling as_feature() correctly
    """
    view = snowflake_dimension_view_with_entity
    feature = view["col_float"].as_feature("FloatFeature")
    assert feature.name == "FloatFeature"
    assert feature.dtype == DBVarType.FLOAT

    feature_dict = feature.dict()
    graph_dict = feature_dict["graph"]
    float_feature_node_dict = get_node(graph_dict, feature_dict["node_name"])
    lookup_node_dict = get_node(graph_dict, "lookup_1")
    assert graph_dict["edges"] == [
        {"source": "input_1", "target": "lookup_1"},
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
            "entity_id": cust_id_entity,
            "scd_parameters": None,
        },
    }


def test_multiple_as_feature__same_join(snowflake_dimension_view_with_entity):
    """
    Test features created from different as_feature call is joined together in sql
    """
    view = snowflake_dimension_view_with_entity
    feature_1 = view["col_float"].as_feature("FloatFeature")
    feature_2 = view[["col_float", "col_char"]]["col_char"].as_feature("CharFeature")
    feature_3_and_4 = view[["col_binary", "col_boolean"]].as_features(
        ["BinaryFeature", "BoolFeature"]
    )
    feature_list = FeatureList([feature_1, feature_2, feature_3_and_4])
    feature_list_sql = feature_list.sql
    assert (
        feature_list_sql
        == textwrap.dedent(
            """
        WITH _FB_AGGREGATED AS (
          SELECT
            "T0"."col_float_b231997b610bd9b3" AS "col_float_b231997b610bd9b3",
            "T0"."col_char_b231997b610bd9b3" AS "col_char_b231997b610bd9b3",
            "T0"."col_binary_b231997b610bd9b3" AS "col_binary_b231997b610bd9b3",
            "T0"."col_boolean_b231997b610bd9b3" AS "col_boolean_b231997b610bd9b3"
          FROM REQUEST_TABLE AS REQ
          LEFT JOIN (
            SELECT
              "col_int" AS "cust_id",
              "col_float" AS "col_float_b231997b610bd9b3",
              "col_char" AS "col_char_b231997b610bd9b3",
              "col_binary" AS "col_binary_b231997b610bd9b3",
              "col_boolean" AS "col_boolean_b231997b610bd9b3"
            FROM (
              SELECT
                "col_int" AS "col_int",
                "col_float" AS "col_float",
                "col_char" AS "col_char",
                "col_text" AS "col_text",
                "col_binary" AS "col_binary",
                "col_boolean" AS "col_boolean",
                "event_timestamp" AS "event_timestamp",
                "created_at" AS "created_at",
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."sf_table"
            )
          ) AS T0
            ON REQ."cust_id" = T0."cust_id"
        )
        SELECT
          "col_float_b231997b610bd9b3" AS "FloatFeature",
          "col_char_b231997b610bd9b3" AS "CharFeature",
          "col_binary_b231997b610bd9b3" AS "BinaryFeature",
          "col_boolean_b231997b610bd9b3" AS "BoolFeature"
        FROM _FB_AGGREGATED AS AGG
        """
        ).strip()
    )
