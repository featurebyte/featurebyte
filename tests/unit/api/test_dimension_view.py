"""
Unit tests for DimensionView class
"""
import pytest

from featurebyte.api.dimension_view import DimensionView
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
    float_feature_node_dict = get_node(graph_dict, "project_2")
    lookup_node_dict = get_node(graph_dict, "lookup_1")
    assert graph_dict["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "lookup_1"},
        {"source": "lookup_1", "target": "project_2"},
    ]
    assert float_feature_node_dict == {
        "name": "project_2",
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
    float_feature_node_dict = get_node(graph_dict, "project_2")
    lookup_node_dict = get_node(graph_dict, "lookup_1")
    assert graph_dict["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "lookup_1"},
        {"source": "lookup_1", "target": "project_2"},
    ]
    assert float_feature_node_dict == {
        "name": "project_2",
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
        },
    }
