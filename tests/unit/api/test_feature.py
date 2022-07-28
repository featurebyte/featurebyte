"""
Unit test for Feature & FeatureList classes
"""
from datetime import datetime
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import FeatureGroup
from featurebyte.exception import DuplicatedRecordException, RecordCreationException
from featurebyte.models.feature import FeatureReadiness
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, Node


@pytest.fixture(name="float_feature_dict")
def float_feature_dict_fixture(float_feature):
    """
    Serialize float feature in dictionary format
    """
    # before serialization, global query graph is used
    assert set(float_feature.graph.nodes) == {
        "input_1",
        "input_2",
        "project_1",
        "project_2",
        "project_3",
        "project_4",
        "groupby_1",
        "groupby_2",
    }
    assert float_feature.graph.edges == {
        "input_2": ["project_1", "groupby_1", "groupby_2"],
        "groupby_2": ["project_2", "project_3", "project_4"],
    }

    feat_dict = float_feature.dict()
    # after serialization, pruned query graph is used
    assert set(feat_dict["graph"]["nodes"]) == {"input_1", "groupby_1", "project_1"}
    assert feat_dict["graph"]["edges"] == {"input_1": ["groupby_1"], "groupby_1": ["project_1"]}
    yield feat_dict


def test_feature_group__getitem__list_of_str(feature_group):
    """
    Test retrieving single column
    """
    feature_group_subset = feature_group[["sum_2h", "sum_1d"]]
    assert isinstance(feature_group_subset, FeatureGroup)
    assert [feat.name for feat in feature_group_subset.feature_objects.values()] == [
        "sum_2h",
        "sum_1d",
    ]


def test_feature__binary_ops_return_feature_type(float_feature):
    """
    Test Feature return correct type after binary operation
    """
    output = float_feature + float_feature
    assert isinstance(output, Feature)


def test_feature__bool_series_key_scalar_value(float_feature, bool_feature):
    """
    Test Feature conditional assignment
    """
    float_feature[bool_feature] = 10
    assert float_feature.node == Node(
        name="cond_assign_1",
        type=NodeType.COND_ASSIGN,
        parameters={"value": 10},
        output_type=NodeOutputType.SERIES,
    )


def test_feature__preview_missing_point_in_time(float_feature):
    """
    Test feature preview validation missing point in time
    """
    invalid_params = {
        "cust_id": "C1",
    }
    with pytest.raises(KeyError) as exc_info:
        float_feature.preview(invalid_params)
    assert "Point in time column not provided: POINT_IN_TIME" in str(exc_info.value)


def test_feature__preview_missing_entity_id(float_feature):
    """
    Test feature preview validation missing point in time
    """
    invalid_params = {
        "POINT_IN_TIME": "2022-04-01",
    }
    with pytest.raises(KeyError) as exc_info:
        float_feature.preview(invalid_params)
    assert "Serving name not provided: cust_id" in str(exc_info.value)


def test_feature__preview_not_a_dict(float_feature):
    """
    Test feature preview validation but dict is not provided
    """
    invalid_params = tuple(["2022-04-01", "C1"])
    with pytest.raises(ValueError) as exc_info:
        float_feature.preview(invalid_params)
    assert "point_in_time_and_serving_name should be a dict" in str(exc_info.value)


def test_feature_deserialization(float_feature, float_feature_dict, snowflake_event_view):
    """
    Test feature deserialization
    """
    global_graph = GlobalQueryGraph()
    global_graph_dict = global_graph.dict()
    float_feature_dict["_id"] = float_feature_dict.pop("id")
    deserialized_float_feature = Feature.parse_obj(float_feature_dict)
    assert deserialized_float_feature.id == float_feature.id
    assert deserialized_float_feature.name == float_feature.name
    assert deserialized_float_feature.var_type == float_feature.var_type
    assert deserialized_float_feature.lineage == float_feature.lineage
    assert deserialized_float_feature.node == float_feature.node
    assert deserialized_float_feature.graph.dict() == global_graph_dict
    assert deserialized_float_feature.row_index_lineage == float_feature.row_index_lineage
    assert deserialized_float_feature.tabular_source == float_feature.tabular_source
    assert deserialized_float_feature.graph == global_graph
    assert id(deserialized_float_feature.graph.nodes) == id(global_graph.nodes)
    tile_id1 = float_feature.graph.nodes["groupby_1"]["parameters"]["tile_id"]

    # construct another identical float feature with an additional unused column,
    # check that the tile_ids are different before serialization, serialized object are the same
    snowflake_event_view["unused_feat"] = 10.0 * snowflake_event_view["cust_id"]
    snowflake_event_view.cust_id.as_entity("customer")
    grouped = snowflake_event_view.groupby("cust_id")
    feature_group = grouped.aggregate(
        value_column="col_float",
        method="sum",
        windows=["30m", "2h", "1d"],
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
        feature_job_setting={
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
    )
    same_float_feature_dict = feature_group["sum_1d"].dict(exclude={"id": True})
    tile_id2 = float_feature.graph.nodes["groupby_2"]["parameters"]["tile_id"]
    assert tile_id1 != tile_id2
    float_feature_dict.pop("_id")
    assert float_feature_dict == same_float_feature_dict


def test_feature_to_json(float_feature):
    """
    Test feature to_json
    """
    # do not include any keys
    output_include = float_feature.json(include={})
    assert output_include == "{}"

    # exclude graph key
    output_exclude = float_feature.json(exclude={"graph": True})
    assert "graph" not in output_exclude

    # exclude_none
    assert float_feature.is_default is None
    output_exclude_none = float_feature.json(exclude_none=True)
    assert "is_default" not in output_exclude_none

    # check encoder
    float_feature.__dict__["created_at"] = datetime.now()
    output_encoder = float_feature.json(encoder=lambda v: "__default__")
    assert '"created_at": "__default__"' in output_encoder


@pytest.fixture(name="mock_insert_feature_registry")
def mock_insert_feature_registry_fixture():
    """
    Mock insert feature registry at the controller level
    """
    with patch(
        "featurebyte.routes.feature.controller.FeatureController.insert_feature_registry"
    ) as mock:
        yield mock


@pytest.fixture(name="saved_feature")
def saved_feature_fixture(snowflake_event_data, float_feature, mock_insert_feature_registry):
    """
    Saved feature fixture
    """
    _ = mock_insert_feature_registry
    event_data_id_before = snowflake_event_data.id
    snowflake_event_data.save()
    assert snowflake_event_data.id == event_data_id_before
    feature_id_before = float_feature.id
    assert float_feature.readiness is None
    float_feature.save()
    assert float_feature.id == feature_id_before
    assert float_feature.readiness == FeatureReadiness.DRAFT
    yield float_feature


def test_feature_save__exception_due_to_event_data_not_saved(float_feature, snowflake_event_data):
    """
    Test feature save failure due to event data not saved
    """
    with pytest.raises(RecordCreationException) as exc:
        float_feature.save()
    expected_msg = (
        f'EventData (event_data.id: "{snowflake_event_data.id}") not found! '
        f"Please save the EventData object first."
    )
    assert expected_msg in str(exc.value)


def test_feature_save__exception_due_to_feature_saved_before(float_feature, saved_feature):
    """
    Test feature save failure due to event data not saved
    """
    _ = saved_feature
    with pytest.raises(DuplicatedRecordException) as exc:
        float_feature.save()
    expected_msg = f'Feature (feature.id: "{float_feature.id}") has been saved before.'
    assert expected_msg in str(exc.value)


def test_feature_name__set_name_when_unnamed(float_feature):
    """
    Test setting name for unnamed features creates alias node
    """
    new_feature = float_feature + 1234

    assert new_feature.name is None
    assert new_feature.node == Node(
        name="add_1", type="add", parameters={"value": 1234}, output_type="series"
    )

    new_feature.name = "my_feature_1234"
    assert new_feature.node == Node(
        name="alias_1", type="alias", parameters={"name": "my_feature_1234"}, output_type="series"
    )
    assert new_feature.graph.backward_edges["alias_1"] == ["add_1"]


def test_feature_name__set_name_invalid_from_project(float_feature):
    """
    Test changing name for already named feature is not allowed
    """
    with pytest.raises(ValidationError) as exc_info:
        float_feature.name = "my_new_feature"
    assert exc_info.value.errors()[0]["msg"] == (
        'Feature "sum_1d" cannot be renamed to "my_new_feature"'
    )


def test_feature_name__set_name_invalid_from_alias(float_feature):
    """
    Test changing name for already named feature is not allowed
    """
    new_feature = float_feature + 1234
    new_feature.name = "my_feature_1234"
    with pytest.raises(ValidationError) as exc_info:
        new_feature.name = "my_feature_1234_v2"
    assert exc_info.value.errors()[0]["msg"] == (
        'Feature "my_feature_1234" cannot be renamed to "my_feature_1234_v2"'
    )
