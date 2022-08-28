"""
Unit test for Feature & FeatureList classes
"""
from datetime import datetime

import pytest
from bson.objectid import ObjectId

from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import FeatureGroup
from featurebyte.exception import (
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from featurebyte.query_graph.graph import GlobalQueryGraph


@pytest.fixture(name="float_feature_dict")
def float_feature_dict_fixture(float_feature):
    """
    Serialize float feature in dictionary format
    """
    # before serialization, global query graph is used
    assert float_feature.saved is False
    assert isinstance(float_feature.graph, GlobalQueryGraph)
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
    assert float_feature.node.dict(exclude={"name": True}) == {
        "type": "alias",
        "parameters": {"name": "sum_1d"},
        "output_type": "series",
    }
    float_feature_dict = float_feature.dict()
    assert float_feature_dict["graph"]["nodes"]["conditional_1"] == {
        "name": "conditional_1",
        "type": "conditional",
        "parameters": {"value": 10},
        "output_type": "series",
    }
    assert float_feature_dict["graph"]["backward_edges"] == {
        "groupby_1": ["input_1"],
        "project_1": ["groupby_1"],
        "gt_1": ["project_1"],
        "conditional_1": ["project_1", "gt_1"],
        "alias_1": ["conditional_1"],
    }


def test_feature__cond_assign_unnamed(float_feature, bool_feature):
    """
    Test Feature conditional assignment on unnamed Feature
    """
    temp_feature = float_feature + 123.0
    temp_feature[bool_feature] = 0.0
    temp_feature_dict = temp_feature.dict()
    assert temp_feature_dict["node"] == {
        "name": "conditional_1",
        "output_type": "series",
        "parameters": {"value": 0.0},
        "type": "conditional",
    }
    assert temp_feature_dict["graph"]["nodes"]["conditional_1"] == {
        "name": "conditional_1",
        "output_type": "series",
        "parameters": {"value": 0.0},
        "type": "conditional",
    }
    # No assignment occurred
    assert temp_feature_dict["graph"]["backward_edges"] == {
        "add_1": ["project_1"],
        "conditional_1": ["add_1", "gt_1"],
        "groupby_1": ["input_1"],
        "gt_1": ["project_1"],
        "project_1": ["groupby_1"],
    }


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
    with pytest.raises(TypeError) as exc_info:
        float_feature.preview(invalid_params)
    expected_error = (
        'type of argument "point_in_time_and_serving_name" must be a dict; got tuple instead'
    )
    assert expected_error in str(exc_info.value)


def test_feature_deserialization(
    float_feature, float_feature_dict, snowflake_feature_store, snowflake_event_view_with_entity
):
    """
    Test feature deserialization
    """
    global_graph = GlobalQueryGraph()
    global_graph_dict = global_graph.dict()
    float_feature_dict["_id"] = float_feature_dict.pop("id")
    float_feature_dict["feature_store"] = snowflake_feature_store
    deserialized_float_feature = Feature.parse_obj(float_feature_dict)
    assert deserialized_float_feature.saved is False
    assert deserialized_float_feature.id == float_feature.id
    assert deserialized_float_feature.name == float_feature.name
    assert deserialized_float_feature.var_type == float_feature.var_type
    assert deserialized_float_feature.node == float_feature.node
    assert deserialized_float_feature.graph.dict() == global_graph_dict
    assert deserialized_float_feature.row_index_lineage == float_feature.row_index_lineage
    assert deserialized_float_feature.tabular_source == float_feature.tabular_source
    assert deserialized_float_feature.graph == global_graph
    assert id(deserialized_float_feature.graph.nodes) == id(global_graph.nodes)
    tile_id1 = float_feature.graph.nodes["groupby_1"]["parameters"]["tile_id"]

    # construct another identical float feature with an additional unused column,
    # check that the tile_ids are different before serialization, serialized object are the same
    snowflake_event_view_with_entity["unused_feat"] = (
        10.0 * snowflake_event_view_with_entity["cust_id"]
    )
    grouped = snowflake_event_view_with_entity.groupby("cust_id")
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
    same_float_feature_dict = feature_group["sum_1d"].dict(
        exclude={"id": True, "feature_namespace_id": True}
    )
    tile_id2 = float_feature.graph.nodes["groupby_2"]["parameters"]["tile_id"]
    assert tile_id1 != tile_id2
    float_feature_dict.pop("_id")
    float_feature_dict.pop("feature_store")
    float_feature_dict.pop("feature_namespace_id")
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
    output_exclude_none = float_feature.json(exclude_none=True)
    assert "is_default" not in output_exclude_none

    # check encoder
    float_feature.__dict__["created_at"] = datetime.now()
    output_encoder = float_feature.json(encoder=lambda v: "__default__")
    assert '"created_at": "__default__"' in output_encoder


@pytest.fixture(name="saved_feature")
def saved_feature_fixture(
    snowflake_feature_store,
    snowflake_event_data,
    float_feature,
    mock_insert_feature_registry,
):
    """
    Saved feature fixture
    """
    _ = mock_insert_feature_registry
    event_data_id_before = snowflake_event_data.id
    snowflake_feature_store.save()
    snowflake_event_data.save()
    assert snowflake_event_data.id == event_data_id_before
    feature_id_before = float_feature.id
    assert float_feature.readiness is None
    assert float_feature.saved is False
    float_feature.save()
    assert float_feature.id == feature_id_before
    assert float_feature.readiness == FeatureReadiness.DRAFT
    assert float_feature.saved is True

    # test list features
    assert float_feature.name == "sum_1d"
    assert Feature.list() == ["sum_1d"]
    return float_feature


def test_feature_save__exception_due_to_event_data_not_saved(float_feature, snowflake_event_data):
    """
    Test feature save failure due to event data not saved
    """
    with pytest.raises(RecordCreationException) as exc:
        float_feature.save()
    expected_msg = (
        f'EventData (id: "{snowflake_event_data.id}") not found. '
        f"Please save the EventData object first."
    )
    assert expected_msg in str(exc.value)


def test_feature_save__exception_due_to_feature_saved_before(float_feature, saved_feature):
    """
    Test feature save failure due to event data not saved
    """
    import pdb

    pdb.set_trace()
    _ = saved_feature
    assert saved_feature.saved is True
    with pytest.raises(ObjectHasBeenSavedError) as exc:
        float_feature.save()
    expected_msg = f'Feature (id: "{float_feature.id}") has been saved before.'
    assert expected_msg in str(exc.value)


def test_feature_name__set_name_when_unnamed(float_feature):
    """
    Test setting name for unnamed features creates alias node
    """
    new_feature = float_feature + 1234

    assert new_feature.name is None
    assert new_feature.node.dict(exclude={"name": True}) == {
        "type": "add",
        "parameters": {"value": 1234},
        "output_type": "series",
    }
    old_node_name = new_feature.node.name

    new_feature.name = "my_feature_1234"
    assert new_feature.node.dict(exclude={"name": True}) == {
        "type": "alias",
        "parameters": {"name": "my_feature_1234"},
        "output_type": "series",
    }
    assert new_feature.graph.backward_edges[new_feature.node.name] == [old_node_name]


def test_feature_name__set_name_invalid_from_project(float_feature):
    """
    Test changing name for already named feature is not allowed
    """
    with pytest.raises(ValueError) as exc_info:
        float_feature.name = "my_new_feature"
    assert str(exc_info.value) == 'Feature "sum_1d" cannot be renamed to "my_new_feature"'


def test_feature_name__set_name_invalid_from_alias(float_feature):
    """
    Test changing name for already named feature is not allowed
    """
    new_feature = float_feature + 1234
    new_feature.name = "my_feature_1234"
    with pytest.raises(ValueError) as exc_info:
        new_feature.name = "my_feature_1234_v2"
    assert str(exc_info.value) == (
        'Feature "my_feature_1234" cannot be renamed to "my_feature_1234_v2"'
    )


def test_feature_name__set_name_invalid_none(float_feature):
    """
    Test setting name as None is not allowed
    """
    new_feature = float_feature + 1234
    with pytest.raises(ValueError) as exc_info:
        new_feature.name = None
    assert str(exc_info.value) == "None is not a valid feature name"


def test_get_feature(saved_feature):
    """
    Test get feature using feature name
    """
    feature = Feature.get(name=saved_feature.name)
    assert feature.saved is True
    assert feature.dict() == saved_feature.dict()
    get_by_id_feat = Feature.get_by_id(feature.id)
    assert get_by_id_feat == feature
    assert get_by_id_feat.saved is True

    # check audit history
    audit_history = saved_feature.audit()
    expected_pagination_info = {"page": 1, "page_size": 10, "total": 1}
    assert audit_history.items() > expected_pagination_info.items()
    history_data = audit_history["data"]
    assert len(history_data) == 1
    assert (
        history_data[0].items()
        > {
            "name": 'insert: "sum_1d"',
            "action_type": "INSERT",
            "previous_values": {},
        }.items()
    )
    assert (
        history_data[0]["current_values"].items()
        > {
            "name": "sum_1d",
            "readiness": "DRAFT",
            "var_type": "FLOAT",
            "updated_at": None,
            "user_id": None,
        }.items()
    )

    with pytest.raises(RecordRetrievalException) as exc:
        Feature.get(name="random_name")
    expected_msg = 'Feature (name: "random_name") not found. Please save the Feature object first.'
    assert expected_msg in str(exc.value)


def test_unary_op_inherits_event_data_id(float_feature):
    """
    Test unary operation inherits event_data_ids
    """
    new_feature = float_feature.isnull()
    assert new_feature.event_data_ids == float_feature.event_data_ids


def test_feature__default_version_info_retrieval(saved_feature):
    """
    Test get feature using feature name
    """
    feature = Feature.get(name=saved_feature.name)
    assert feature.is_default is True
    assert feature.default_version_mode == DefaultVersionMode.AUTO
    assert feature.default_readiness == FeatureReadiness.DRAFT
    assert feature.saved is True

    new_feature = feature.copy()
    new_feature.__dict__["_id"] = ObjectId()
    new_feature.__dict__["version"] = f"{new_feature.version}_1"
    new_feature.__dict__["saved"] = False
    new_feature.save()
    assert new_feature.is_default is True
    assert new_feature.default_version_mode == DefaultVersionMode.AUTO
    assert new_feature.default_readiness == FeatureReadiness.DRAFT

    # check that feature becomes non-default
    assert feature.is_default is False


def test_feature_derived_from_saved_feature_not_saved(saved_feature):
    """
    Test feature derived from saved feature is consider not saved
    """
    derived_feat = saved_feature + 1
    assert derived_feat.saved is False
