"""
Unit test for Feature & FeatureList classes
"""
from datetime import datetime

import pytest

from featurebyte.api.feature import Feature, FeatureGroup
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node


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
        "groupby_1",
        "groupby_2",
    }
    assert float_feature.graph.edges == {
        "input_2": ["project_1", "groupby_1", "groupby_2"],
        "groupby_2": ["project_2"],
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
    assert feature_group_subset.protected_columns == {"cust_id"}
    assert feature_group_subset.inherited_columns == {"cust_id"}
    assert feature_group_subset.entity_identifiers == ["cust_id"]
    assert feature_group_subset.inception_node == feature_group.inception_node


def test_feature_group__getitem__series_key(feature_group, bool_feature):
    """
    Test filtering on feature group object
    """
    feature_group_subset = feature_group[bool_feature]
    assert isinstance(feature_group_subset, FeatureGroup)
    assert feature_group_subset.inception_node == feature_group.inception_node


def test_feature_group__override_protected_column(feature_group):
    """
    Test attempting to change feature group's protected column value
    """
    assert "cust_id" in feature_group.protected_columns
    with pytest.raises(ValueError) as exc:
        feature_group["cust_id"] = feature_group["cust_id"] * 2
    expected_msg = "Entity identifier column 'cust_id' cannot be modified!"
    assert expected_msg in str(exc.value)


def test_feature__binary_ops_return_feature_type(float_feature):
    """
    Test Feature return correct type after binary operation
    """
    output = float_feature + float_feature
    assert isinstance(output, Feature)
    assert output.inception_node == float_feature.inception_node
    assert output.protected_columns == float_feature.protected_columns


def test_feature__getitem__series_key(float_feature, bool_feature):
    """
    Test Feature filtering
    """
    output = float_feature[bool_feature]
    assert isinstance(output, Feature)
    assert output.inception_node == float_feature.inception_node
    assert output.protected_columns == float_feature.protected_columns

    with pytest.raises(TypeError) as exc:
        _ = bool_feature[float_feature]
    assert "Only boolean Series filtering is supported!" in str(exc.value)


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
    global_graph_dict = float_feature.graph.dict()
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
    assert float_feature.version is None
    output_exclude_none = float_feature.json(exclude_none=True)
    assert "version" not in output_exclude_none

    # check encoder
    float_feature.created_at = datetime.now()
    output_encoder = float_feature.json(encoder=lambda v: "__default__")
    assert '"created_at": "__default__"' in output_encoder
