"""
Unit test for Feature & FeatureList classes
"""
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from bson.objectid import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature, FeatureNamespace
from featurebyte.api.feature_list import FeatureGroup
from featurebyte.exception import (
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure
from tests.util.helper import check_aggressively_pruned_graph, get_node


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
    assert set(node["name"] for node in feat_dict["graph"]["nodes"]) == {
        "input_1",
        "groupby_1",
        "project_1",
    }
    assert feat_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "groupby_1"},
        {"source": "groupby_1", "target": "project_1"},
    ]
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
    cond_node = get_node(float_feature_dict["graph"], "conditional_1")
    assert cond_node == {
        "name": "conditional_1",
        "type": "conditional",
        "parameters": {"value": 10},
        "output_type": "series",
    }
    assert float_feature_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "groupby_1"},
        {"source": "groupby_1", "target": "project_1"},
        {"source": "project_1", "target": "gt_1"},
        {"source": "project_1", "target": "conditional_1"},
        {"source": "gt_1", "target": "conditional_1"},
        {"source": "conditional_1", "target": "alias_1"},
    ]


def test_feature__cond_assign_unnamed(float_feature, bool_feature):
    """
    Test Feature conditional assignment on unnamed Feature
    """
    temp_feature = float_feature + 123.0
    temp_feature[bool_feature] = 0.0
    temp_feature_dict = temp_feature.dict()
    cond_node = get_node(temp_feature_dict["graph"], node_name="conditional_1")
    assert cond_node == {
        "name": "conditional_1",
        "output_type": "series",
        "parameters": {"value": 0.0},
        "type": "conditional",
    }
    # No assignment occurred
    assert temp_feature_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "groupby_1"},
        {"source": "groupby_1", "target": "project_1"},
        {"source": "project_1", "target": "gt_1"},
        {"source": "project_1", "target": "add_1"},
        {"source": "add_1", "target": "conditional_1"},
        {"source": "gt_1", "target": "conditional_1"},
    ]


def test_feature__preview_missing_point_in_time(float_feature):
    """
    Test feature preview validation missing point in time
    """
    invalid_params = {
        "cust_id": "C1",
    }
    with pytest.raises(RecordRetrievalException) as exc_info:
        float_feature.preview(invalid_params)
    assert "Point in time column not provided: POINT_IN_TIME" in str(exc_info.value)


def test_feature__preview_missing_entity_id(float_feature):
    """
    Test feature preview validation missing point in time
    """
    invalid_params = {
        "POINT_IN_TIME": "2022-04-01",
    }
    with pytest.raises(RecordRetrievalException) as exc_info:
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
    float_feature_dict["_id"] = float_feature_dict.pop("id")
    float_feature_dict["feature_store"] = snowflake_feature_store
    deserialized_float_feature = Feature.parse_obj(float_feature_dict)
    assert deserialized_float_feature.saved is False
    assert deserialized_float_feature.id == float_feature.id
    assert deserialized_float_feature.name == float_feature.name
    assert deserialized_float_feature.dtype == float_feature.dtype
    assert deserialized_float_feature.tabular_source == float_feature.tabular_source
    assert deserialized_float_feature.graph == global_graph
    assert id(deserialized_float_feature.graph.nodes) == id(global_graph.nodes)

    # construct another identical float feature with an additional unused column,
    snowflake_event_view_with_entity["unused_feat"] = (
        10.0 * snowflake_event_view_with_entity["cust_id"]
    )
    grouped = snowflake_event_view_with_entity.groupby("cust_id")
    feature_group = grouped.aggregate_over(
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
    float_feature_dict.pop("_id")
    float_feature_dict.pop("feature_store")
    float_feature_dict.pop("feature_namespace_id")

    # as serialization only perform non-aggressive pruning (all travelled nodes are kept)
    # here we need to perform aggressive pruning & compare the final graph to make sure they are the same
    check_aggressively_pruned_graph(
        left_obj_dict=float_feature_dict, right_obj_dict=same_float_feature_dict
    )


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
):
    """
    Saved feature fixture
    """
    event_data_id_before = snowflake_event_data.id
    snowflake_feature_store.save()
    snowflake_event_data.save()
    assert snowflake_event_data.id == event_data_id_before
    feature_id_before = float_feature.id
    assert float_feature.readiness is FeatureReadiness.DRAFT
    assert float_feature.saved is False

    # check the groupby node before feature is saved
    graph = QueryGraphModel(**float_feature.dict()["graph"])
    groupby_node = graph.nodes_map["groupby_1"]
    assert groupby_node.parameters.names == ["sum_30m", "sum_2h", "sum_1d"]
    assert groupby_node.parameters.windows == ["30m", "2h", "1d"]

    float_feature.save()
    assert float_feature.id == feature_id_before
    assert float_feature.readiness == FeatureReadiness.DRAFT
    assert float_feature.saved is True

    # check the groupby node after feature is saved (node parameters should get pruned)
    graph = QueryGraphModel(**float_feature.dict()["graph"])
    groupby_node = graph.nodes_map["groupby_1"]
    assert groupby_node.parameters.names == ["sum_1d"]
    assert groupby_node.parameters.windows == ["1d"]
    assert (
        groupby_node.parameters.tile_id
        == "TILE_F1800_M300_B600_99CB16A0CBF5645D5C2D1DEA5CA74D4BD1660817"
    )
    assert groupby_node.parameters.aggregation_id == "sum_072a1700018ba111c99ff5d80e934ef4dd5a9f85"

    # test list features
    assert float_feature.name == "sum_1d"
    float_feature_namespace = FeatureNamespace.get(float_feature.name)
    feature_list = Feature.list()
    assert_frame_equal(
        feature_list,
        pd.DataFrame(
            {
                "name": [float_feature_namespace.name],
                "dtype": [float_feature_namespace.dtype],
                "readiness": [float_feature_namespace.readiness],
                "online_enabled": [float_feature.online_enabled],
                "data": [["sf_event_data"]],
                "entities": [["customer"]],
                "created_at": [float_feature_namespace.created_at],
            }
        ),
    )

    return float_feature


def test_info(saved_feature):
    """
    Test info
    """
    info_dict = saved_feature.info()
    expected_info = {
        "name": "sum_1d",
        "dtype": "FLOAT",
        "entities": [{"name": "customer", "serving_names": ["cust_id"]}],
        "tabular_data": [{"name": "sf_event_data", "status": "DRAFT"}],
        "default_version_mode": "AUTO",
        "default_feature_id": str(saved_feature.id),
        "readiness": {"this": "DRAFT", "default": "DRAFT"},
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert "created_at" in info_dict, info_dict
    assert "version" in info_dict, info_dict
    assert set(info_dict["version"]) == {"this", "default"}, info_dict["version"]
    assert info_dict["versions_info"] is None, info_dict["versions_info"]

    verbose_info_dict = saved_feature.info(verbose=True)
    assert verbose_info_dict.items() > expected_info.items(), verbose_info_dict
    assert "created_at" in verbose_info_dict, verbose_info_dict
    assert "version" in verbose_info_dict, verbose_info_dict
    assert set(verbose_info_dict["version"]) == {"this", "default"}, verbose_info_dict["version"]

    assert "versions_info" in verbose_info_dict, verbose_info_dict
    assert len(verbose_info_dict["versions_info"]) == 1, verbose_info_dict
    assert set(verbose_info_dict["versions_info"][0]) == {
        "version",
        "readiness",
        "created_at",
    }, verbose_info_dict


def test_feature_save__exception_due_to_event_data_not_saved(float_feature, snowflake_event_data):
    """
    Test feature save failure due to event data not saved
    """
    with pytest.raises(RecordCreationException) as exc:
        float_feature.save()
    expected_msg = (
        f'TabularData (id: "{snowflake_event_data.id}") not found. '
        f"Please save the TabularData object first."
    )
    assert expected_msg in str(exc.value)


def test_feature_save__exception_due_to_feature_saved_before(float_feature, saved_feature):
    """
    Test feature save failure due to event data not saved
    """
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
    assert new_feature.graph.backward_edges_map[new_feature.node.name] == [old_node_name]


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
    assert get_by_id_feat.dict() == feature.dict()
    assert get_by_id_feat.saved is True

    # ensure Proxy object works in binary operations
    _ = get_by_id_feat == feature

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
            "dtype": "FLOAT",
            "updated_at": None,
            "user_id": None,
        }.items()
    )

    with pytest.raises(RecordRetrievalException) as exc:
        lazy_feature = Feature.get(name="random_name")
        _ = lazy_feature.name
    expected_msg = 'Feature (name: "random_name") not found. Please save the Feature object first.'
    assert expected_msg in str(exc.value)


def test_unary_op_inherits_event_data_id(float_feature):
    """
    Test unary operation inherits tabular_data_ids
    """
    new_feature = float_feature.isnull()
    assert new_feature.tabular_data_ids == float_feature.tabular_data_ids


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


def test_create_new_version(saved_feature):
    """Test creation a new version"""
    new_version = saved_feature.create_new_version(
        feature_job_setting=FeatureJobSetting(
            blind_spot="45m", frequency="30m", time_modulo_frequency="15m"
        )
    )

    assert new_version.id != saved_feature.id
    assert new_version.saved is True

    saved_feature_version = saved_feature.version
    assert saved_feature_version.suffix is None
    assert new_version.version == {"name": saved_feature_version.name, "suffix": 1}

    new_version_dict = new_version.dict()
    groupby_node = new_version_dict["graph"]["nodes"][1]
    groupby_node_params = groupby_node["parameters"]
    assert groupby_node["type"] == "groupby"
    assert groupby_node_params["blind_spot"] == 45 * 60
    assert groupby_node_params["frequency"] == 30 * 60
    assert groupby_node_params["time_modulo_frequency"] == 15 * 60


def test_create_new_version__error(float_feature):
    """Test creation a new version (exception)"""
    with pytest.raises(RecordCreationException) as exc:
        float_feature.create_new_version(
            feature_job_setting=FeatureJobSetting(
                blind_spot="45m", frequency="30m", time_modulo_frequency="15m"
            )
        )

    expected_msg = (
        f'Feature (id: "{float_feature.id}") not found. Please save the Feature object first.'
    )
    assert expected_msg in str(exc.value)


def test_composite_features(snowflake_event_data_with_entity):
    """Test composite features' property"""
    entity = Entity(name="binary", serving_names=["col_binary"])
    entity.save()

    # make col_binary as an entity column
    snowflake_event_data_with_entity.col_binary.as_entity("binary")

    event_view = EventView.from_event_data(snowflake_event_data_with_entity)
    feature_job_setting = {
        "blind_spot": "10m",
        "frequency": "30m",
        "time_modulo_frequency": "5m",
    }
    feature_group_by_cust_id = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_job_setting,
        feature_names=["sum_30m_by_cust_id"],
    )
    feature_group_by_binary = event_view.groupby("col_binary").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_job_setting,
        feature_names=["sum_30m_by_binary"],
    )
    composite_feature = (
        feature_group_by_cust_id["sum_30m_by_cust_id"]
        + feature_group_by_binary["sum_30m_by_binary"]
    )
    assert set(composite_feature.entity_identifiers) == {"cust_id", "col_binary"}


def test_update_readiness_and_default_version_mode(saved_feature):
    """Test update feature readiness"""
    assert saved_feature.readiness == FeatureReadiness.DRAFT
    saved_feature.update_readiness("PRODUCTION_READY")
    assert saved_feature.readiness == FeatureReadiness.PRODUCTION_READY
    saved_feature.update_readiness(FeatureReadiness.DRAFT)
    assert saved_feature.readiness == FeatureReadiness.DRAFT

    # check update on the same readiness
    saved_feature.update_readiness(FeatureReadiness.DRAFT)
    assert saved_feature.readiness == FeatureReadiness.DRAFT

    # check default version mode
    assert saved_feature.default_version_mode == DefaultVersionMode.AUTO
    saved_feature.update_default_version_mode("MANUAL")
    assert saved_feature.default_version_mode == DefaultVersionMode.MANUAL

    # test update on wrong readiness input
    with pytest.raises(ValueError) as exc:
        saved_feature.update_readiness("random")
    assert "'random' is not a valid FeatureReadiness" in str(exc.value)


def test_update_readiness_and_default_version_mode__unsaved_feature(float_feature):
    """Test update feature readiness on unsaved feature"""
    _ = float_feature
    with pytest.raises(RecordUpdateException) as exc:
        float_feature.update_readiness(FeatureReadiness.PRODUCTION_READY)
    expected = (
        f'Feature (id: "{float_feature.id}") not found. Please save the Feature object first.'
    )
    assert expected in str(exc.value)

    with pytest.raises(RecordRetrievalException) as exc:
        float_feature.update_default_version_mode(DefaultVersionMode.MANUAL)
    namespace_id = float_feature.feature_namespace_id
    expected = (
        f'FeatureNamespace (id: "{namespace_id}") not found. Please save the Feature object first.'
    )
    assert expected in str(exc.value)


def test_get_sql(float_feature):
    """Test get sql for feature"""
    assert float_feature.sql.endswith(
        'SELECT\n  "agg_w86400_sum_072a1700018ba111c99ff5d80e934ef4dd5a9f85" AS "sum_1d"\n'
        "FROM _FB_AGGREGATED AS AGG"
    )


def test_list_filter(saved_feature):
    """Test filters in list"""
    # test filter by data and entity
    feature_list = Feature.list(data="sf_event_data")
    assert feature_list.shape[0] == 1

    feature_list = Feature.list(data="other_data")
    assert feature_list.shape[0] == 0

    feature_list = Feature.list(entity="customer")
    assert feature_list.shape[0] == 1

    feature_list = Feature.list(entity="other_entity")
    assert feature_list.shape[0] == 0

    feature_list = Feature.list(data="sf_event_data", entity="customer")
    assert feature_list.shape[0] == 1

    feature_list = Feature.list(data="sf_event_data", entity="other_entity")
    assert feature_list.shape[0] == 0

    feature_list = Feature.list(data="other_data", entity="customer")
    assert feature_list.shape[0] == 0


def test_is_time_based(saved_feature):
    """
    Test is_time_based
    """
    # Default saved_feature is time based
    is_time_based = saved_feature.is_time_based
    assert is_time_based

    # Mock out GroupOperationStructure to have time-based property set to true
    with patch(
        "featurebyte.models.feature.FeatureModel.extract_operation_structure"
    ) as mocked_extract:
        mocked_extract.return_value = GroupOperationStructure(
            row_index_lineage=("item_groupby_1",),
            is_time_based=False,
        )
        assert not saved_feature.is_time_based
