"""
Unit test for Feature classes
"""

import textwrap
import time
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId
from freezegun import freeze_time
from pandas.testing import assert_frame_equal

import featurebyte
from featurebyte import (
    DefaultVersionMode,
    MissingValueImputation,
    activate_and_get_catalog,
    get_version,
    list_unsaved_features,
)
from featurebyte.api.catalog import Catalog
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature, FeatureNamespace
from featurebyte.api.feature_group import FeatureGroup
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.table import Table
from featurebyte.enum import DBVarType, SourceType
from featurebyte.exception import (
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordDeletionException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.models.feature_store import TableStatus
from featurebyte.models.relationship import RelationshipType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSetting,
    TableFeatureJobSetting,
    TableIdFeatureJobSetting,
)
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.cleaning_operation import (
    ColumnCleaningOperation,
    TableCleaningOperation,
)
from featurebyte.query_graph.node.generic import GroupByNode, ProjectNode
from featurebyte.query_graph.transform.null_filling_value import NullFillingValueExtractor
from featurebyte.query_graph.transform.offline_store_ingest import (
    OfflineStoreIngestQueryGraphTransformer,
)
from tests.unit.api.base_feature_or_target_test import FeatureOrTargetBaseTestSuite, TestItemType
from tests.util.helper import (
    check_aggressively_pruned_graph,
    check_decomposed_graph_output_node_hash,
    check_on_demand_feature_code_generation,
    check_sdk_code_generation,
    compare_pydantic_obj,
    deploy_features_through_api,
    get_node,
)


@pytest.fixture(name="float_feature_dict")
def float_feature_dict_fixture(float_feature):
    """
    Serialize float feature in dictionary format
    """
    # before serialization, global query graph is used
    assert float_feature.saved is False
    assert isinstance(float_feature.graph, GlobalQueryGraph)
    feat_dict = float_feature.model_dump()

    # after serialization, pruned query graph is used
    assert set(node["name"] for node in feat_dict["graph"]["nodes"]) == {
        "input_1",
        "groupby_1",
        "project_1",
        "graph_1",
    }
    assert feat_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "graph_1", "target": "groupby_1"},
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
    assert float_feature.node.model_dump(exclude={"name": True}) == {
        "type": "alias",
        "parameters": {"name": "sum_1d"},
        "output_type": "series",
    }
    float_feature_dict = float_feature.model_dump()
    cond_node = get_node(float_feature_dict["graph"], "conditional_1")
    assert cond_node == {
        "name": "conditional_1",
        "type": "conditional",
        "parameters": {"value": 10},
        "output_type": "series",
    }
    assert float_feature_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "graph_1", "target": "groupby_1"},
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
    temp_feature_dict = temp_feature.model_dump()
    cond_node = get_node(temp_feature_dict["graph"], node_name="conditional_1")
    assert cond_node == {
        "name": "conditional_1",
        "output_type": "series",
        "parameters": {"value": 0.0},
        "type": "conditional",
    }
    # No assignment occurred
    assert temp_feature_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "graph_1", "target": "groupby_1"},
        {"source": "groupby_1", "target": "project_1"},
        {"source": "project_1", "target": "gt_1"},
        {"source": "project_1", "target": "add_1"},
        {"source": "add_1", "target": "conditional_1"},
        {"source": "gt_1", "target": "conditional_1"},
    ]


def test_feature__cond_assign_different_groupby_operations(
    snowflake_event_view_with_entity, feature_group_feature_job_setting
):
    """
    Test conditional assignment of features not limited by Column's row index lineage
    """
    view = snowflake_event_view_with_entity
    feature_1 = view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["12h"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["feature_1"],
    )["feature_1"]
    feature_2 = view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="min",
        windows=["12h"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["feature_2"],
    )["feature_2"]
    feature_1[feature_2 < 0] = 100
    feature_dict = feature_1.model_dump()
    cond_node = get_node(feature_dict["graph"], "conditional_1")
    assert cond_node == {
        "name": "conditional_1",
        "type": "conditional",
        "parameters": {"value": 100},
        "output_type": "series",
    }


def test_feature__preview_missing_point_in_time(float_feature):
    """
    Test feature preview validation missing point in time
    """
    invalid_params = pd.DataFrame([{"cust_id": "C1"}])
    with pytest.raises(RecordRetrievalException) as exc_info:
        float_feature.preview(invalid_params)
    assert "Point in time column not provided: POINT_IN_TIME" in str(exc_info.value)


def test_feature__preview_missing_entity_id(float_feature):
    """
    Test feature preview validation missing required entity
    """
    invalid_params = pd.DataFrame([{"POINT_IN_TIME": "2022-04-01"}])
    with pytest.raises(RecordRetrievalException) as exc_info:
        float_feature.preview(invalid_params)
    assert "Required entities are not provided in the request" in str(exc_info.value)


def test_feature_deserialization(
    float_feature, float_feature_dict, snowflake_feature_store, snowflake_event_view_with_entity
):
    """
    Test feature deserialization
    """
    global_graph = GlobalQueryGraph()
    float_feature_dict["_id"] = float_feature_dict.pop("id")
    float_feature_dict["feature_store"] = snowflake_feature_store
    deserialized_float_feature = Feature.model_validate(float_feature_dict)
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
        feature_job_setting=FeatureJobSetting(
            blind_spot="10m",
            period="30m",
            offset="5m",
        ),
    )
    same_float_feature_dict = feature_group["sum_1d"].model_dump(exclude={"id": True})
    float_feature_dict.pop("_id")
    float_feature_dict.pop("feature_store")

    # as serialization only perform non-aggressive pruning (all travelled nodes are kept)
    # here we need to perform aggressive pruning & compare the final graph to make sure they are the same
    check_aggressively_pruned_graph(
        left_obj_dict=float_feature_dict, right_obj_dict=same_float_feature_dict
    )

    # check pruned graph and node_name are set properly
    pruned_graph, mapped_node = feature_group["sum_1d"].extract_pruned_graph_and_node()  # noqa: F841
    assert mapped_node.name != feature_group["sum_1d"].node_name


def test_feature_to_json(float_feature):
    """
    Test feature to_json
    """
    # do not include any keys
    output_include = float_feature.model_dump_json(include={})
    assert output_include == "{}"

    # exclude graph key
    output_exclude = float_feature.model_dump_json(exclude={"graph": True})
    assert "graph" not in output_exclude

    # exclude_none
    output_exclude_none = float_feature.model_dump_json(exclude_none=True)
    assert "is_default" not in output_exclude_none


@pytest.fixture(name="saved_feature")
def saved_feature_fixture(
    snowflake_event_table,
    float_feature,
):
    """
    Saved feature fixture
    """
    event_table_id_before = snowflake_event_table.id
    snowflake_event_table.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(blind_spot="10m", period="30m", offset="5m")
    )
    assert snowflake_event_table.id == event_table_id_before
    feature_id_before = float_feature.id
    assert float_feature.readiness is FeatureReadiness.DRAFT
    assert float_feature.saved is False

    # check the groupby node before feature is saved
    graph = QueryGraphModel(**float_feature.model_dump()["graph"])
    groupby_node = graph.nodes_map["groupby_1"]
    assert groupby_node.parameters.names == ["sum_30m", "sum_2h", "sum_1d"]
    assert groupby_node.parameters.windows == ["30m", "2h", "1d"]

    float_feature.save()
    assert float_feature.id == feature_id_before
    assert float_feature.readiness == FeatureReadiness.DRAFT
    assert float_feature.saved is True
    assert float_feature.cached_model.definition_hash == "e2778a5f89053e279269b5b177e33f3ac51c18b8"

    # check the groupby node after feature is saved (node parameters should get pruned)
    graph = QueryGraphModel(**float_feature.model_dump()["graph"])
    groupby_node = graph.nodes_map["groupby_1"]
    assert groupby_node.parameters.names == ["sum_1d"]
    assert groupby_node.parameters.windows == ["1d"]
    assert groupby_node.parameters.tile_id == "TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295"
    assert groupby_node.parameters.aggregation_id == "sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"

    assert float_feature.name == "sum_1d"
    return float_feature


def test_list(saved_feature):
    """
    Test list features
    """
    # test list features
    saved_feature_namespace = FeatureNamespace.get(saved_feature.name)
    feature_list = Feature.list(include_id=True)
    assert_frame_equal(
        feature_list,
        pd.DataFrame({
            "id": [str(saved_feature.id)],
            "name": [saved_feature_namespace.name],
            "dtype": [saved_feature.dtype],
            "readiness": [saved_feature_namespace.readiness],
            "online_enabled": [saved_feature.online_enabled],
            "tables": [["sf_event_table"]],
            "primary_tables": [["sf_event_table"]],
            "entities": [["customer"]],
            "primary_entities": [["customer"]],
            "created_at": [saved_feature_namespace.created_at.isoformat()],
        }),
    )


def test_info(saved_feature):
    """
    Test info
    """
    info_dict = saved_feature.info()
    data_feature_job_setting = {
        "table_name": "sf_event_table",
        "feature_job_setting": {
            "blind_spot": "600s",
            "period": "1800s",
            "offset": "300s",
            "execution_buffer": "0s",
        },
    }
    expected_info = {
        "name": "sum_1d",
        "dtype": "FLOAT",
        "entities": [{"name": "customer", "serving_names": ["cust_id"], "catalog_name": "catalog"}],
        "primary_entity": [
            {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "catalog"}
        ],
        "tables": [{"name": "sf_event_table", "status": "PUBLIC_DRAFT", "catalog_name": "catalog"}],
        "table_feature_job_setting": {
            "this": [data_feature_job_setting],
            "default": [data_feature_job_setting],
        },
        "table_cleaning_operation": {"this": [], "default": []},
        "default_version_mode": "AUTO",
        "default_feature_id": str(saved_feature.id),
        "readiness": {"this": "DRAFT", "default": "DRAFT"},
        "catalog_name": "catalog",
        "namespace_description": None,
        "description": None,
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

    # check database details at input node are not included
    node = saved_feature.cached_model.graph.get_node_by_name("input_1")
    assert node.parameters.feature_store_details.details is None


def test_feature_save__exception_due_to_feature_saved_before(float_feature, saved_feature):
    """
    Test feature save failure due to event table not saved
    """
    _ = saved_feature
    assert saved_feature.saved is True
    with pytest.raises(ObjectHasBeenSavedError) as exc:
        float_feature.save()
    expected_msg = f'Feature (id: "{float_feature.id}") has been saved before.'
    assert expected_msg in str(exc.value)

    # check that saving a saved feature twice won't throw ObjectHasBeenSavedError
    feature_id = saved_feature.id
    saved_feature.save(conflict_resolution="retrieve")
    assert saved_feature.id == feature_id


def test_feature_name__set_name_when_unnamed(float_feature):
    """
    Test setting name for unnamed features creates alias node
    """
    new_feature = float_feature + 1234

    assert new_feature.name is None
    assert new_feature.node.model_dump(exclude={"name": True}) == {
        "type": "add",
        "parameters": {"value": 1234, "right_op": False},
        "output_type": "series",
    }
    old_node_name = new_feature.node.name

    new_feature.name = "my_feature_1234"
    assert new_feature.node.model_dump(exclude={"name": True}) == {
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
    assert feature.model_dump() == saved_feature.model_dump()
    get_by_id_feat = Feature.get_by_id(feature.id)
    assert get_by_id_feat.model_dump() == feature.model_dump()
    assert get_by_id_feat.saved is True

    # ensure Proxy object works in binary operations
    _ = get_by_id_feat == feature

    # check audit history
    audit_history = saved_feature.audit()
    assert (audit_history["action_type"] == "INSERT").all()
    assert (audit_history["name"] == 'insert: "sum_1d"').all()
    assert audit_history["old_value"].isnull().all()
    assert set(audit_history["field_name"]) == {
        "tabular_source.table_details.table_name",
        "updated_at",
        "tabular_source.feature_store_id",
        "table_ids",
        "primary_entity_ids",
        "primary_table_ids",
        "user_id",
        "tabular_source.table_details.database_name",
        "feature_namespace_id",
        "online_enabled",
        "created_at",
        "entity_dtypes",
        "entity_ids",
        "entity_join_steps",
        "version.name",
        "feature_list_ids",
        "raw_graph.nodes",
        "raw_graph.edges",
        "node_name",
        "offline_store_info",
        "version.suffix",
        "deployed_feature_list_ids",
        "tabular_source.table_details.schema_name",
        "name",
        "readiness",
        "relationships_info",
        "table_id_column_names",
        "table_id_cleaning_operations",
        "table_id_feature_job_settings",
        "graph.edges",
        "graph.nodes",
        "dtype",
        "catalog_id",
        "definition",
        "definition_hash",
        "user_defined_function_ids",
        "block_modification_by",
        "aggregation_ids",
        "aggregation_result_names",
        "agg_result_name_include_serving_names",
        "description",
        "online_store_table_names",
        "last_updated_by_scheduled_task_at",
        "is_deleted",
    }

    with pytest.raises(RecordRetrievalException) as exc:
        Feature.get(name="random_name")

    expected_msg = 'FeatureNamespace (name: "random_name") not found. Please save the FeatureNamespace object first.'
    assert expected_msg in str(exc.value)


def test_unary_op_inherits_event_table_id(float_feature):
    """
    Test unary operation inherits table_ids
    """
    new_feature = float_feature.isnull()
    assert new_feature.table_ids == float_feature.table_ids


def test_feature__default_version_info_retrieval(
    saved_feature, snowflake_event_table, mock_api_object_cache
):
    """
    Test get feature using feature name
    """
    _ = mock_api_object_cache
    feature = Feature.get(name=saved_feature.name)
    feature_model = feature.cached_model
    assert feature.is_default is True
    assert feature.default_version_mode == DefaultVersionMode.AUTO
    assert feature.default_readiness == FeatureReadiness.DRAFT
    assert feature.saved is True

    new_feature = feature.create_new_version(
        table_feature_job_settings=[
            TableFeatureJobSetting(
                table_name=snowflake_event_table.name,
                feature_job_setting=FeatureJobSetting(blind_spot="45m", period="30m", offset="15m"),
            )
        ],
    )
    assert new_feature.is_default is True
    assert new_feature.default_version_mode == DefaultVersionMode.AUTO
    assert new_feature.default_readiness == FeatureReadiness.DRAFT

    # check the derived attribute is regenerated
    new_feature_model = new_feature.cached_model
    assert new_feature_model.table_id_feature_job_settings == [
        TableIdFeatureJobSetting(
            table_id=snowflake_event_table.id,
            feature_job_setting=FeatureJobSetting(
                blind_spot="2700s", period="1800s", offset="900s"
            ),
        )
    ]
    assert new_feature_model.definition_hash != feature_model.definition_hash

    # check that feature becomes non-default
    assert feature.is_default is False


def test_feature_derived_from_saved_feature_not_saved(saved_feature):
    """
    Test feature derived from saved feature is consider not saved
    """
    derived_feat = saved_feature + 1
    assert derived_feat.saved is False


def test_create_new_version(saved_feature, snowflake_event_table):
    """Test creation a new version"""
    new_version = saved_feature.create_new_version(
        table_feature_job_settings=[
            TableFeatureJobSetting(
                table_name=snowflake_event_table.name,
                feature_job_setting=FeatureJobSetting(blind_spot="45m", period="30m", offset="15m"),
            )
        ],
        table_cleaning_operations=None,
    )

    assert new_version.id != saved_feature.id
    assert new_version.saved is True

    saved_feature_version = saved_feature.version
    assert new_version.version == f"{saved_feature_version}_1"

    new_version_dict = new_version.model_dump()
    assert new_version_dict["graph"]["nodes"][1]["type"] == "graph"
    groupby_node = new_version_dict["graph"]["nodes"][2]
    groupby_node_params = groupby_node["parameters"]
    assert groupby_node["type"] == "groupby"
    assert groupby_node_params["feature_job_setting"] == {
        "blind_spot": "2700s",
        "period": "1800s",
        "offset": "900s",
        "execution_buffer": "0s",
    }

    # before deletion
    _ = Feature.get_by_id(new_version.id)

    # check feature deletion
    new_version.delete()
    assert new_version.saved is False

    # check that feature is no longer retrievable
    with pytest.raises(RecordRetrievalException) as exc_info:
        Feature.get_by_id(new_version.id)
    expected_msg = (
        f'Feature (id: "{new_version.id}") not found. Please save the Feature object first.'
    )
    assert expected_msg in str(exc_info.value)


def test_delete_feature(saved_feature):
    """Test deletion of feature"""
    assert saved_feature.readiness == FeatureReadiness.DRAFT
    saved_feature.delete()
    assert saved_feature.saved is False

    with pytest.raises(RecordRetrievalException) as exc_info:
        Feature.get_by_id(saved_feature.id)

    expected_msg = (
        f'Feature (id: "{saved_feature.id}") not found. Please save the Feature object first.'
    )
    assert expected_msg in str(exc_info.value)


def test_create_new_version__with_data_cleaning_operations(
    saved_feature, snowflake_event_table, update_fixtures
):
    """Test creation of new version with table cleaning operations"""
    # check sdk code generation of source feature
    check_sdk_code_generation(saved_feature, to_use_saved_data=True)

    # create a new feature version
    new_version = saved_feature.create_new_version(
        table_feature_job_settings=[
            TableFeatureJobSetting(
                table_name=snowflake_event_table.name,
                feature_job_setting=FeatureJobSetting(blind_spot="45m", period="30m", offset="15m"),
            )
        ],
        table_cleaning_operations=[
            TableCleaningOperation(
                table_name="sf_event_table",
                column_cleaning_operations=[
                    # column to be aggregated on
                    ColumnCleaningOperation(
                        column_name="col_float",
                        cleaning_operations=[MissingValueImputation(imputed_value=0.0)],
                    ),
                    # group by column
                    ColumnCleaningOperation(
                        column_name="cust_id",
                        cleaning_operations=[MissingValueImputation(imputed_value=-999)],
                    ),
                    # unused column
                    ColumnCleaningOperation(
                        column_name="col_int",
                        cleaning_operations=[MissingValueImputation(imputed_value=0)],
                    ),
                ],
            )
        ],
    )

    # check sdk code generation for newly created feature
    check_sdk_code_generation(
        new_version,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/feature_time_based_with_data_cleaning_operations.py",
        update_fixtures=update_fixtures,
        table_id=saved_feature.table_ids[0],
    )

    # create another new feature version without table cleaning operations
    version_without_clean_ops = new_version.create_new_version(
        table_cleaning_operations=[
            TableCleaningOperation(table_name="sf_event_table", column_cleaning_operations=[])
        ]
    )
    check_sdk_code_generation(version_without_clean_ops, to_use_saved_data=True)


def test_create_new_version__error(float_feature):
    """Test creation a new version (exception)"""
    with pytest.raises(RecordCreationException) as exc:
        float_feature.create_new_version(
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name="sf_event_table",
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="45m", period="30m", offset="15m"
                    ),
                )
            ],
            table_cleaning_operations=None,
        )

    expected_msg = (
        f'Feature (id: "{float_feature.id}") not found. Please save the Feature object first.'
    )
    assert expected_msg in str(exc.value)


def test_feature__as_default_version(saved_feature):
    """Test feature as_default_version method"""
    new_version = saved_feature.create_new_version(
        table_feature_job_settings=[
            TableFeatureJobSetting(
                table_name="sf_event_table",
                feature_job_setting=FeatureJobSetting(blind_spot="15m", period="30m", offset="15m"),
            )
        ],
        table_cleaning_operations=None,
    )
    assert new_version.is_default is True
    assert new_version.default_version_mode == "AUTO"

    # check setting default version fails when default version mode is not MANUAL
    with pytest.raises(RecordUpdateException) as exc:
        saved_feature.as_default_version()
    expected = "Cannot set default feature ID when default version mode is not MANUAL."
    assert expected in str(exc.value)

    # check get by name use the default version
    assert Feature.get(name=saved_feature.name) == new_version

    # check setting default version manually
    assert new_version.is_default is True
    assert saved_feature.is_default is False
    saved_feature.update_default_version_mode(DefaultVersionMode.MANUAL)
    saved_feature.as_default_version()
    assert new_version.is_default is False
    assert saved_feature.is_default is True

    # check get by name use the default version
    assert Feature.get(name=saved_feature.name) == saved_feature

    # check get by name and version
    assert Feature.get(name=saved_feature.name, version=new_version.version) == new_version

    # check logic to prevent setting lower readiness version as default
    saved_feature.update_readiness(FeatureReadiness.PUBLIC_DRAFT)
    with pytest.raises(RecordUpdateException) as exc:
        new_version.as_default_version()

    error_msg = (
        f"Cannot set default feature ID to {new_version.id} "
        f"because its readiness level (DRAFT) is lower than the readiness level of "
        f"version {saved_feature.version} (PUBLIC_DRAFT)."
    )
    assert error_msg in str(exc.value)
    assert new_version.is_default is False
    assert saved_feature.is_default is True

    new_version.update_readiness(FeatureReadiness.PUBLIC_DRAFT)
    new_version.as_default_version()
    assert new_version.is_default is True
    assert saved_feature.is_default is False


def check_offline_store_ingest_graph_on_composite_feature(
    feature_model, cust_entity_id, transaction_entity_id
):
    """Check offline store ingest graph on composite feature"""
    # case 1: no entity relationship
    assert feature_model.relationships_info == []
    ingest_query_graphs = (
        feature_model.offline_store_info.extract_offline_store_ingest_query_graphs()
    )

    # check the first offline store ingest query graph
    assert len(ingest_query_graphs) == 2
    if ingest_query_graphs[0].node_name == "project_1":
        ingest_query_graph1 = ingest_query_graphs[0]
        ingest_query_graph2 = ingest_query_graphs[1]
    else:
        ingest_query_graph1 = ingest_query_graphs[1]
        ingest_query_graph2 = ingest_query_graphs[0]

    assert ingest_query_graph1.output_column_name.startswith(
        f"__{feature_model.versioned_name}__part"
    )
    assert ingest_query_graph1.primary_entity_ids == [transaction_entity_id]
    assert ingest_query_graph1.graph.edges_map == {
        "input_1": ["graph_1"],
        "graph_1": ["groupby_1"],
        "groupby_1": ["project_1"],
    }
    out_node = ingest_query_graph1.graph.get_node_by_name(ingest_query_graph1.node_name)
    assert isinstance(out_node, ProjectNode)
    assert out_node.parameters.columns == ["sum_30m_by_bool"]

    # check the second offline store ingest query graph
    assert ingest_query_graph2.output_column_name.startswith(
        f"__{feature_model.versioned_name}__part"
    )
    assert ingest_query_graph2.node_name == "add_1"
    assert ingest_query_graph2.primary_entity_ids == [cust_entity_id]
    groupby_node1 = ingest_query_graph2.graph.get_node_by_name("groupby_1")
    assert isinstance(groupby_node1, GroupByNode)
    assert groupby_node1.parameters.names == ["sum_30m_by_cust_id_1h"]
    groupby_node2 = ingest_query_graph2.graph.get_node_by_name("groupby_2")
    assert isinstance(groupby_node2, GroupByNode)
    assert groupby_node2.parameters.names == ["sum_30m_by_cust_id_30m"]

    # check decomposed graph
    transformer = OfflineStoreIngestQueryGraphTransformer(graph=feature_model.graph)
    output = transformer.transform(
        target_node=feature_model.node,
        relationships_info=feature_model.relationships_info,
        feature_name=feature_model.name,
        feature_version=feature_model.version.to_str(),
    )
    assert output.graph.edges_map == {
        "graph_1": ["add_1"],
        "graph_2": ["add_1"],
        "add_1": ["alias_1"],
    }

    # check the output node hash before and after decomposition
    check_decomposed_graph_output_node_hash(feature_model=feature_model, output=output)
    check_on_demand_feature_code_generation(feature_model=feature_model)

    # case 2: with entity relationship between the two entities (expect no query graph decomposition)
    entity_ids = feature_model.entity_ids
    relative_entity_id = ObjectId()
    relationships_info = [
        EntityRelationshipInfo(
            relationship_type=RelationshipType.CHILD_PARENT,
            entity_id=entity_ids[0],
            related_entity_id=relative_entity_id,
            relation_table_id=ObjectId(),
        ),
        EntityRelationshipInfo(
            relationship_type=RelationshipType.CHILD_PARENT,
            entity_id=relative_entity_id,
            related_entity_id=entity_ids[1],
            relation_table_id=ObjectId(),
        ),
    ]
    new_feature_model = feature_model.model_copy(update={"relationships_info": relationships_info})
    transformer = OfflineStoreIngestQueryGraphTransformer(graph=new_feature_model.graph)
    decomposed_result = transformer.transform(
        target_node=new_feature_model.node,
        relationships_info=new_feature_model.relationships_info,
        feature_name=new_feature_model.name,
        feature_version=new_feature_model.version.to_str(),
    )
    assert decomposed_result.is_decomposed is False


def test_composite_features(
    snowflake_event_table_with_entity,
    cust_id_entity,
    mock_deployment_flow,
):
    """Test composite features' property"""
    _ = mock_deployment_flow
    entity = Entity(name="bool", serving_names=["col_bool"])
    entity.save()

    # make col_binary as an entity column
    snowflake_event_table_with_entity.col_boolean.as_entity("bool")

    event_view = snowflake_event_table_with_entity.get_view()
    feature_job_setting = FeatureJobSetting(
        blind_spot="10m",
        period="30m",
        offset="5m",
    )
    feature_group_by_cust_id_30m = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_job_setting,
        feature_names=["sum_30m_by_cust_id_30m"],
    )
    feature_group_by_cust_id_1h = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["1h"],
        feature_job_setting=feature_job_setting,
        feature_names=["sum_30m_by_cust_id_1h"],
    )
    feature_group_by_bool = event_view.groupby("col_boolean").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_job_setting,
        feature_names=["sum_30m_by_bool"],
    )
    composite_feature = (
        feature_group_by_cust_id_30m["sum_30m_by_cust_id_30m"]
        + feature_group_by_cust_id_1h["sum_30m_by_cust_id_1h"]
        + feature_group_by_bool["sum_30m_by_bool"]
    )

    assert composite_feature.primary_entity == [
        Entity.get_by_id(cust_id_entity.id),
        Entity.get_by_id(entity.id),
    ]
    assert composite_feature.entity_ids == sorted([cust_id_entity.id, entity.id])
    assert composite_feature.graph.get_entity_columns(node_name=composite_feature.node_name) == [
        "col_boolean",
        "cust_id",
    ]

    # save the feature first
    composite_feature.name = "composite_feature"
    composite_feature.save()
    deploy_features_through_api([composite_feature])

    # get the offline store ingest query graphs
    feature_model = composite_feature.cached_model
    assert isinstance(feature_model, FeatureModel)
    check_offline_store_ingest_graph_on_composite_feature(
        feature_model, cust_id_entity.id, entity.id
    )


def test_offline_store_ingest_query_graphs__without_graph_decomposition(
    saved_feature, mock_deployment_flow
):
    """Test offline store ingest query graphs"""
    _ = mock_deployment_flow

    deploy_features_through_api([saved_feature])
    feature_model = saved_feature.cached_model
    assert isinstance(feature_model, FeatureModel)
    ingest_query_graphs = (
        feature_model.offline_store_info.extract_offline_store_ingest_query_graphs()
    )
    assert len(ingest_query_graphs) == 1
    assert ingest_query_graphs[0].graph == feature_model.graph
    assert ingest_query_graphs[0].node_name == feature_model.node_name
    assert ingest_query_graphs[0].ref_node_name is None


def test_update_readiness_and_default_version_mode(saved_feature):
    """Test update feature readiness"""
    assert saved_feature.readiness == FeatureReadiness.DRAFT
    saved_feature.update_readiness("PRODUCTION_READY")
    assert saved_feature.readiness == FeatureReadiness.PRODUCTION_READY
    saved_feature.update_readiness(FeatureReadiness.PUBLIC_DRAFT)
    assert saved_feature.readiness == FeatureReadiness.PUBLIC_DRAFT

    # check update on the same readiness
    saved_feature.update_readiness(FeatureReadiness.PUBLIC_DRAFT)
    assert saved_feature.readiness == FeatureReadiness.PUBLIC_DRAFT

    # check default version mode
    assert saved_feature.default_version_mode == DefaultVersionMode.AUTO
    saved_feature.update_default_version_mode("MANUAL")
    assert saved_feature.default_version_mode == DefaultVersionMode.MANUAL

    # test update on wrong readiness input
    with pytest.raises(ValueError) as exc:
        saved_feature.update_readiness("random")
    assert "'random' is not a valid FeatureReadiness" in str(exc.value)


def test_update_readiness__fail_guardrail_checks(saved_feature):
    """Test update feature readiness fail guardrail checks"""
    assert saved_feature.readiness == FeatureReadiness.DRAFT
    saved_feature.update_readiness(FeatureReadiness.PRODUCTION_READY)

    # create a new feature and try to update it production ready
    new_feature = saved_feature.create_new_version(
        table_feature_job_settings=[
            TableFeatureJobSetting(
                table_name="sf_event_table",
                feature_job_setting=FeatureJobSetting(blind_spot="45m", period="30m", offset="15m"),
            )
        ],
    )

    # update saved_feature to PRODUCTION_READY should fail guardrail checks
    # test fail guardrail check due to existing feature version that is production ready
    with pytest.raises(RecordUpdateException) as exc:
        new_feature.update_readiness(FeatureReadiness.PRODUCTION_READY)

    expected_msg = (
        "Found another feature version that is already PRODUCTION_READY. "
        f'Please deprecate the feature "sum_1d" with ID {saved_feature.id} first before promoting the promoted '
        "version as there can only be one feature version that is production ready at any point in time. "
        f"We are unable to promote the feature with ID {new_feature.id} right now."
    )
    assert expected_msg in str(exc.value)

    # test fail guardrail check due to using deprecated table
    saved_feature.update_readiness(FeatureReadiness.PUBLIC_DRAFT)

    table = Table.get_by_id(saved_feature.table_ids[0])
    table.update_status(TableStatus.DEPRECATED)

    with pytest.raises(RecordUpdateException) as exc:
        new_feature.update_readiness(FeatureReadiness.PRODUCTION_READY)

    expected_msg = (
        'Found a deprecated table "sf_event_table" that is used by the feature "sum_1d". '
        "We are unable to promote the feature to PRODUCTION_READY right now."
    )
    assert expected_msg in str(exc.value)


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
    assert expected in str(exc.value)


def test_get_sql(float_feature):
    """Test get sql for feature"""
    assert float_feature.sql.endswith(
        'SELECT\n  CAST("_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS DOUBLE) AS "sum_1d"\nFROM _FB_AGGREGATED AS AGG'
    )


def test_list_filter(saved_feature):
    """Test filters in list"""
    # test filter by table and entity
    feature_list = Feature.list(primary_table="sf_event_table")
    assert feature_list.shape[0] == 1

    feature_list = Feature.list(primary_table="other_data", include_id=True)
    assert feature_list.shape[0] == 0

    feature_list = Feature.list(primary_entity="customer")
    assert feature_list.shape[0] == 1

    feature_list = Feature.list(primary_entity="other_entity")
    assert feature_list.shape[0] == 0

    feature_list = Feature.list(primary_table="sf_event_table", primary_entity="customer")
    assert feature_list.shape[0] == 1

    feature_list = Feature.list(primary_table=["sf_event_table"], primary_entity=["customer"])
    assert feature_list.shape[0] == 1

    feature_list = Feature.list(primary_table="sf_event_table", primary_entity="other_entity")
    assert feature_list.shape[0] == 0

    feature_list = Feature.list(primary_table=["sf_event_table"], primary_entity=["other_entity"])
    assert feature_list.shape[0] == 0

    feature_list = Feature.list(primary_table="other_data", primary_entity="customer")
    assert feature_list.shape[0] == 0


def test_is_time_based(saved_feature, non_time_based_feature):
    """
    Test is_time_based
    """
    # window aggregation feature is time based
    assert saved_feature.is_time_based is True

    # item aggregation feature is not time based
    assert non_time_based_feature.is_time_based is False


def test_list_versions(saved_feature):
    """
    Test list_versions
    """
    # save a few more features
    feature_group = FeatureGroup(items=[])
    feature_group["new_feat1"] = saved_feature + 1
    feature_group["new_feat2"] = saved_feature + 2
    feature_group.save()

    # check feature class list_version & feature object list_versions
    assert_frame_equal(
        Feature.list_versions(),
        pd.DataFrame({
            "id": [
                str(feature_group["new_feat2"].id),
                str(feature_group["new_feat1"].id),
                str(saved_feature.id),
            ],
            "name": ["new_feat2", "new_feat1", saved_feature.name],
            "version": [saved_feature.version] * 3,
            "dtype": [saved_feature.dtype] * 3,
            "readiness": [saved_feature.readiness] * 3,
            "online_enabled": [saved_feature.online_enabled] * 3,
            "tables": [["sf_event_table"]] * 3,
            "primary_tables": [["sf_event_table"]] * 3,
            "entities": [["customer"]] * 3,
            "primary_entities": [["customer"]] * 3,
            "created_at": [
                feature_group[
                    "new_feat2"
                ].cached_model.created_at.isoformat(),  # DEV-1820: created_at is not synced
                feature_group[
                    "new_feat1"
                ].cached_model.created_at.isoformat(),  # DEV-1820: created_at is not synced
                saved_feature.created_at.isoformat(),
            ],
            "is_default": [True] * 3,
        }),
    )
    assert_frame_equal(
        saved_feature.list_versions(),
        pd.DataFrame({
            "id": [str(saved_feature.id)],
            "name": [saved_feature.name],
            "version": [saved_feature.version],
            "dtype": [saved_feature.dtype],
            "readiness": [saved_feature.readiness],
            "online_enabled": [saved_feature.online_enabled],
            "tables": [["sf_event_table"]],
            "primary_tables": [["sf_event_table"]],
            "entities": [["customer"]],
            "primary_entities": [["customer"]],
            "created_at": [saved_feature.created_at.isoformat()],
            "is_default": [True],
        }),
    )

    # check documentation of the list_versions
    assert Feature.list_versions.__doc__ == Feature._list_versions.__doc__
    assert (
        saved_feature.list_versions.__doc__ == saved_feature._list_versions_with_same_name.__doc__
    )


@freeze_time("2023-01-20 03:20:00")
def test_get_feature_jobs_status(saved_feature, feature_job_logs, update_fixtures):
    """
    Test get_feature_jobs_status
    """
    if update_fixtures:
        # update job_logs.csv fixture
        log_fixture_path = "tests/fixtures/feature_job_status/job_logs.csv"
        feature_job_logs = pd.read_csv(log_fixture_path)
        feature_job_logs["CREATED_AT"] = pd.to_datetime(feature_job_logs["CREATED_AT"])

        # extract tile ID and aggregation ID from the feature
        groupby_node = saved_feature.extract_pruned_graph_and_node()[0].get_node_by_name(
            "groupby_1"
        )
        tile_id = groupby_node.parameters.tile_id
        feature_job_logs["AGGREGATION_ID"] = groupby_node.parameters.aggregation_id
        feature_job_logs["SESSION_ID"] = feature_job_logs["SESSION_ID"].apply(
            lambda x: f"{tile_id}|{x.split('|')[1]}"
        )
        feature_job_logs.to_csv(log_fixture_path, index=False)

    with patch(
        "featurebyte.service.tile_job_log.TileJobLogService.get_logs_dataframe"
    ) as mock_get_jobs_dataframe:
        mock_get_jobs_dataframe.return_value = feature_job_logs
        job_status_result = saved_feature.get_feature_jobs_status(
            job_history_window=24, job_duration_tolerance=1700
        )

    fixture_path = "tests/fixtures/feature_job_status/expected_session_logs.parquet"
    if update_fixtures:
        job_status_result.job_session_logs.to_parquet(fixture_path)
        raise ValueError("Fixtures updated. Please run test again without --update-fixtures flag")
    else:
        expected_session_logs = pd.read_parquet(fixture_path)

        # NOTE: Parquet serialization and deserialization converts NaT to None
        # converting the problematic values to None before comparing
        session_logs = job_status_result.job_session_logs.copy()
        session_logs.loc[session_logs["ERROR"].isna(), "ERROR"] = None
        assert_frame_equal(session_logs, expected_session_logs)


@freeze_time("2023-01-20 03:20:00")
def test_get_feature_jobs_status_incomplete_logs(saved_feature, feature_job_logs):
    """
    Test get_feature_jobs_status incomplete logs found
    """
    with patch(
        "featurebyte.service.tile_job_log.TileJobLogService.get_logs_dataframe"
    ) as mock_get_jobs_dataframe:
        mock_get_jobs_dataframe.return_value = feature_job_logs[:1]
        job_status_result = saved_feature.get_feature_jobs_status(job_history_window=24)
    assert job_status_result.job_session_logs.shape == (1, 12)
    expected_feature_job_summary = pd.DataFrame({
        "aggregation_hash": {0: "e8c51d7d"},
        "frequency(min)": {0: 30},
        "completed_jobs": {0: 0},
        "max_duration(s)": {0: np.nan},
        "95 percentile": {0: np.nan},
        "frac_late": {0: np.nan},
        "exceed_period": {0: 0},
        "failed_jobs": {0: 0},
        "incomplete_jobs": {0: 48},
        "time_since_last": {0: "NaT"},
    })
    assert_frame_equal(
        job_status_result.feature_job_summary, expected_feature_job_summary, check_dtype=False
    )


@freeze_time("2023-01-20 03:20:00")
def test_get_feature_jobs_status_empty_logs(saved_feature, feature_job_logs):
    """
    Test get_feature_jobs_status incomplete logs found
    """
    with patch(
        "featurebyte.service.tile_job_log.TileJobLogService.get_logs_dataframe"
    ) as mock_get_jobs_dataframe:
        mock_get_jobs_dataframe.return_value = feature_job_logs[:0]
        job_status_result = saved_feature.get_feature_jobs_status(job_history_window=24)
    assert job_status_result.job_session_logs.shape == (0, 12)
    expected_feature_job_summary = pd.DataFrame({
        "aggregation_hash": {0: "e8c51d7d"},
        "frequency(min)": {0: 30},
        "completed_jobs": {0: 0},
        "max_duration(s)": {0: np.nan},
        "95 percentile": {0: np.nan},
        "frac_late": {0: np.nan},
        "exceed_period": {0: 0},
        "failed_jobs": {0: 0},
        "incomplete_jobs": {0: 48},
        "time_since_last": {0: "NaT"},
    })
    assert_frame_equal(
        job_status_result.feature_job_summary, expected_feature_job_summary, check_dtype=False
    )


def test_get_feature_jobs_status_feature_without_tile(
    saved_scd_table, cust_id_entity, feature_job_logs
):
    """
    Test get_feature_jobs_status for feature without tile
    """
    saved_scd_table["col_text"].as_entity(cust_id_entity.name)
    scd_view = saved_scd_table.get_view()
    feature = scd_view["effective_timestamp"].as_feature("Latest Record Change Date")
    feature.save()

    with patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.return_value = feature_job_logs[:0]
        job_status_result = feature.get_feature_jobs_status()

    assert job_status_result.feature_tile_table.shape == (0, 2)
    assert job_status_result.feature_job_summary.shape == (0, 10)
    assert job_status_result.job_session_logs.shape == (0, 12)


def test_feature_synchronization(saved_feature):
    """Test feature synchronization"""
    # construct a cloned feature (feature with the same feature ID)
    cloned_feat = Feature.get_by_id(id=saved_feature.id)

    # update the original feature's version mode (stored at feature namespace record)
    target_mode = DefaultVersionMode.MANUAL
    assert saved_feature.default_version_mode != target_mode
    saved_feature.update_default_version_mode(target_mode)
    assert saved_feature.default_version_mode == target_mode

    # check the clone's version mode also get updated
    assert cloned_feat.default_version_mode == target_mode

    # update original feature's readiness (stored at feature record)
    target_readiness = FeatureReadiness.PUBLIC_DRAFT
    assert saved_feature.readiness != target_readiness
    saved_feature.update_readiness(target_readiness)
    assert saved_feature.readiness == target_readiness

    # check the clone's readiness value
    assert cloned_feat.readiness == target_readiness


def test_feature_deletion_failure(saved_feature):
    """Test feature deletion failure"""
    feature_list = FeatureList([saved_feature], name="test_feature_list")
    feature_list.save()
    with pytest.raises(RecordDeletionException) as exc_info:
        saved_feature.delete()

    version = feature_list.version
    expected_msg = (
        "Feature is still in use by feature list(s). Please remove the following feature list(s) first:\n"
        f"[{{'id': '{feature_list.id}',\n  'name': 'test_feature_list',\n  'version': '{version}'}}]"
    )
    assert expected_msg == exc_info.value.response.json()["detail"]


def test_feature_properties_from_cached_model__before_save(float_feature):
    """Test (unsaved) feature properties from cached model"""
    # check properties derived from feature model directly
    assert float_feature.saved is False
    assert float_feature.readiness == FeatureReadiness.DRAFT
    assert float_feature.online_enabled is False
    assert float_feature.deployed_feature_list_ids == []

    # check properties use feature namespace model info
    props = ["is_default", "default_version_mode", "default_readiness"]
    for prop in props:
        with pytest.raises(RecordRetrievalException):
            _ = getattr(float_feature, prop)


def test_feature_properties_from_cached_model__after_save(saved_feature):
    """Test (saved) feature properties from cached model"""
    # check properties derived from feature model directly
    assert saved_feature.saved is True
    assert saved_feature.readiness == FeatureReadiness.DRAFT
    assert saved_feature.online_enabled is False
    assert saved_feature.deployed_feature_list_ids == []

    # check properties use feature namespace model info
    assert saved_feature.is_default is True
    assert saved_feature.default_version_mode == DefaultVersionMode.AUTO
    assert saved_feature.default_readiness == FeatureReadiness.DRAFT


@pytest.fixture(name="feature_with_clean_column_names")
def feature_with_clean_column_names_fixture(saved_event_table, cust_id_entity):
    """Feature with clean column names"""
    col_names = ["col_int", "col_float", "cust_id"]
    for col_name in col_names:
        col = saved_event_table[col_name]
        col.update_critical_data_info(
            cleaning_operations=[MissingValueImputation(imputed_value=-1)]
        )

    saved_event_table.cust_id.as_entity(cust_id_entity.name)
    event_view = saved_event_table.get_view()
    feature = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_names=["sum_30m"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="10m",
            period="30m",
            offset="5m",
        ),
    )["sum_30m"]
    return feature, col_names


def test_feature_graph_prune_unused_cleaning_operations(feature_with_clean_column_names):
    """Test feature graph pruning unused cleaning operations"""
    feature, col_names = feature_with_clean_column_names

    # check feature graph before saving
    graph = QueryGraphModel(**feature._get_create_payload()["graph"])
    event_view_graph_node_metadata = graph.get_node_by_name("graph_1").parameters.metadata
    compare_pydantic_obj(
        event_view_graph_node_metadata.column_cleaning_operations,
        expected=[
            {
                "column_name": col_name,
                "cleaning_operations": [{"type": "missing", "imputed_value": -1}],
            }
            for col_name in col_names
        ],
    )

    # check feature graph after saving
    feature.save()
    graph = QueryGraphModel(**feature.model_dump()["graph"])

    # check event view graph node metadata
    event_view_graph_node = graph.get_node_by_name("graph_1")
    assert event_view_graph_node.parameters.type == "event_view"
    event_view_graph_node_metadata = event_view_graph_node.parameters.metadata
    compare_pydantic_obj(
        event_view_graph_node_metadata.column_cleaning_operations,
        expected=[
            {
                "column_name": col_name,
                "cleaning_operations": [{"type": "missing", "imputed_value": -1}],
            }
            for col_name in ["col_float", "cust_id"]
        ],
    )

    # check cleaning graph node (should contain only those columns that are used in the feature)
    cleaning_graph_node = event_view_graph_node.parameters.graph.get_node_by_name("graph_1")
    assert cleaning_graph_node.parameters.type == "cleaning"
    nested_graph = cleaning_graph_node.parameters.graph
    for assign_node in nested_graph.iterate_nodes(
        target_node=cleaning_graph_node.output_node,
        node_type="assign",
    ):
        assert assign_node.parameters.name in {"col_float", "cust_id"}


def test_feature_definition(feature_with_clean_column_names):
    """Test feature definition"""
    feature, _ = feature_with_clean_column_names

    # before save, the redundant cleaning operation should be included
    redundant_clean_op = 'column_name="col_int",'
    assert redundant_clean_op in feature.definition

    # after save, the redundant cleaning operation should be removed
    header = f"# Generated by SDK version: {get_version()}\n"
    expected = (
        header
        + textwrap.dedent(
            f"""
    from bson import ObjectId
    from featurebyte import ColumnCleaningOperation
    from featurebyte import EventTable
    from featurebyte import FeatureJobSetting
    from featurebyte import MissingValueImputation


    # event_table name: "sf_event_table"
    event_table = EventTable.get_by_id(ObjectId("6337f9651050ee7d5980660d"))
    event_view = event_table.get_view(
        view_mode="manual",
        drop_column_names=["created_at"],
        column_cleaning_operations=[
            ColumnCleaningOperation(
                column_name="col_float",
                cleaning_operations=[MissingValueImputation(imputed_value=-1.0)],
            ),
            ColumnCleaningOperation(
                column_name="cust_id",
                cleaning_operations=[MissingValueImputation(imputed_value=-1)],
            ),
        ],
    )
    grouped = event_view.groupby(by_keys=["cust_id"], category=None).aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_names=["sum_30m"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="600s", period="1800s", offset="300s"
        ),
        skip_fill_na=True,
        offset=None,
    )
    feat = grouped["sum_30m"]
    output = feat
    output.save(_id=ObjectId("{feature.id}"))
    """
        ).strip()
    )
    feature.save()
    assert feature.definition.strip() == expected
    assert redundant_clean_op not in expected


@pytest.mark.parametrize("main_data_from_event_table", [True, False])
def test_feature_create_new_version__multiple_event_table(
    saved_event_table,
    snowflake_database_table_scd_table,
    cust_id_entity,
    main_data_from_event_table,
    patch_initialize_entity_dtype,
):
    """Test create new version with multiple feature job settings"""
    another_event_table = snowflake_database_table_scd_table.create_event_table(
        name="another_event_table",
        event_id_column="col_int",
        event_timestamp_column="effective_timestamp",
        record_creation_timestamp_column="end_timestamp",
    )

    # label entity column
    saved_event_table.cust_id.as_entity(cust_id_entity.name)
    another_event_table.col_text.as_entity(cust_id_entity.name)

    event_view = saved_event_table.get_view()
    another_event_view = another_event_table.get_view()
    if main_data_from_event_table:
        event_view.join(
            another_event_view,
            on="col_int",
            how="left",
            rsuffix="_another",
        )
        event_view_used = event_view
        entity_col_name = "cust_id"
        main_data_name, non_main_data_name = saved_event_table.name, another_event_table.name
    else:
        another_event_view.join(
            event_view,
            on="col_int",
            how="left",
            rsuffix="_another",
        )
        event_view_used = another_event_view
        entity_col_name = "col_text"
        main_data_name, non_main_data_name = another_event_table.name, saved_event_table.name

    feature = event_view_used.groupby(entity_col_name).aggregate_over(
        value_column=None,
        method="count",
        windows=["30m"],
        feature_names=["count_30m"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="10m",
            period="30m",
            offset="5m",
        ),
    )["count_30m"]
    feature.save()

    # create new version with different feature job setting on another event table dataset
    feature_job_setting = FeatureJobSetting(blind_spot="20m", period="30m", offset="5m")
    with pytest.raises(RecordCreationException) as exc:
        feature.create_new_version(
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name=non_main_data_name,
                    feature_job_setting=feature_job_setting,
                ),
            ],
        )

    # error expected as the group by is on event table dataset, but not on another event table dataset
    expected_error = (
        "Feature job setting does not result a new feature version. "
        "This is because the new feature version is the same as the source feature."
    )
    assert expected_error in str(exc.value)

    # create new version on event table & check group by params
    new_version = feature.create_new_version(
        table_feature_job_settings=[
            TableFeatureJobSetting(
                table_name=main_data_name, feature_job_setting=feature_job_setting
            ),
        ]
    )
    pruned_graph, _ = new_version.extract_pruned_graph_and_node()
    group_by_params = pruned_graph.get_node_by_name("groupby_1").parameters
    assert group_by_params.feature_job_setting == feature_job_setting


def test_primary_entity__unsaved_feature(float_feature, cust_id_entity):
    """
    Test primary_entity property for an unsaved feature
    """
    assert float_feature.primary_entity == [Entity.get_by_id(cust_id_entity.id)]


def test_primary_entity__saved_feature(saved_feature, cust_id_entity):
    """
    Test primary_entity property for a saved feature
    """
    assert saved_feature.primary_entity == [Entity.get_by_id(cust_id_entity.id)]


def test_primary_entity__feature_group(feature_group, cust_id_entity):
    """
    Test primary_entity property for a feature group
    """
    assert feature_group.primary_entity == [Entity.get_by_id(cust_id_entity.id)]


def test_list_unsaved_features(
    snowflake_database_table,
    feature_group_feature_job_setting,
    float_feature,
    count_per_category_feature_group,
    sum_per_category_feature,
    count_per_category_feature,
    snowflake_feature_store,
):
    """
    Test list_unsaved_features method
    """
    try:
        # create feature in new catalog
        Catalog.create(name="test_catalog", feature_store_name=snowflake_feature_store.name)
        event_table = snowflake_database_table.create_event_table(
            name="sf_event_table",
            event_id_column="col_int",
            event_timestamp_column="event_timestamp",
            record_creation_timestamp_column="created_at",
        )
        Entity.create(name="cust_id_entity", serving_names=["cust_id"])
        event_table.cust_id.as_entity("cust_id_entity")
        bool_feature = (
            event_table.get_view()
            .groupby("cust_id")
            .aggregate_over(
                value_column="col_float",
                method="sum",
                windows=["1d"],
                feature_job_setting=feature_group_feature_job_setting,
                feature_names=["sum_1d"],
            )["sum_1d"]
            > 100.0
        )
        activate_and_get_catalog("catalog")

        # create unsaved features
        unsaved_feature = float_feature  # noqa: F841
        feature_group = FeatureGroup(  # noqa: F841
            [float_feature, sum_per_category_feature],
        )
        feature_list_1 = FeatureList(
            [count_per_category_feature_group, sum_per_category_feature], name="FL1"
        )
        feature_list_2 = FeatureList(  # noqa: F841
            [float_feature, sum_per_category_feature, count_per_category_feature], name="FL2"
        )

        # test list unsaved features
        unsaved_feature_df = list_unsaved_features().sort_values(["name", "variable_name"])
        expected_df = pd.DataFrame({
            "variable_name": [
                "count_per_category_feature",
                'count_per_category_feature_group["counts_1d"]',
                'feature_list_1["counts_1d"]',
                'feature_list_2["counts_1d"]',
                'count_per_category_feature_group["counts_2h"]',
                'feature_list_1["counts_2h"]',
                'count_per_category_feature_group["counts_30m"]',
                'feature_list_1["counts_30m"]',
                'feature_group["sum_1d"]',
                'feature_list_2["sum_1d"]',
                "float_feature",
                "unsaved_feature",
                'feature_group["sum_30m"]',
                'feature_list_1["sum_30m"]',
                'feature_list_2["sum_30m"]',
                "sum_per_category_feature",
                "bool_feature",
            ],
            "name": [
                "counts_1d",
                "counts_1d",
                "counts_1d",
                "counts_1d",
                "counts_2h",
                "counts_2h",
                "counts_30m",
                "counts_30m",
                "sum_1d",
                "sum_1d",
                "sum_1d",
                "sum_1d",
                "sum_30m",
                "sum_30m",
                "sum_30m",
                "sum_30m",
                None,
            ],
            "catalog": [
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "catalog",
                "test_catalog",
            ],
            "active_catalog": [
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                False,
            ],
        })
        assert unsaved_feature_df.columns.tolist() == [
            "object_id",
            "variable_name",
            "name",
            "catalog",
            "active_catalog",
        ]
        pd.testing.assert_frame_equal(
            unsaved_feature_df[["variable_name", "name", "catalog", "active_catalog"]].reset_index(
                drop=True
            ),
            expected_df,
        )

        # save some features
        feature_list_1.save()
        float_feature.save()
        unsaved_feature_df = list_unsaved_features().sort_values(["name", "variable_name"])
        expected_df = pd.DataFrame([
            {
                "variable_name": "bool_feature",
                "name": None,
                "catalog": "test_catalog",
                "active_catalog": False,
            }
        ])
        pd.testing.assert_frame_equal(
            unsaved_feature_df[["variable_name", "name", "catalog", "active_catalog"]].reset_index(
                drop=True
            ),
            expected_df,
        )

        # save remaining
        activate_and_get_catalog("test_catalog")
        bool_feature.name = "bool"
        bool_feature.save()
        unsaved_feature_df = list_unsaved_features().sort_values(["name", "variable_name"])
        pd.testing.assert_frame_equal(
            unsaved_feature_df,
            pd.DataFrame(
                columns=["object_id", "variable_name", "name", "catalog", "active_catalog"]
            ),
        )
    finally:
        activate_and_get_catalog("catalog")


def test_unsaved_feature_repr(
    float_feature,
):
    expected_value = f"Feature[FLOAT](name=sum_1d, node_name={float_feature.node_name})"
    assert repr(float_feature) == expected_value

    # html representation for unsaved object should be the same as repr
    assert float_feature._repr_html_() == expected_value


def test_feature_dtype(
    float_feature,
    bool_feature,
    non_time_based_feature,
    sum_per_category_feature,
):
    """Test feature dtype before and after save"""
    bool_feature.name = "bool_feat"

    # test feature dtype before save
    assert float_feature.dtype == DBVarType.FLOAT
    assert bool_feature.dtype == DBVarType.BOOL
    assert non_time_based_feature.dtype == DBVarType.FLOAT
    assert sum_per_category_feature.dtype == DBVarType.OBJECT

    float_feature.save()
    bool_feature.save()
    non_time_based_feature.save()
    sum_per_category_feature.save()

    # test feature dtype after save
    assert float_feature.dtype == DBVarType.FLOAT
    assert bool_feature.dtype == DBVarType.BOOL
    assert non_time_based_feature.dtype == DBVarType.FLOAT
    assert sum_per_category_feature.dtype == DBVarType.OBJECT


@pytest.mark.flaky(reruns=3)
@pytest.mark.asyncio
async def test_feature_loading_time(mock_api_object_cache, saved_feature, persistent):
    """Test feature loading time (to detect changes in performance)"""
    # mock api object cache to disable caching
    _ = mock_api_object_cache
    sample_size = 10
    feature_loading_limit = 300

    # get elapsed time with persistent
    start = time.time()
    for _ in range(sample_size):
        _ = await persistent.find_one(
            collection_name="feature", query_filter={"_id": saved_feature.id}
        )
    persistent_elapsed_time = time.time() - start

    # get end-to-end elapsed time
    start = time.time()
    for _ in range(sample_size):
        _ = Feature.get_by_id(saved_feature.id)
    elapsed_time = time.time() - start

    # compute persistent elapsed time to elapsed time ratio
    ratio = elapsed_time / persistent_elapsed_time
    assert ratio < feature_loading_limit, ratio


class TestFeatureTestSuite(FeatureOrTargetBaseTestSuite):
    """Test suite for feature"""

    item_type = TestItemType.FEATURE
    expected_item_definition = (
        f"""
    # Generated by SDK version: {featurebyte.get_version()}"""
        + """
    from bson import ObjectId
    from featurebyte import EventTable
    from featurebyte import FeatureJobSetting

    event_table = EventTable.get_by_id(ObjectId("{table_id}"))
    event_view = event_table.get_view(
        view_mode="manual",
        drop_column_names=["created_at"],
        column_cleaning_operations=[],
    )
    grouped = event_view.groupby(by_keys=["cust_id"], category=None).aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["1d"],
        feature_names=["sum_1d"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="600s", period="1800s", offset="300s"
        ),
        skip_fill_na=True,
        offset=None,
    )
    feat = grouped["sum_1d"]
    output = feat
    """
    )
    expected_saved_item_definition = (
        f"""
    # Generated by SDK version: {featurebyte.get_version()}"""
        + """
    from bson import ObjectId
    from featurebyte import EventTable
    from featurebyte import FeatureJobSetting


    # event_table name: "sf_event_table"
    event_table = EventTable.get_by_id(ObjectId("{table_id}"))
    event_view = event_table.get_view(
        view_mode="manual",
        drop_column_names=["created_at"],
        column_cleaning_operations=[],
    )
    grouped = event_view.groupby(by_keys=["cust_id"], category=None).aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["1d"],
        feature_names=["sum_1d"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="600s", period="1800s", offset="300s"
        ),
        skip_fill_na=True,
        offset=None,
    )
    feat = grouped["sum_1d"]
    output = feat
    output.save(_id=ObjectId("{item_id}"))
    """
    )


def test_feature_relationships_info(saved_feature, cust_id_entity, transaction_entity):
    """Test feature relationships info"""
    _ = transaction_entity
    relationships_info = saved_feature.cached_model.relationships_info
    assert saved_feature.cached_model.entity_ids == [cust_id_entity.id]
    # transaction entity is the child entity of the feature primary entity (cust_id_entity)
    # hence, it is not included in the relationships info as relationships info only
    # contains the ancestor relationships of the primary entity
    assert len(relationships_info) == 0


def test_complex_feature_with_duplicated_feature_job_setting(
    snowflake_event_table_with_entity, feature_group_feature_job_setting
):
    """Test complex feature with duplicated feature job setting"""
    snowflake_event_table_with_entity.update_default_feature_job_setting(
        feature_job_setting=feature_group_feature_job_setting,
    )
    event_view = snowflake_event_table_with_entity.get_view()
    grouped = event_view.groupby("cust_id")
    feat_sum_col_float = grouped.aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_30m"],
    )["sum_30m"]
    feat_sum_col_int = grouped.aggregate_over(
        value_column="col_int",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_30m"],
    )["sum_30m"]
    complex_feat = feat_sum_col_int + feat_sum_col_float
    complex_feat.name = "complex_feat"
    complex_feat.save()
    table_id_feature_job_settings = complex_feat.cached_model.table_id_feature_job_settings
    assert len(table_id_feature_job_settings) == 1
    assert table_id_feature_job_settings[0].model_dump() == {
        "table_id": snowflake_event_table_with_entity.id,
        "feature_job_setting": {
            "blind_spot": "600s",
            "period": "1800s",
            "offset": "300s",
            "execution_buffer": "0s",
        },
    }


def test_feature_or_view_column_name_contains_quote(
    cust_id_entity,
    snowflake_event_table_with_entity,
    feature_group_feature_job_setting,
):
    """Test feature or view column name contains quote"""
    _ = cust_id_entity
    snowflake_event_table_with_entity.update_default_feature_job_setting(
        feature_job_setting=feature_group_feature_job_setting,
    )
    event_view = snowflake_event_table_with_entity.get_view()
    mask_col_name = '"mask" col\'s name'
    event_view[mask_col_name] = event_view["col_float"] < 0
    cols = ["col_float", "cust_id", mask_col_name]
    sub_event_view = event_view[cols]
    mask = sub_event_view[mask_col_name]
    sub_event_view["col_float"][mask] = 0
    feature_name = '"col_float"\'s aggregation'
    feature = sub_event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=[feature_name],
    )[feature_name]

    # check feature can be saved without error
    feature.save()


def test_create_new_version_for_non_tile_window_aggregate(
    snowflake_event_table_with_entity,
    snowflake_event_view_with_entity_and_feature_job,
    count_distinct_window_aggregate_feature,
):
    """Test create new version for non-tile window aggregate feature"""
    feature_fjs = FeatureJobSetting(blind_spot="600s", period="1800s", offset="300s")
    count_distinct_window_aggregate_feature.save()
    assert count_distinct_window_aggregate_feature.cached_model.table_id_feature_job_settings == [
        TableIdFeatureJobSetting(
            table_id=snowflake_event_table_with_entity.id,
            feature_job_setting=feature_fjs,
        )
    ]
    assert (
        snowflake_event_view_with_entity_and_feature_job.default_feature_job_setting != feature_fjs
    )

    # attempt to upgrade readiness to production ready should fail
    with pytest.raises(RecordUpdateException) as exc:
        count_distinct_window_aggregate_feature.update_readiness(FeatureReadiness.PRODUCTION_READY)
    expected_msg = (
        "Discrepancies found between the promoted feature version you are trying to promote to PRODUCTION_READY, "
        "and the input table."
    )
    assert expected_msg in str(exc.value)

    # check create new version
    new_fjs = FeatureJobSetting(blind_spot="45m", period="30m", offset="15m")
    new_version = count_distinct_window_aggregate_feature.create_new_version(
        table_feature_job_settings=[
            TableFeatureJobSetting(
                table_name=snowflake_event_table_with_entity.name,
                feature_job_setting=new_fjs,
            )
        ],
    )

    # check non-tile window aggregate feature's new version
    assert new_version.cached_model.table_id_feature_job_settings == [
        TableIdFeatureJobSetting(
            table_id=snowflake_event_table_with_entity.id,
            feature_job_setting=new_fjs,
        )
    ]
    assert set(new_version.cached_model.graph.nodes_map.keys()) == {
        "input_1",
        "graph_1",
        "non_tile_window_aggregate_1",
        "project_1",
    }


def test_update_feature_type(
    saved_feature, snowflake_dimension_table, cust_id_entity, mock_api_object_cache
):
    """Test feature type"""
    _ = mock_api_object_cache

    assert saved_feature.feature_type == "numeric"
    saved_feature.feature_namespace.update_feature_type("categorical")

    # update feature type
    snowflake_dimension_table["col_int"].as_entity(cust_id_entity.name)
    view = snowflake_dimension_table.get_view()
    feature = view["col_int"].as_feature("IntFeature")
    feature.save()

    assert feature.feature_type == "categorical"
    feature.update_feature_type("numeric")
    assert feature.feature_type == "numeric"


def test_feature_generated_from_empty_event_join_column_names(
    snowflake_item_table, saved_event_table, cust_id_entity
):
    """Test feature generated from item view with empty event_join_column_names"""
    # construct an item view with empty event_join_column_names
    saved_event_table.cust_id.as_entity(None)
    snowflake_item_table.event_id_col.as_entity(cust_id_entity.name)
    item_view = snowflake_item_table.get_view(view_mode="manual", event_join_column_names=[])
    assert item_view.node.parameters.metadata.event_join_column_names == []

    # create feature from item view & save it should not raise error
    feat = item_view.groupby("event_id_col").aggregate(
        "item_amount",
        method="sum",
        feature_name="order_size",
    )
    feat.save()

    expected_item_view = textwrap.dedent(
        """
        item_view = item_table.get_view(
            event_suffix=None,
            view_mode="manual",
            drop_column_names=[],
            column_cleaning_operations=[],
            event_drop_column_names=[],
            event_column_cleaning_operations=[],
            event_join_column_names=[],
        )
        """
    ).strip()
    assert expected_item_view in feat.definition


def test_feature_count_dict_get_value_dtype(
    snowflake_item_table,
    saved_event_table,
    snowflake_dimension_table,
    cust_id_entity,
    transaction_entity,
    feature_group_feature_job_setting,
):
    """Test feature created from count_dict operations with get_value has FLOAT dtype"""
    # create entities
    item_entity = Entity(name="item", serving_names=["item_id_col"])
    item_entity.save()

    # product entity for dimension table (like GROCERYPRODUCTGUID)
    product_entity = Entity(name="product", serving_names=["item_type"])
    product_entity.save()

    # invoice entity (like GROCERYINVOICEGUID)
    invoice_entity = Entity(name="invoice", serving_names=["invoice_id"])
    invoice_entity.save()

    # label entity columns
    snowflake_item_table.item_id_col.as_entity(item_entity.name)
    snowflake_item_table.event_id_col.as_entity(invoice_entity.name)
    snowflake_item_table.item_type.as_entity(product_entity.name)
    snowflake_dimension_table.col_text.as_entity(product_entity.name)
    saved_event_table.col_int.as_entity(invoice_entity.name)
    saved_event_table.cust_id.as_entity(cust_id_entity.name)

    # get views
    item_view = snowflake_item_table.get_view()
    event_view = saved_event_table.get_view()
    dim_view = snowflake_dimension_table.get_view()

    # join item_view with dim_view on product type (like INVOICEITEMS join GROCERYPRODUCT)
    item_view = item_view.join(dim_view, on="item_type", rsuffix="_product")

    # create a groupby with category parameter (similar to ProductGroup in debug_dtype.py)
    # group by event_id_col (invoice), category is col_boolean from product dimension (with rsuffix)
    item_view_by_invoice = item_view.groupby("event_id_col", category="col_boolean_product")

    # Aggregate with sum to create a count_dict feature
    invoice_items_sum_by_category = item_view_by_invoice.aggregate(
        "item_amount",
        method="sum",
        feature_name="invoice_items_sum_by_product_category",
    )

    # add feature to event view (like adding to GROCERYINVOICE)
    event_view = event_view.add_feature(
        "invoice_items_sum_by_product_category",
        invoice_items_sum_by_category,
    )

    # create aggregate_over with latest method
    event_view_by_customer = event_view.groupby(["cust_id"])
    customer_latest_invoice_items_sum_by_category = event_view_by_customer.aggregate_over(
        "invoice_items_sum_by_product_category",
        method="latest",
        feature_names=["customer_latest_invoice_items_sum_by_category_13w"],
        windows=["13w"],
        feature_job_setting=feature_group_feature_job_setting,
    )["customer_latest_invoice_items_sum_by_category_13w"]

    # get value from count_dict using cd accessor
    feature_with_get_value = customer_latest_invoice_items_sum_by_category.cd.get_value("test_key")

    # set name and save
    feature_with_get_value.name = "customer_latest_invoice_items_sum_by_category_test_key"
    feature_with_get_value.save()

    # assert the dtype is FLOAT
    assert feature_with_get_value.dtype == DBVarType.FLOAT


def test_get_value_with_null_filling_extractor(
    snowflake_event_table_with_entity, arbitrary_default_feature_job_setting
):
    """Test get_value work with NullFillingValueExtractor"""
    event_view = snowflake_event_table_with_entity.get_view()
    event_view_by_customer = event_view.groupby(["cust_id"], category="col_text")
    feat = event_view_by_customer.aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=arbitrary_default_feature_job_setting,
        feature_names=["sum_by_cust_id"],
    )["sum_by_cust_id"]
    feat1 = feat.cd.get_value(key="test_key")
    feat1.name = "sum_by_cust_id_test_key"
    feat1.save()
    feat = event_view_by_customer.aggregate_over(
        value_column=None,
        method="count",
        windows=["30m"],
        feature_job_setting=arbitrary_default_feature_job_setting,
        feature_names=["count_by_cust_id"],
    )["count_by_cust_id"]
    feat2 = feat.cd.get_value(key="test_key")
    feat2.name = "count_by_cust_id_test_key"
    feat2.save()

    fill_value1 = (
        NullFillingValueExtractor(graph=feat1.cached_model.graph)
        .extract(node=feat1.cached_model.node, source_type=SourceType.SNOWFLAKE)
        .fill_value
    )
    assert fill_value1 is None

    fill_value2 = (
        NullFillingValueExtractor(graph=feat2.cached_model.graph)
        .extract(node=feat2.cached_model.node, source_type=SourceType.SNOWFLAKE)
        .fill_value
    )
    assert fill_value2 is None
