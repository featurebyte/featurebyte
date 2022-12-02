"""
Tests for Feature related models
"""
import json
import os
from datetime import datetime

import pytest
from bson.objectid import ObjectId

from featurebyte.models.feature import FeatureModel, FeatureNamespaceModel, FeatureReadiness
from featurebyte.query_graph.node.metadata.operation import AggregationColumn, SourceDataColumn


@pytest.fixture(name="feature_namespace_dict")
def feature_name_space_dict_fixture():
    """Fixture for a FixtureNameSpace dict"""
    feature_ids = [ObjectId("631b00277280fc9aa9522794"), ObjectId("631b00277280fc9aa9522793")]
    entity_ids = [ObjectId("631b00277280fc9aa9522790"), ObjectId("631b00277280fc9aa9522789")]
    tabular_data_ids = [ObjectId("631b00277280fc9aa9522792"), ObjectId("631b00277280fc9aa9522791")]
    return {
        "name": "some_feature_name",
        "dtype": "FLOAT",
        "feature_ids": feature_ids,
        "online_enabled_feature_ids": [],
        "readiness": "DRAFT",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "default_feature_id": feature_ids[0],
        "default_version_mode": "MANUAL",
        "entity_ids": entity_ids,
        "tabular_data_ids": tabular_data_ids,
        "user_id": None,
    }


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_feature(test_dir):
    """Fixture for a Feature dict"""
    feature_fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(feature_fixture_path) as file_handle:
        return json.load(file_handle)


def test_feature_model(feature_model_dict, test_dir):
    """Test feature model serialize & deserialize"""
    # pylint: disable=duplicate-code
    feature = FeatureModel(**feature_model_dict)
    feature_json = feature.json(by_alias=True)
    loaded_feature = FeatureModel.parse_raw(feature_json)
    feature_dict = feature.dict()
    assert loaded_feature.id == feature.id
    assert loaded_feature.dict() == feature_dict
    assert feature_dict == {
        "created_at": None,
        "deployed_feature_list_ids": [],
        "dtype": "FLOAT",
        "entity_ids": [ObjectId("6332fdb21e8f0eeccc414513")],
        "tabular_data_ids": [ObjectId("6332fdb21e8f0eeccc414512")],
        "feature_list_ids": [],
        "feature_namespace_id": ObjectId("6332fdb31e8f0eeccc414516"),
        "graph": {
            "edges": [
                {"source": "input_1", "target": "groupby_1"},
                {"source": "groupby_1", "target": "project_1"},
            ],
            "nodes": feature_dict["graph"]["nodes"],
        },
        "id": ObjectId("6332fdb31e8f0eeccc414515"),
        "name": "sum_30m",
        "node_name": "project_1",
        "online_enabled": False,
        "readiness": "DRAFT",
        "row_index_lineage": ("groupby_1",),
        "tabular_source": {
            "feature_store_id": ObjectId("6332fdb21e8f0eeccc414510"),
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        },
        "updated_at": None,
        "user_id": None,
        "version": None,
    }

    # DEV-556: check older record can be loaded
    with open(
        os.path.join(test_dir, "fixtures/backward_compatibility/feature_sum_2h.json")
    ) as file_handle:
        old_feat_dict = json.load(file_handle)
        old_feat_dict["online_enabled"] = None
        loaded_old_feature = FeatureModel.parse_obj(old_feat_dict)
        assert loaded_old_feature.online_enabled is False
        assert loaded_old_feature.node_name == "project_1"


def test_feature_name_space(feature_namespace_dict):
    """Test feature namespace model"""
    feature_name_space = FeatureNamespaceModel.parse_obj(feature_namespace_dict)
    serialized_feature_name_space = feature_name_space.dict(exclude={"id": True})
    feature_name_space_dict_sorted_ids = {
        key: sorted(value) if key.endswith("_ids") else value
        for key, value in feature_namespace_dict.items()
    }
    assert serialized_feature_name_space == feature_name_space_dict_sorted_ids
    loaded_feature_name_space = FeatureNamespaceModel.parse_raw(
        feature_name_space.json(by_alias=True)
    )
    assert loaded_feature_name_space == feature_name_space


def test_feature_readiness_ordering():
    """Test to cover feature readiness ordering"""
    assert (
        FeatureReadiness.PRODUCTION_READY
        > FeatureReadiness.DRAFT
        > FeatureReadiness.QUARANTINE
        > FeatureReadiness.DEPRECATED
    )
    assert FeatureReadiness.min() == FeatureReadiness.DEPRECATED
    assert FeatureReadiness.max() == FeatureReadiness.PRODUCTION_READY


def test_extract_operation_structure(feature_model_dict):
    """Test extract_operation_structure method"""
    feature = FeatureModel(**feature_model_dict)
    op_struct = feature.extract_operation_structure()
    common_source_col_params = {
        "tabular_data_id": ObjectId(feature_model_dict["tabular_data_ids"][0]),
        "tabular_data_type": "event_data",
        "node_names": {"input_1"},
    }
    expected_columns = [SourceDataColumn(name="col_float", **common_source_col_params)]
    assert op_struct.source_columns == expected_columns
    assert op_struct.derived_columns == []
    assert op_struct.aggregations == [
        AggregationColumn(
            name="sum_30m",
            method="sum",
            groupby=["cust_id"],
            window="30m",
            category=None,
            type="aggregation",
            column=SourceDataColumn(name="col_float", **common_source_col_params),
            filter=False,
            groupby_type="groupby",
            node_names={"input_1", "groupby_1", "project_1"},
        )
    ]
