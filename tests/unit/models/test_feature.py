"""
Tests for Feature related models
"""
import json
import os
from datetime import datetime

import pytest
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_namespace import FeatureNamespaceModel, FeatureReadiness
from featurebyte.query_graph.node.metadata.operation import AggregationColumn, SourceDataColumn


@pytest.fixture(name="feature_namespace_dict")
def feature_name_space_dict_fixture():
    """Fixture for a FixtureNameSpace dict"""
    feature_ids = [ObjectId("631b00277280fc9aa9522794"), ObjectId("631b00277280fc9aa9522793")]
    entity_ids = [ObjectId("631b00277280fc9aa9522790"), ObjectId("631b00277280fc9aa9522789")]
    table_ids = [ObjectId("631b00277280fc9aa9522792"), ObjectId("631b00277280fc9aa9522791")]
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
        "table_ids": table_ids,
        "user_id": None,
        "catalog_id": DEFAULT_CATALOG_ID,
        "block_modification_by": [],
    }


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_feature(test_dir):
    """Fixture for a Feature dict"""
    feature_fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(feature_fixture_path) as file_handle:
        return json.load(file_handle)


def test_feature_model(feature_model_dict, api_object_to_id):
    """Test feature model serialize & deserialize"""
    # pylint: disable=duplicate-code
    feature = FeatureModel(**feature_model_dict)
    feature_json = feature.json(by_alias=True)
    loaded_feature = FeatureModel.parse_raw(feature_json)
    feature_dict = feature.dict(by_alias=True)
    assert loaded_feature.id == feature.id
    assert loaded_feature.dict(by_alias=True) == feature_dict
    assert feature_dict == {
        "created_at": None,
        "deployed_feature_list_ids": [],
        "dtype": "FLOAT",
        "entity_ids": [ObjectId(api_object_to_id["entity"])],
        "table_ids": [ObjectId(api_object_to_id["event_table"])],
        "feature_list_ids": [],
        "feature_namespace_id": feature_dict["feature_namespace_id"],
        "graph": {
            "edges": feature_dict["graph"]["edges"],
            "nodes": feature_dict["graph"]["nodes"],
        },
        "_id": ObjectId(feature_model_dict["_id"]),
        "name": "sum_30m",
        "node_name": "project_1",
        "online_enabled": False,
        "readiness": "DRAFT",
        "tabular_source": {
            "feature_store_id": ObjectId(api_object_to_id["feature_store"]),
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        },
        "updated_at": None,
        "user_id": None,
        "version": None,
        "definition": None,
        "catalog_id": DEFAULT_CATALOG_ID,
        "primary_table_ids": [ObjectId(api_object_to_id["event_table"])],
        "user_defined_function_ids": [],
        "block_modification_by": [],
        "aggregation_ids": ["sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"],
        "aggregation_result_names": [
            "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
        ],
    }


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
        > FeatureReadiness.PUBLIC_DRAFT
        > FeatureReadiness.DRAFT
        > FeatureReadiness.DEPRECATED
    )
    assert FeatureReadiness.min() == FeatureReadiness.DEPRECATED
    assert FeatureReadiness.max() == FeatureReadiness.PRODUCTION_READY


def test_extract_operation_structure(feature_model_dict):
    """Test extract_operation_structure method"""
    feature = FeatureModel(**feature_model_dict)
    op_struct = feature.extract_operation_structure()
    common_source_col_params = {
        "table_id": feature.table_ids[0],
        "table_type": "event_table",
        "node_names": {"input_1", "graph_1"},
        "node_name": "graph_1",
        "filter": False,
    }
    expected_columns = [
        SourceDataColumn(name="col_float", dtype="FLOAT", **common_source_col_params)
    ]
    assert op_struct.source_columns == expected_columns
    assert op_struct.derived_columns == []
    assert op_struct.aggregations == [
        AggregationColumn(
            name="sum_30m",
            method="sum",
            keys=["cust_id"],
            window="30m",
            category=None,
            type="aggregation",
            column=SourceDataColumn(name="col_float", dtype="FLOAT", **common_source_col_params),
            filter=False,
            aggregation_type="groupby",
            node_names={"input_1", "graph_1", "groupby_1", "project_1"},
            node_name="groupby_1",
            dtype="FLOAT",
        )
    ]
