"""
Tests for Feature related models
"""

import json
import os
from datetime import datetime

import pytest
from bson import ObjectId

from featurebyte.common.model_util import get_version
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_namespace import FeatureNamespaceModel, FeatureReadiness
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
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
        "description": None,
        "is_deleted": False,
    }


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_feature(test_dir):
    """Fixture for a Feature dict"""
    feature_fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(feature_fixture_path) as file_handle:
        return json.load(file_handle)


def test_feature_model(feature_model_dict, api_object_to_id):
    """Test feature model serialize & deserialize"""

    feature = FeatureModel(**feature_model_dict)
    feature_json = feature.model_dump_json(by_alias=True)
    loaded_feature = FeatureModel.model_validate_json(feature_json)
    feature_dict = feature.model_dump(by_alias=True)
    assert loaded_feature.id == feature.id
    assert loaded_feature.model_dump(by_alias=True) == feature_dict
    assert feature_dict == {
        "created_at": None,
        "deployed_feature_list_ids": [],
        "dtype": "FLOAT",
        "entity_ids": [ObjectId(api_object_to_id["entity"])],
        "entity_join_steps": None,
        "entity_dtypes": ["INT"],
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
        "offline_store_info": None,
        "online_enabled": False,
        "readiness": "DRAFT",
        "relationships_info": None,
        "table_id_column_names": [
            {
                "table_id": ObjectId(api_object_to_id["event_table"]),
                "column_names": ["col_float", "cust_id", "event_timestamp"],
            }
        ],
        "table_id_cleaning_operations": [
            {
                "table_id": ObjectId(api_object_to_id["event_table"]),
                "column_cleaning_operations": [
                    {"column_name": column_name, "cleaning_operations": []}
                    for column_name in ["col_float", "cust_id", "event_timestamp"]
                ],
            }
        ],
        "table_id_feature_job_settings": [
            {
                "table_id": ObjectId(api_object_to_id["event_table"]),
                "feature_job_setting": {
                    "blind_spot": "600s",
                    "period": "1800s",
                    "offset": "300s",
                    "execution_buffer": "0s",
                },
            }
        ],
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
        "version": {"name": get_version(), "suffix": None},
        "definition": None,
        "catalog_id": DEFAULT_CATALOG_ID,
        "primary_entity_ids": [],
        "primary_table_ids": [ObjectId(api_object_to_id["event_table"])],
        "user_defined_function_ids": [],
        "block_modification_by": [],
        "aggregation_ids": ["sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"],
        "aggregation_result_names": [
            "_fb_internal_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
            "_fb_internal_window_w7200_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
            "_fb_internal_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
        ],
        "agg_result_name_include_serving_names": False,
        "description": None,
        "definition_hash": None,
        "online_store_table_names": ["ONLINE_STORE_377553E5920DD2DB8B17F21DDD52F8B1194A780C"],
        "last_updated_by_scheduled_task_at": None,
        "is_deleted": False,
    }


def test_feature_name_validation(feature_model_dict):
    """Test feature name validation"""
    feature_model_dict["name"] = f"__{feature_model_dict['name']}"
    expected_error = "FeatureModel name cannot start with '__' as it is reserved for internal use."
    with pytest.raises(ValueError, match=expected_error):
        FeatureModel(**feature_model_dict)


def test_feature_name_space(feature_namespace_dict):
    """Test feature namespace model"""
    feature_name_space = FeatureNamespaceModel.model_validate(feature_namespace_dict)
    serialized_feature_name_space = feature_name_space.model_dump(exclude={"id": True})
    feature_name_space_dict_sorted_ids = {
        key: sorted(value) if key.endswith("_ids") else value
        for key, value in feature_namespace_dict.items()
    }
    assert serialized_feature_name_space == feature_name_space_dict_sorted_ids
    loaded_feature_name_space = FeatureNamespaceModel.model_validate_json(
        feature_name_space.model_dump_json(by_alias=True)
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
        SourceDataColumn(
            name="col_float", dtype_info=DBVarTypeInfo(dtype="FLOAT"), **common_source_col_params
        )
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
            offset=None,
            type="aggregation",
            column=SourceDataColumn(
                name="col_float",
                dtype_info=DBVarTypeInfo(dtype="FLOAT"),
                **common_source_col_params,
            ),
            filter=False,
            aggregation_type="groupby",
            node_names={"input_1", "graph_1", "groupby_1", "project_1"},
            node_name="groupby_1",
            dtype_info=DBVarTypeInfo(dtype="FLOAT"),
        )
    ]


@pytest.mark.asyncio
async def test_ingest_graph_and_node(feature_model_dict, app_container):
    """Test ingest_graph_and_node method"""
    feature_model_dict["version"] = {"name": "V231231", "suffix": None}
    feature_model_dict["catalog_id"] = app_container.catalog_id
    feature = FeatureModel(**feature_model_dict)
    service = app_container.offline_store_info_initialization_service
    offline_store_info = await service.initialize_offline_store_info(
        feature=feature,
        table_name_prefix="cat1",
        entity_id_to_serving_name={entity_id: str(entity_id) for entity_id in feature.entity_ids},
    )
    ingest_query_graph = offline_store_info.extract_offline_store_ingest_query_graphs()[0]

    # case 1: no decomposed graph
    assert ingest_query_graph.ref_node_name is None
    _, ingest_node = ingest_query_graph.ingest_graph_and_node()
    assert ingest_node.type == "alias"
    assert ingest_node.parameters.name == feature.versioned_name

    # case 2: set ref_node_name to make it likes a decomposed graph (output is non-alias node)
    assert feature.node.type != "alias"
    ingest_query_graph.ref_node_name = "graph_1"
    _, ingest_node = ingest_query_graph.ingest_graph_and_node()
    assert ingest_node.name == "alias_1"  # check there is only one alias node
    assert ingest_node.type == "alias"
    assert ingest_node.parameters.name == feature.versioned_name

    # case 3: set ref_node_name to make it likes a decomposed graph (output is alias node)
    input_node = ingest_query_graph.graph.get_node_by_name(ingest_query_graph.node_name)
    inserted_alias_node = ingest_query_graph.graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "some_column_name"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    ingest_query_graph.node_name = inserted_alias_node.name
    assert ingest_query_graph.node_name == "alias_1"
    _, ingest_node = ingest_query_graph.ingest_graph_and_node()
    assert ingest_node.name == "alias_1"  # check there is only one alias node
    assert ingest_node.type == "alias"
    # check the alias node name is the feature name
    assert ingest_node.parameters.name == feature.versioned_name
