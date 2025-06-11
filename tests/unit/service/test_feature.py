"""
Test FeatureService
"""

import json
import os
from datetime import datetime
from unittest.mock import patch

import pytest
from bson import ObjectId

from featurebyte import FeatureStore
from featurebyte.exception import (
    DocumentCreationError,
    DocumentInconsistencyError,
    DocumentModificationBlockedError,
    DocumentNotFoundError,
)
from featurebyte.models import FeatureModel
from featurebyte.models.base import ReferenceInfo
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.schema import SQLiteDetails
from featurebyte.schema.feature import FeatureServiceCreate
from featurebyte.service.feature import FeatureService


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_fixture(test_dir):
    """
    Feature model dict fixture
    """
    feature_model_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(feature_model_path) as file_handle:
        return json.load(file_handle)


@pytest.fixture(name="sqlite_feature_store")
def sqlite_feature_store_fixture(mock_get_persistent):
    """
    Sqlite source fixture
    """
    _ = mock_get_persistent
    return FeatureStore(
        name="sqlite_datasource",
        type="sqlite",
        details=SQLiteDetails(filename="some_filename"),
    )


@pytest.fixture(name="mock_execute_query", autouse=True)
def execute_query_fixture():
    """
    Execute query fixture
    """
    with patch("featurebyte.session.base.BaseSession.execute_query") as mock_execute_query:
        yield mock_execute_query


@pytest.mark.asyncio
async def test_update_document__inconsistency_error(feature_service, feature):
    """Test feature creation - document inconsistency error"""
    data_dict = feature.model_dump(by_alias=True)
    data_dict["_id"] = ObjectId()
    data_dict["name"] = "random_name"
    with pytest.raises(DocumentInconsistencyError) as exc:
        await feature_service.create_document(
            data=FeatureServiceCreate(**data_dict),
        )
    expected_msg = (
        'FeatureModel (name: "random_name") object(s) within the same namespace must have the same "name" value '
        '(namespace: "sum_30m", FeatureModel: "random_name").'
    )
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_get_document_by_name_and_version(
    feature_service, table_service, feature, app_container
):
    """Test feature service - get_document_by_name_and_version"""
    doc = await feature_service.get_document_by_name_and_version(
        name=feature.name, version=feature.version
    )
    assert doc == feature

    # check get document by name and version using different catalog ID
    with pytest.raises(DocumentNotFoundError) as exc:
        another_feat_service = FeatureService(
            user=feature_service.user,
            persistent=feature_service.persistent,
            catalog_id=ObjectId(),
            table_service=table_service,
            feature_namespace_service=app_container.feature_namespace_service,
            namespace_handler=app_container.namespace_handler,
            block_modification_handler=app_container.block_modification_handler,
            entity_serving_names_service=app_container.entity_serving_names_service,
            entity_relationship_extractor_service=app_container.entity_relationship_extractor_service,
            derive_primary_entity_helper=app_container.derive_primary_entity_helper,
            offline_store_info_initialization_service=app_container.offline_store_info_initialization_service,
            storage=app_container.storage,
            redis=app_container.redis,
        )
        await another_feat_service.get_document_by_name_and_version(
            name=feature.name, version=feature.version
        )

    expected_msg = (
        f'Feature (name: "{feature.name}", version: "{feature.version.to_str()}") not found. '
        f"Please save the Feature object first."
    )
    assert expected_msg in str(exc.value)

    # check get document by name and version using random name
    with pytest.raises(DocumentNotFoundError) as exc:
        await feature_service.get_document_by_name_and_version(
            name="random_name", version=feature.version
        )
    expected_msg = (
        f'Feature (name: "random_name", version: "{feature.version.to_str()}") not found. '
        f"Please save the Feature object first."
    )
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_feature_document_contains_raw_graph(feature_service, feature, api_object_to_id):
    """Test raw graph is stored"""
    expected_groupby_node = {
        "name": "groupby_1",
        "output_type": "frame",
        "parameters": {
            "agg_func": "sum",
            "aggregation_id": "sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
            "entity_ids": [ObjectId(api_object_to_id["entity"])],
            "keys": ["cust_id"],
            "names": ["sum_30m"],
            "parent": "col_float",
            "serving_names": ["cust_id"],
            "tile_id": "TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
            "tile_id_version": 2,
            "feature_job_setting": {
                "offset": "300s",
                "blind_spot": "600s",
                "execution_buffer": "0s",
                "period": "1800s",
            },
            "timestamp": "event_timestamp",
            "timestamp_metadata": None,
            "value_by": None,
            "windows": ["30m"],
            "offset": None,
        },
        "type": "groupby",
    }
    # note: raw graph's node parameters is not pruned
    expected_raw_groupby_params = expected_groupby_node["parameters"].copy()
    expected_raw_groupby_params["names"] = ["sum_30m", "sum_2h", "sum_1d"]
    expected_raw_groupby_params["windows"] = ["30m", "2h", "1d"]
    expected_raw_groupby_params["aggregation_id"] = "sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    expected_raw_groupby_params["tile_id"] = "TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295"
    expected_raw_groupby_node = {**expected_groupby_node, "parameters": expected_raw_groupby_params}
    async for doc in feature_service.list_documents_as_dict_iterator(
        query_filter={"_id": feature.id}
    ):
        graph = QueryGraphModel(**doc["graph"])
        raw_graph = QueryGraphModel(**doc["raw_graph"])
        groupby_node = graph.get_node_by_name("groupby_1")
        raw_groupby_node = raw_graph.get_node_by_name("groupby_1")
        assert groupby_node.model_dump() == expected_groupby_node
        assert raw_groupby_node.model_dump() == expected_raw_groupby_node


@pytest.mark.asyncio
async def test_update_readiness(feature_service, feature):
    """Test update readiness method checks for block modification by"""
    reference_info = ReferenceInfo(asset_name="some_asset_name", document_id=ObjectId())
    await feature_service.add_block_modification_by(
        query_filter={"_id": feature.id}, reference_info=reference_info
    )
    updated_feature = await feature_service.get_document(document_id=feature.id)
    assert updated_feature.block_modification_by == [reference_info]

    with pytest.raises(DocumentModificationBlockedError):
        await feature_service.update_readiness(document_id=feature.id, readiness="production_ready")


@pytest.mark.asyncio
async def test_update_last_updated_date(feature_service, feature):
    """Test update_last_updated_by_scheduled_task_at method"""
    updated_feature = await feature_service.get_document(document_id=feature.id)
    assert updated_feature.last_updated_by_scheduled_task_at is None

    last_updated_date = datetime.utcnow()
    aggregation_ids = updated_feature.aggregation_ids
    await feature_service.update_last_updated_by_scheduled_task_at(
        aggregation_ids=[aggregation_ids[0]], last_updated_by_scheduled_task_at=last_updated_date
    )

    new_updated_feature = await feature_service.get_document(document_id=feature.id)
    date_format = "%Y-%m-%d %H:%M:%S"
    assert new_updated_feature.last_updated_by_scheduled_task_at.strftime(
        date_format
    ) == last_updated_date.strftime(date_format)


def test_validate_feature(feature_service, feature, snowflake_event_table_id):
    """Test validate feature method"""
    feature_dict = feature.model_dump(by_alias=True)

    # construct a feature with inconsistent feature job setting
    query_graph = QueryGraph(**feature_dict["graph"])
    view_node = query_graph.get_node_by_name(node_name="graph_1")
    groupby_node = query_graph.get_node_by_name(node_name="groupby_1")
    feature_node = query_graph.get_node_by_name(node_name="project_1")
    node_params = groupby_node.model_dump(by_alias=True)["parameters"]
    node_params["names"] = ["sum_1w"]
    node_params["windows"] = ["1w"]
    node_params["feature_job_setting"]["period"] = "3600s"
    another_groupby_node = query_graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[view_node],
    )
    another_feature = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["sum_1w"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[another_groupby_node],
    )
    divide_node = query_graph.add_operation(
        node_type=NodeType.DIV,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[feature_node, another_feature],
    )
    alias_node = query_graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "sum_1w_div_sum_30m"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[divide_node],
    )

    # prepare new feature model
    feature_dict["graph"] = query_graph
    feature_dict["node_name"] = alias_node.name
    feature_dict["name"] = alias_node.parameters.name
    new_feature = FeatureModel(**feature_dict)
    assert new_feature.name == "sum_1w_div_sum_30m"

    # validate feature
    with pytest.raises(DocumentCreationError) as exc:
        feature_service.validate_feature(feature=new_feature)

    expected_message = (
        f"Feature job settings for table {snowflake_event_table_id} are not consistent. "
        "Two different feature job settings are found: blind_spot='600s' period='1800s' "
        "offset='300s' execution_buffer='0s' and blind_spot='600s' period='3600s' offset='300s' "
        "execution_buffer='0s'"
    )
    assert expected_message in str(exc.value)
