"""
Test for VersionService
"""
import json
import os

import pytest
import pytest_asyncio

from featurebyte.common.model_util import get_version
from featurebyte.exception import DocumentError
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature_list import FeatureListNewVersionMode
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.feature import FeatureCreate, FeatureNewVersionCreate
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListNewVersionCreate,
    FeatureVersionInfo,
)


@pytest.mark.asyncio
async def test_create_new_feature_version(
    version_service, feature, feature_namespace_service, get_credential
):
    """Test create new feature version"""
    version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            feature_job_setting=FeatureJobSetting(
                blind_spot="1d", frequency="1d", time_modulo_frequency="1h"
            ),
        ),
        get_credential=get_credential,
    )

    # compare groupby node
    expected_common_params = {
        "keys": ["cust_id"],
        "parent": "col_float",
        "agg_func": "sum",
        "value_by": None,
        "windows": ["30m", "2h", "1d"],
        "timestamp": "event_timestamp",
        "names": ["sum_30m", "sum_2h", "sum_1d"],
        "serving_names": ["cust_id"],
    }
    parameters = feature.graph.get_node_by_name("groupby_1").parameters
    assert parameters.dict() == {
        **expected_common_params,
        "blind_spot": 600,
        "time_modulo_frequency": 300,
        "frequency": 1800,
        "tile_id": "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
        "aggregation_id": "sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
    }

    new_parameters = version.graph.get_node_by_name("groupby_1").parameters
    assert new_parameters.dict() == {
        **expected_common_params,
        "blind_spot": 86400,
        "time_modulo_frequency": 3600,
        "frequency": 86400,
        "tile_id": "sf_table_f86400_m3600_b86400_d1c26f05670a559eec7e90107400f228b14c209f",
        "aggregation_id": "sum_27c04cd4b7f10ab112b95b39b21f294d353752f7",
    }

    # compare edges & other nodes
    assert feature.graph.edges == version.graph.edges
    for node_name in ["input_1", "project_1"]:
        assert feature.graph.get_node_by_name(node_name) == version.graph.get_node_by_name(
            node_name
        )
    _ = feature

    # check version
    version_name = get_version()
    assert feature.version == {"name": version_name, "suffix": None}
    assert version.version == {"name": version_name, "suffix": 1}

    # check other attributes
    assert version.node_name == "project_1"
    assert version.readiness == "DRAFT"
    assert version.feature_namespace_id == feature.feature_namespace_id
    assert version.entity_ids == feature.entity_ids
    assert version.feature_list_ids == []
    assert version.deployed_feature_list_ids == []
    assert version.online_enabled is False

    # check feature namespace service get updated
    namespace = await feature_namespace_service.get_document(
        document_id=feature.feature_namespace_id
    )
    assert namespace.feature_ids == [feature.id, version.id]


@pytest.fixture(name="invalid_query_graph_groupby_node")
def invalid_query_graph_groupby_node_fixture(
    snowflake_feature_store_details_dict, snowflake_table_details_dict
):
    """Invalid query graph fixture"""
    groupby_node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "avg",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["a_2h_average", "a_48h_average"],
        "windows": ["2h", "48h"],
    }
    graph = QueryGraph()
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "generic",
            "columns": ["random_column"],
            "table_details": snowflake_table_details_dict,
            "feature_store_details": snowflake_feature_store_details_dict,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    node_groupy = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params=groupby_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input],
    )
    return graph, node_groupy


def test_version_service__iterate_groupby_and_event_input_node_pairs__invalid_graph(
    version_service, invalid_query_graph_groupby_node
):
    """Test value error is raised when the input graph is invalid"""
    graph, node = invalid_query_graph_groupby_node
    with pytest.raises(ValueError) as exc:
        for (
            groupby_node,
            input_node,
        ) in version_service._iterate_groupby_and_event_data_input_node_pairs(
            graph=graph, target_node=node
        ):
            _ = groupby_node, input_node
    expected_msg = "Groupby node does not have valid event data!"
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_version__document_error(version_service, feature, get_credential):
    """Test create new feature version (document error due to no change is detected)"""
    # check no feature job settings
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(source_feature_id=feature.id, feature_job_setting=None),
            get_credential=get_credential,
        )

    expected_msg = "No change detected on the new feature version."
    assert expected_msg in str(exc.value)

    # check same feature job settings
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(
                source_feature_id=feature.id,
                feature_job_setting=FeatureJobSetting(
                    blind_spot="10m", frequency="30m", time_modulo_frequency="5m"
                ),
            ),
            get_credential=get_credential,
        )

    assert expected_msg in str(exc.value)


@pytest_asyncio.fixture(name="feature_sum_2h")
async def feature_sum_2h_fixture(test_dir, feature_service, feature, mock_insert_feature_registry):
    """Feature sum_2h fixture"""
    _ = mock_insert_feature_registry, feature
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_2h.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature = await feature_service.create_document(data=FeatureCreate(**payload))
        return feature


@pytest_asyncio.fixture(name="feature_list_multi")
async def feature_list_fixture(test_dir, feature, feature_sum_2h, feature_list_service):
    """Feature list model"""
    _ = feature, feature_sum_2h
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_list_multi.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature_list = await feature_list_service.create_document(data=FeatureListCreate(**payload))
        return feature_list


@pytest.mark.asyncio
async def test_create_new_feature_list_version__document_error__no_change_detected(
    version_service, feature_list_multi, get_credential
):
    """Test create new feature version (error due to no change detected)"""
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=feature_list_multi.id,
                mode=FeatureListNewVersionMode.AUTO,
            ),
            get_credential=get_credential,
        )

    expected_msg = "No change detected on the new feature list version."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__document_error__feature_info_is_missing(
    version_service, feature_list_multi, get_credential
):
    """Test create new feature version (error due to feature info is missing)"""
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=feature_list_multi.id,
                mode=FeatureListNewVersionMode.MANUAL,
            ),
            get_credential=get_credential,
        )
    expected_msg = "Feature info is missing."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__document_error__unexpected_feature_info(
    version_service, feature_list, feature_sum_2h, get_credential
):
    """Test create new feature version (error due to unexpected feature)"""
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=feature_list.id,
                mode=FeatureListNewVersionMode.MANUAL,
                features=[
                    FeatureVersionInfo(name=feature_sum_2h.name, version=feature_sum_2h.version)
                ],
            ),
            get_credential=get_credential,
        )
    expected_msg = 'Features ("sum_2h") are not in the original FeatureList'
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__auto_mode(
    version_service,
    feature,
    feature_sum_2h,
    feature_list_multi,
    feature_readiness_service,
    get_credential,
):
    """Test create new feature version (auto mode)"""
    new_feat_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            feature_job_setting=FeatureJobSetting(
                blind_spot="1d", frequency="1d", time_modulo_frequency="1h"
            ),
        ),
        get_credential=get_credential,
    )
    feat_namespace = await feature_readiness_service.update_feature_namespace(
        feature_namespace_id=new_feat_version.feature_namespace_id, return_document=True
    )
    assert feat_namespace.default_feature_id == new_feat_version.id

    new_flist_version = await version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(
            source_feature_list_id=feature_list_multi.id,
            mode=FeatureListNewVersionMode.AUTO,
        ),
        get_credential=get_credential,
    )
    assert sorted(new_flist_version.feature_ids) == sorted([new_feat_version.id, feature_sum_2h.id])

    # check document error (due to no change is detected)
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=new_flist_version.id,
                mode=FeatureListNewVersionMode.AUTO,
            ),
            get_credential=get_credential,
        )
    expected_msg = "No change detected on the new feature list version."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__manual_mode(
    version_service,
    feature,
    feature_sum_2h,
    feature_list_multi,
    feature_readiness_service,
    get_credential,
):
    """Test create new feature version (manual mode)"""
    new_feat_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature_sum_2h.id,
            feature_job_setting=FeatureJobSetting(
                blind_spot="1d", frequency="1d", time_modulo_frequency="1h"
            ),
        ),
        get_credential=get_credential,
    )
    feat_namespace = await feature_readiness_service.update_feature_namespace(
        feature_namespace_id=new_feat_version.feature_namespace_id, return_document=True
    )
    assert feat_namespace.default_feature_id == new_feat_version.id

    new_flist_version = await version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(
            source_feature_list_id=feature_list_multi.id,
            mode=FeatureListNewVersionMode.MANUAL,
            features=[
                FeatureVersionInfo(name=new_feat_version.name, version=new_feat_version.version)
            ],
        ),
        get_credential=get_credential,
    )
    assert sorted(new_flist_version.feature_ids) == sorted([new_feat_version.id, feature.id])

    # check document error (due to no change is detected)
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=new_flist_version.id,
                mode=FeatureListNewVersionMode.MANUAL,
                features=[
                    FeatureVersionInfo(name=new_feat_version.name, version=new_feat_version.version)
                ],
            ),
            get_credential=get_credential,
        )
    expected_msg = "No change detected on the new feature list version."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__semi_auto_mode(
    version_service,
    feature,
    feature_sum_2h,
    feature_list_multi,
    feature_readiness_service,
    get_credential,
):
    """Test create new feature version (semi-auto mode)"""
    new_feat_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature_sum_2h.id,
            feature_job_setting=FeatureJobSetting(
                blind_spot="1d", frequency="1d", time_modulo_frequency="1h"
            ),
        ),
        get_credential=get_credential,
    )
    feat_namespace = await feature_readiness_service.update_feature_namespace(
        feature_namespace_id=new_feat_version.feature_namespace_id, return_document=True
    )
    assert feat_namespace.default_feature_id == new_feat_version.id

    new_flist_version = await version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(
            source_feature_list_id=feature_list_multi.id,
            mode=FeatureListNewVersionMode.SEMI_AUTO,
            features=[FeatureVersionInfo(name=feature.name, version=feature.version)],
        ),
        get_credential=get_credential,
    )
    assert sorted(new_flist_version.feature_ids) == sorted([new_feat_version.id, feature.id])

    # check document error (due to no change is detected)
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=feature_list_multi.id,
                mode=FeatureListNewVersionMode.SEMI_AUTO,
                features=[
                    FeatureVersionInfo(name=feature_sum_2h.name, version=feature_sum_2h.version)
                ],
            ),
            get_credential=get_credential,
        )
    expected_msg = "No change detected on the new feature list version."
    assert expected_msg in str(exc.value)
