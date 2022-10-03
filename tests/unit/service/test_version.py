"""
Test for VersionService
"""
import pytest

from featurebyte.common.model_util import get_version
from featurebyte.exception import DocumentError
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.feature import VersionCreate


@pytest.mark.asyncio
async def test_create_new_feature_version(
    version_service, feature, feature_namespace_service, get_credential
):
    """Test create new feature version"""
    version = await version_service.create_new_feature_version(
        data=VersionCreate(
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
            data=VersionCreate(source_feature_id=feature.id, feature_job_setting=None),
            get_credential=get_credential,
        )

    expected_msg = "No change detected on the new feature version."
    assert expected_msg in str(exc.value)

    # check same feature job settings
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=VersionCreate(
                source_feature_id=feature.id,
                feature_job_setting=FeatureJobSetting(
                    blind_spot="10m", frequency="30m", time_modulo_frequency="5m"
                ),
            ),
            get_credential=get_credential,
        )

    assert expected_msg in str(exc.value)
