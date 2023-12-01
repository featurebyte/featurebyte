"""
RequestColumn unit tests
"""
import pytest

from featurebyte.api.feature import Feature
from featurebyte.api.request_column import RequestColumn
from featurebyte.enum import DBVarType
from featurebyte.models import FeatureModel
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.transform.offline_ingest_extractor import (
    OfflineStoreIngestQueryGraphExtractor,
)
from tests.util.helper import check_decomposed_graph_output_node_hash, check_sdk_code_generation


def test_point_in_time_request_column():
    """
    Test point_in_time request column
    """
    point_in_time = RequestColumn.point_in_time()
    assert isinstance(point_in_time, RequestColumn)
    node_dict = point_in_time.node.dict()
    assert node_dict == {
        "name": "request_column_1",
        "type": "request_column",
        "output_type": "series",
        "parameters": {"column_name": "POINT_IN_TIME", "dtype": "TIMESTAMP"},
    }


def test_point_in_time_minus_timestamp_feature(latest_event_timestamp_feature, update_fixtures):
    """
    Test an on-demand feature involving point in time
    """
    new_feature = (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day
    new_feature.name = "Time Since Last Event (days)"
    assert isinstance(new_feature, Feature)

    assert new_feature.entity_ids == latest_event_timestamp_feature.entity_ids
    assert new_feature.feature_store == latest_event_timestamp_feature.feature_store
    assert new_feature.tabular_source == latest_event_timestamp_feature.tabular_source

    new_feature.save()
    loaded_feature = Feature.get(new_feature.name)
    check_sdk_code_generation(
        loaded_feature,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/feature_with_request_column.py",
        update_fixtures=update_fixtures,
        table_id=new_feature.table_ids[0],
    )

    # check offline store ingest query graph
    new_feature_model = new_feature.cached_model
    assert isinstance(new_feature_model, FeatureModel)
    extractor = OfflineStoreIngestQueryGraphExtractor(graph=new_feature_model.graph)
    output = extractor.extract(
        node=new_feature_model.node, relationships_info=new_feature_model.relationships_info
    )

    # check decomposed graph structure
    assert output.graph.edges_map == {
        "graph_1": ["date_diff_1"],
        "graph_2": ["date_diff_1"],
        "date_diff_1": ["timedelta_extract_1"],
        "timedelta_extract_1": ["alias_1"],
    }
    # make sure the graph node types are expected (graph_1 & graph_2 order depends on the node hash,
    # which is not deterministic)
    graph_node_param1 = output.graph.nodes_map["graph_1"].parameters
    graph_node_param2 = output.graph.nodes_map["graph_2"].parameters
    assert {graph_node_param1.type, graph_node_param2.type} == {
        GraphNodeType.OFFLINE_STORE_INGEST_QUERY,
        GraphNodeType.OFFLINE_STORE_REQUEST_COLUMN_QUERY,
    }
    if graph_node_param1.type == GraphNodeType.OFFLINE_STORE_REQUEST_COLUMN_QUERY:
        assert graph_node_param1.output_column_name == "__feature__req_part0"
        assert graph_node_param2.output_column_name == "__feature__part0"
    else:
        assert graph_node_param1.output_column_name == "__feature__part0"
        assert graph_node_param2.output_column_name == "__feature__req_part0"

    # check output node hash
    check_decomposed_graph_output_node_hash(
        feature_model=new_feature_model,
        offline_store_ingest_query_graph_extractor_output=output,
    )


def test_request_column_non_point_in_time_blocked():
    """
    Test non-point-in-time request column is blocked
    """
    with pytest.raises(NotImplementedError) as exc:
        _ = RequestColumn.create_request_column("foo", DBVarType.FLOAT)
    assert "Currently only POINT_IN_TIME column is supported" in str(exc.value)


def test_request_column_offline_store_query_extraction(latest_event_timestamp_feature):
    """Test offline store query extraction for request column"""
    new_feature = (
        RequestColumn.point_in_time()
        + (RequestColumn.point_in_time() - RequestColumn.point_in_time())
        - latest_event_timestamp_feature
    ).dt.day
    new_feature.name = "Time Since Last Event (days)"
    new_feature.save()

    # check offline store ingest query graph
    new_feature_model = new_feature.cached_model
    assert isinstance(new_feature_model, FeatureModel)
    extractor = OfflineStoreIngestQueryGraphExtractor(graph=new_feature_model.graph)
    output = extractor.extract(
        node=new_feature_model.node, relationships_info=new_feature_model.relationships_info
    )

    # check decomposed graph structure
    assert output.graph.edges_map == {
        "graph_1": ["date_diff_1"],
        "graph_2": ["date_diff_1"],
        "date_diff_1": ["timedelta_extract_1"],
        "timedelta_extract_1": ["alias_1"],
    }

    graph_node_1 = output.graph.nodes_map["graph_1"]
    graph_node_2 = output.graph.nodes_map["graph_2"]
    if graph_node_1.parameters.type == GraphNodeType.OFFLINE_STORE_REQUEST_COLUMN_QUERY:
        request_graph_node = graph_node_1
    else:
        request_graph_node = graph_node_2

    # check request graph node structure: make sure the graph node contains request column operations
    assert request_graph_node.parameters.graph.edges_map == {
        "request_column_1": ["date_diff_1", "date_diff_1", "date_add_1"],
        "date_diff_1": ["date_add_1"],
    }

    # check output node hash
    check_decomposed_graph_output_node_hash(
        feature_model=new_feature_model,
        offline_store_ingest_query_graph_extractor_output=output,
    )
