"""
RequestColumn unit tests
"""

import pytest

from featurebyte.api.feature import Feature
from featurebyte.api.request_column import RequestColumn
from featurebyte.common.model_util import get_version
from featurebyte.enum import DBVarType
from featurebyte.models import FeatureModel
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.transform.offline_store_ingest import (
    OfflineStoreIngestQueryGraphTransformer,
)
from tests.util.helper import (
    check_decomposed_graph_output_node_hash,
    check_on_demand_feature_code_generation,
    check_sdk_code_generation,
    deploy_features_through_api,
)


def test_point_in_time_request_column():
    """
    Test point_in_time request column
    """
    point_in_time = RequestColumn.point_in_time()
    assert isinstance(point_in_time, RequestColumn)
    node_dict = point_in_time.node.model_dump()
    assert node_dict == {
        "name": "request_column_1",
        "type": "request_column",
        "output_type": "series",
        "parameters": {
            "column_name": "POINT_IN_TIME",
            "dtype": "TIMESTAMP",
            "dtype_info": {"dtype": "TIMESTAMP", "metadata": None},
        },
    }


def test_point_in_time_minus_timestamp_feature(
    latest_event_timestamp_feature, update_fixtures, mock_deployment_flow
):
    """
    Test an on-demand feature involving point in time
    """
    _ = mock_deployment_flow
    new_feature = (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day
    new_feature.name = "Time Since Last Event (days)"
    assert isinstance(new_feature, Feature)

    assert new_feature.entity_ids == latest_event_timestamp_feature.entity_ids
    assert new_feature.feature_store == latest_event_timestamp_feature.feature_store
    assert new_feature.tabular_source == latest_event_timestamp_feature.tabular_source

    new_feature.save()
    deploy_features_through_api([new_feature])

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
    transformer = OfflineStoreIngestQueryGraphTransformer(graph=new_feature_model.graph)
    output = transformer.transform(
        target_node=new_feature_model.node,
        relationships_info=new_feature_model.relationships_info,
        feature_name=new_feature_model.name,
        feature_version=new_feature_model.version.to_str(),
    )

    # check decomposed graph structure
    assert output.graph.edges_map == {
        "date_diff_1": ["timedelta_extract_1"],
        "graph_1": ["date_diff_1"],
        "request_column_1": ["date_diff_1"],
        "timedelta_extract_1": ["alias_1"],
    }
    graph_node_param = output.graph.nodes_map["graph_1"].parameters
    assert graph_node_param.type == GraphNodeType.OFFLINE_STORE_INGEST_QUERY
    assert (
        graph_node_param.output_column_name
        == f"__Time Since Last Event (days)_{get_version()}__part0"
    )

    # check output node hash
    check_decomposed_graph_output_node_hash(
        feature_model=new_feature_model,
        output=output,
    )
    check_on_demand_feature_code_generation(feature_model=new_feature_model)


def test_request_column_non_point_in_time_blocked():
    """
    Test non-supported request column is blocked
    """
    with pytest.raises(NotImplementedError) as exc:
        _ = RequestColumn.create_request_column("foo", DBVarType.FLOAT)
    assert "Only POINT_IN_TIME and FORECAST_POINT columns are supported" in str(exc.value)


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
    transformer = OfflineStoreIngestQueryGraphTransformer(graph=new_feature_model.graph)
    output = transformer.transform(
        target_node=new_feature_model.node,
        relationships_info=new_feature_model.relationships_info,
        feature_name=new_feature_model.name,
        feature_version=new_feature_model.version.to_str(),
    )

    # check decomposed graph structure
    assert output.graph.edges_map == {
        "date_add_1": ["date_diff_2"],
        "date_diff_1": ["date_add_1"],
        "date_diff_2": ["timedelta_extract_1"],
        "graph_1": ["date_diff_2"],
        "request_column_1": ["date_diff_1", "date_diff_1", "date_add_1"],
        "timedelta_extract_1": ["alias_1"],
    }

    # check output node hash
    check_decomposed_graph_output_node_hash(
        feature_model=new_feature_model,
        output=output,
    )
