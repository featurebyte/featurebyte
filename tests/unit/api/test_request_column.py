"""
RequestColumn unit tests
"""

import pytest

from featurebyte.api.feature import Feature
from featurebyte.api.request_column import RequestColumn
from featurebyte.enum import DBVarType
from featurebyte.models import FeatureModel
from featurebyte.query_graph.transform.offline_store_ingest import (
    OfflineStoreIngestQueryGraphTransformer,
)
from tests.util.helper import (
    check_decomposed_graph_output_node_hash,
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
    # After changes to handle point-in-time request columns, the feature is no longer decomposed
    # since all transformations happen in the offline store
    assert output.graph.edges_map == {}

    # check output node hash
    check_decomposed_graph_output_node_hash(
        feature_model=new_feature_model,
        output=output,
    )
    # Note: check_on_demand_feature_code_generation is not called because the feature
    # is no longer decomposed after changes to handle point-in-time request columns


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
    transformer = OfflineStoreIngestQueryGraphTransformer(graph=new_feature_model.graph)
    output = transformer.transform(
        target_node=new_feature_model.node,
        relationships_info=new_feature_model.relationships_info,
        feature_name=new_feature_model.name,
        feature_version=new_feature_model.version.to_str(),
    )

    # check decomposed graph structure
    # After changes to handle point-in-time request columns, the feature is no longer decomposed
    # since all transformations happen in the offline store
    assert output.graph.edges_map == {}

    # check output node hash
    check_decomposed_graph_output_node_hash(
        feature_model=new_feature_model,
        output=output,
    )
