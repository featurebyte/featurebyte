"""
RequestColumn unit tests
"""

import pytest
from bson import ObjectId

from featurebyte.api.context import Context
from featurebyte.api.feature import Feature
from featurebyte.api.request_column import RequestColumn
from featurebyte.common.model_util import get_version
from featurebyte.enum import DBVarType, TimeIntervalUnit
from featurebyte.models import FeatureModel
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
from featurebyte.query_graph.transform.offline_store_ingest import (
    OfflineStoreIngestQueryGraphTransformer,
)
from tests.util.helper import (
    check_decomposed_graph_output_node_hash,
    check_on_demand_feature_code_generation,
    check_sdk_code_generation,
    deploy_features_through_api,
    reset_global_graph,
)


def test_point_in_time_request_column():
    """
    Test point_in_time request column
    """
    reset_global_graph()
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
            "context_id": None,
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


def test_internal_create_request_column():
    """
    Test internal _create_request_column works for arbitrary column names and dtypes
    """
    request_col = RequestColumn._create_request_column("annual_income", DBVarType.FLOAT)
    assert isinstance(request_col, RequestColumn)
    assert request_col.name == "annual_income"
    assert request_col.dtype == DBVarType.FLOAT
    assert request_col.tabular_source is None
    assert request_col.feature_store is None
    node_dict = request_col.node.model_dump()
    assert node_dict["type"] == "request_column"
    assert node_dict["output_type"] == "series"
    assert node_dict["parameters"]["column_name"] == "annual_income"
    assert node_dict["parameters"]["dtype"] == "FLOAT"
    assert node_dict["parameters"]["context_id"] is None


def test_internal_create_request_column_with_context_id():
    """
    Test internal _create_request_column stores context_id in node parameters
    """
    context_id = ObjectId("6471a3d0f2b3c8a1e9d5f012")
    request_col = RequestColumn._create_request_column(
        "annual_income", DBVarType.FLOAT, context_id=context_id
    )
    assert isinstance(request_col, RequestColumn)
    node_dict = request_col.node.model_dump()
    assert node_dict["parameters"]["context_id"] == context_id


@pytest.mark.parametrize(
    "column_name, column_dtype",
    [
        ("credit_score", DBVarType.INT),
        ("customer_name", DBVarType.VARCHAR),
        ("is_active", DBVarType.BOOL),
    ],
)
def test_internal_create_request_column_various_dtypes(column_name, column_dtype):
    """
    Test internal _create_request_column works with various dtypes
    """
    request_col = RequestColumn._create_request_column(column_name, column_dtype)
    assert isinstance(request_col, RequestColumn)
    assert request_col.name == column_name
    assert request_col.dtype == column_dtype
    node_dict = request_col.node.model_dump()
    assert node_dict["parameters"]["column_name"] == column_name
    assert node_dict["parameters"]["dtype"] == column_dtype.value


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


def test_forecast_point_request_column():
    """
    Test forecast_point request column creation via create_request_column
    """
    # Create a FORECAST_POINT request column with DATE dtype (default)
    forecast_point = RequestColumn._create_request_column(
        "FORECAST_POINT",
        DBVarType.DATE,
    )
    assert isinstance(forecast_point, RequestColumn)
    node_dict = forecast_point.node.model_dump()
    # Check node properties (excluding name which depends on global state)
    assert node_dict["type"] == "request_column"
    assert node_dict["output_type"] == "series"
    assert node_dict["parameters"] == {
        "column_name": "FORECAST_POINT",
        "dtype": "DATE",
        "dtype_info": {"dtype": "DATE", "metadata": None},
        "context_id": None,
    }


def test_forecast_point_minus_timestamp_feature(
    latest_event_timestamp_feature,
    cust_id_entity,
    transaction_entity,
    mock_deployment_flow,
    update_fixtures,
):
    """
    Test an on-demand feature involving forecast point.
    """
    _ = mock_deployment_flow
    _ = transaction_entity

    # Create a context with forecast_point_schema
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone="America/New_York",
    )
    forecast_context = Context(
        name="forecast_context",
        primary_entity_ids=[cust_id_entity.id],
        forecast_point_schema=forecast_schema,
    )
    forecast_context.save()

    # Create a feature using forecast_point from context
    new_feature = (forecast_context.forecast_point - latest_event_timestamp_feature).dt.day
    new_feature.name = "Days Until Forecast (from latest event)"
    assert isinstance(new_feature, Feature)

    # Verify the feature has correct properties
    assert new_feature.entity_ids == latest_event_timestamp_feature.entity_ids

    # Save the feature and deploy
    new_feature.save()
    deploy_features_through_api([new_feature])

    # Check SDK code generation
    loaded_feature = Feature.get(new_feature.name)
    check_sdk_code_generation(
        loaded_feature,
        to_use_saved_data=True,
        fixture_path="tests/fixtures/sdk_code/feature_with_forecast_point.py",
        update_fixtures=update_fixtures,
        table_id=new_feature.table_ids[0],
        context_id=forecast_context.id,
    )

    # Verify the feature model
    new_feature_model = new_feature.cached_model
    assert isinstance(new_feature_model, FeatureModel)
    check_on_demand_feature_code_generation(feature_model=new_feature_model)


# def test_forecast_point_dt_hour(cust_id_entity, transaction_entity, mock_deployment_flow):
#     """
#     Test a simple on-demand feature using forecast_point.dt.hour.
#     """
#     _ = mock_deployment_flow
#     _ = transaction_entity

#     # Create a context with forecast_point_schema
#     forecast_schema = ForecastPointSchema(
#         granularity=TimeIntervalUnit.DAY,
#         dtype=DBVarType.DATE,
#         is_utc_time=False,
#         timezone="America/New_York",
#     )
#     forecast_context = Context(
#         name="forecast_context_hour",
#         primary_entity_ids=[cust_id_entity.id],
#         forecast_point_schema=forecast_schema,
#     )
#     forecast_context.save()

#     # Create a feature using forecast_point.dt.hour
#     new_feature = forecast_context.forecast_point.dt.hour
#     new_feature.name = "Forecast Hour"
#     assert isinstance(new_feature, Feature)

#     # Save the feature
#     new_feature.save()

#     # Verify the feature model
#     new_feature_model = new_feature.cached_model
#     assert isinstance(new_feature_model, FeatureModel)
#     check_on_demand_feature_code_generation(feature_model=new_feature_model)
