"""
Test forward aggregator.
"""
import pytest

from featurebyte import AggFunc
from featurebyte.api.aggregator.forward_aggregator import ForwardAggregator
from featurebyte.query_graph.enum import NodeOutputType, NodeType


@pytest.fixture(name="forward_aggregator")
def forward_aggregator_fixture(snowflake_event_view):
    """
    Fixture for forward aggregator
    """
    columns_info = snowflake_event_view.columns_info
    entity_ids = []
    for col in columns_info:
        if col.entity_id:
            entity_ids.append(col.entity_id)
    return ForwardAggregator(
        view=snowflake_event_view,
        category=None,
        entity_ids=entity_ids,
        keys=["col_int"],
        serving_names=["serving_name"],
    )


def test_forward_aggregate(forward_aggregator):
    """
    Test forward_aggregate.
    """
    target = forward_aggregator.forward_aggregate("col_float", AggFunc.SUM, "7d", "target")
    # Assert Target is constructed with appropriate values
    assert target.name == "target"
    # assert target.window == "7d"  # TODO: Fix this

    # Assert forward aggregate node has been added into the graph
    view = forward_aggregator.view
    target_node = target.node
    forward_aggregate_nodes = [
        node for node in view.graph.iterate_nodes(target_node, NodeType.FORWARD_AGGREGATE)
    ]
    assert len(forward_aggregate_nodes) == 1
    forward_aggregate_node = forward_aggregate_nodes[0]
    assert forward_aggregate_node.type == NodeType.FORWARD_AGGREGATE
    assert forward_aggregate_node.output_type == NodeOutputType.FRAME
    assert forward_aggregate_node.parameters.dict() == {
        "keys": forward_aggregator.keys,
        "name": "target",
        "parent": "col_float",
        "agg_func": AggFunc.SUM,
        "window": "7d",
        "serving_names": forward_aggregator.serving_names,
        "value_by": None,
        "entity_ids": forward_aggregator.entity_ids,
        "table_details": forward_aggregator.view.tabular_source.table_details.dict(),
        "timestamp_col": "event_timestamp",
    }

    # Assert Target's current node is the project node
    assert target_node.type == NodeType.PROJECT
    assert target_node.output_type == NodeOutputType.SERIES
    assert target_node.parameters.dict() == {
        "columns": ["target"],
    }

    # Get operation structure to verify output category
    operation_structure = target.graph.extract_operation_structure(target_node)
    assert operation_structure.output_category == "target"


def test_prepare_node_parameters(forward_aggregator):
    """
    Test prepare node parameters
    """
    node_params = forward_aggregator._prepare_node_parameters(
        value_column="col_float",
        method=AggFunc.SUM,
        window="7d",
        target_name="target",
        timestamp_col="timestamp_col",
    )
    assert node_params == {
        "keys": forward_aggregator.keys,
        "parent": "col_float",
        "agg_func": "sum",
        "window": "7d",
        "name": "target",
        "serving_names": forward_aggregator.serving_names,
        "value_by": None,
        "entity_ids": forward_aggregator.entity_ids,
        "table_details": forward_aggregator.view.tabular_source.table_details,
        "timestamp_col": "timestamp_col",
    }


@pytest.mark.parametrize(
    "value_column, method, window, target_name, expected_error",
    [
        ("col_float", AggFunc.SUM, "7d", "target", None),
        ("random", AggFunc.SUM, "7d", "target", KeyError),
        ("col_float", "random", "7d", "target", ValueError),
        ("col_float", AggFunc.SUM, "random", "target", ValueError),
    ],
)
def test_validate_parameters(
    forward_aggregator, value_column, method, window, target_name, expected_error
):
    """
    Test validate parameters
    """
    if expected_error:
        with pytest.raises(expected_error):
            forward_aggregator._validate_parameters(
                value_column=value_column,
                method=method,
                window=window,
                target_name=target_name,
            )
    else:
        forward_aggregator._validate_parameters(
            value_column=value_column,
            method=method,
            window=window,
            target_name=target_name,
        )
