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
    target = forward_aggregator.forward_aggregate("col_float", AggFunc.SUM, "7d", "1d", "target")
    # Assert Target is constructed with appropriate values
    assert target.name == "target"
    assert target.horizon == "7d"
    assert target.blind_spot == "1d"

    # Assert forward aggregate node has been added into the graph
    view = forward_aggregator.view
    target_node = target.internal_graph.get_node_by_name(target.internal_node_name)
    forward_aggregate_nodes = [
        node for node in view.graph.iterate_nodes(target_node, NodeType.FORWARD_AGGREGATE)
    ]
    assert len(forward_aggregate_nodes) == 1
    forward_aggregate_node = forward_aggregate_nodes[0]
    assert forward_aggregate_node.type == NodeType.FORWARD_AGGREGATE
    assert forward_aggregate_node.parameters.dict() == {
        "keys": forward_aggregator.keys,
        "parent": "col_float",
        "agg_func": AggFunc.SUM,
        "horizon": "7d",
        "blind_spot": "1d",
        "serving_name": forward_aggregator.serving_names[0],
        "value_by": "col_float",
        "entity_ids": forward_aggregator.entity_ids,
        "table_details": forward_aggregator.view.tabular_source.table_details.dict(),
    }

    # Assert Target's current node is the project node
    assert target_node.type == NodeType.PROJECT
    assert target_node.output_type == NodeOutputType.SERIES
    assert target_node.parameters.dict() == {
        "columns": ["target"],
    }


def test_prepare_node_parameters(forward_aggregator):
    """
    Test prepare node parameters
    """
    node_params = forward_aggregator._prepare_node_parameters(
        value_column="col_float",
        method=AggFunc.SUM,
        horizon="7d",
        blind_spot="1d",
        target_name="target",
    )
    assert node_params == {
        "keys": forward_aggregator.keys,
        "parent": "col_float",
        "agg_func": "sum",
        "horizon": "7d",
        "blind_spot": "1d",
        "name": "target",
        "serving_name": forward_aggregator.serving_names[0],
        "value_by": "col_float",
        "entity_ids": forward_aggregator.entity_ids,
        "table_details": forward_aggregator.view.tabular_source.table_details,
    }


@pytest.mark.parametrize(
    "value_column, method, horizon, blind_spot, target_name, expected_error",
    [
        ("col_float", AggFunc.SUM, "7d", "1d", "target", None),
        ("random", AggFunc.SUM, "7d", "random", "target", KeyError),
        ("col_float", "random", "7d", "1d", "target", ValueError),
        ("col_float", AggFunc.SUM, "random", "1d", "target", ValueError),
        ("col_float", AggFunc.SUM, "7d", "random", "target", ValueError),
    ],
)
def test_validate_parameters(
    forward_aggregator, value_column, method, horizon, blind_spot, target_name, expected_error
):
    """
    Test validate parameters
    """
    if expected_error:
        with pytest.raises(expected_error):
            forward_aggregator._validate_parameters(
                value_column=value_column,
                method=method,
                horizon=horizon,
                blind_spot=blind_spot,
                target_name=target_name,
            )
    else:
        forward_aggregator._validate_parameters(
            value_column=value_column,
            method=method,
            horizon=horizon,
            blind_spot=blind_spot,
            target_name=target_name,
        )
