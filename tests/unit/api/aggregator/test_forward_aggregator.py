"""
Test forward aggregator.
"""

import textwrap

import pytest

from featurebyte import AggFunc
from featurebyte.api.aggregator.forward_aggregator import ForwardAggregator
from featurebyte.query_graph.enum import NodeOutputType, NodeType


@pytest.fixture(name="forward_aggregator")
def forward_aggregator_fixture(snowflake_event_view_with_entity):
    """
    Fixture for forward aggregator
    """
    columns_info = snowflake_event_view_with_entity.columns_info
    keys = ["col_int"]
    entity_ids = []
    for col in columns_info:
        if col.name in keys:
            entity_ids.append(col.entity_id)
    return ForwardAggregator(
        view=snowflake_event_view_with_entity,
        category=None,
        entity_ids=entity_ids,
        keys=keys,
        serving_names=["serving_name"],
    )


def test_forward_aggregate_fill_na(forward_aggregator):
    """
    Test forward_aggregate.
    """
    fill_value = 1
    expected_warning = "The parameters 'skip_fill_na' and 'fill_value' are deprecated and will be removed in a future version."
    with pytest.raises(ValueError) as exc_info:
        with pytest.warns(match=expected_warning):
            forward_aggregator.forward_aggregate(
                "col_float", AggFunc.SUM, "7d", "target", fill_value, True
            )

    assert "Specifying both fill_value and skip_fill_na is not allowed" in str(exc_info.value)

    # Verify that the fill value node is there
    with pytest.warns(match=expected_warning):
        target = forward_aggregator.forward_aggregate(
            "col_float", AggFunc.SUM, "7d", "target", fill_value
        )

    target_node = target.node

    # Check that we have the fill value
    conditional_nodes = [
        node
        for node in forward_aggregator.view.graph.iterate_nodes(target_node, NodeType.CONDITIONAL)
    ]
    assert len(conditional_nodes) == 1
    conditional_node = conditional_nodes[0]
    assert conditional_node.type == NodeType.CONDITIONAL
    assert conditional_node.parameters.model_dump() == {"value": 1}

    # Check that the final node is the alias node, and that it has the target name
    assert target_node.type == NodeType.ALIAS
    assert target_node.output_type == NodeOutputType.SERIES
    assert target_node.parameters.model_dump() == {"name": "target"}


@pytest.mark.parametrize("offset", [None, "1d"])
def test_forward_aggregate(forward_aggregator, offset):
    """
    Test forward_aggregate.
    """
    target = forward_aggregator.forward_aggregate(
        "col_float", AggFunc.SUM, "7d", "target", offset=offset
    )
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
    assert forward_aggregate_node.parameters.model_dump() == {
        "keys": forward_aggregator.keys,
        "name": "target",
        "parent": "col_float",
        "agg_func": AggFunc.SUM,
        "window": "7d",
        "offset": None if offset is None else offset,
        "serving_names": forward_aggregator.serving_names,
        "value_by": None,
        "entity_ids": forward_aggregator.entity_ids,
        "timestamp_col": "event_timestamp",
        "timestamp_metadata": None,
    }

    # Assert Target's current node is the project node
    assert target_node.type == NodeType.PROJECT
    assert target_node.output_type == NodeOutputType.SERIES
    assert target_node.parameters.model_dump() == {"columns": ["target"]}

    # Get operation structure to verify output category
    operation_structure = target.graph.extract_operation_structure(
        target_node,
        keep_all_source_columns=True,
    )
    assert operation_structure.output_category == "target"


def test_fillna_preserve_target_name(forward_aggregator):
    """
    Test forward_aggregate.
    """
    target = forward_aggregator.forward_aggregate(
        "col_float",
        AggFunc.SUM,
        "7d",
        "col_float_target",
    )
    target.fillna(0)
    assert target.name == "col_float_target"
    target.save()

    # Must be ALIAS node type and not CONDITIONAL
    target_model = target.cached_model
    node = target_model.graph.get_node_by_name(target_model.node_name)
    assert node.model_dump() == {
        "name": "alias_1",
        "type": NodeType.ALIAS,
        "output_type": "series",
        "parameters": {"name": "col_float_target"},
    }


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
        offset="1d",
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
        "timestamp_col": "timestamp_col",
        "timestamp_metadata": None,
        "offset": "1d",
    }


@pytest.mark.parametrize(
    "value_column, method, window, target_name, expected_error",
    [
        ("col_float", AggFunc.SUM, "7d", "target", None),
        ("random", AggFunc.SUM, "7d", "target", KeyError),
        ("col_float", "random", "7d", "target", ValueError),
        ("col_float", AggFunc.SUM, "random", "target", ValueError),
        (None, AggFunc.COUNT, "7d", "target", None),
        ("col_float", AggFunc.COUNT, "7d", "target", ValueError),
        (None, AggFunc.SUM, "7d", "target", ValueError),
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


def test_forward_aggregate__fill_value(snowflake_event_view_with_entity):
    """
    Test forward_aggregate.
    """
    # create target without fill_value
    target = snowflake_event_view_with_entity.groupby("col_int").forward_aggregate(
        value_column="col_float",
        method="sum",
        window="7d",
        target_name="target",
        fill_value=0.0,
    )
    target.save()

    # definition expected fill null value operations
    partial_definition = """
    target = event_view.groupby(
        by_keys=["col_int"], category=None
    ).forward_aggregate(
        value_column="col_float",
        method="sum",
        window="7d",
        target_name="target",
        fill_value=None,
        skip_fill_na=True,
        offset=None,
    )
    target_1 = target.copy()
    target_1[target.isnull()] = 0.0
    target_1.name = "target"
    output = target_1
    """
    assert textwrap.dedent(partial_definition).strip() in target.definition
