"""
Test for graph quick pruning related logics
"""
import pytest

from featurebyte.query_graph.graph import QueryGraph


def check_quick_pruned_graph(original_graph, quick_pruned_graph, target_nodes, mapped_node_names):
    """Check the aggressively prune graph of the original graph and the quick pruned graph are the same"""
    for target_node, mapped_node_name in zip(target_nodes, mapped_node_names):
        # perform pruning original graph
        pruned_graph, node_name_map = original_graph.prune(target_node=target_node)
        pruned_target_node_name = node_name_map[target_node.name]

        # convert query graph from QueryGraphModel to QueryGraph
        quick_pruned_graph = QueryGraph(**quick_pruned_graph.dict())
        target_node_quick = quick_pruned_graph.get_node_by_name(mapped_node_name)

        # perform pruning on quick-pruned graph
        pruned_graph_from_quick_pruned, node_name_map = quick_pruned_graph.prune(
            target_node=target_node_quick
        )
        pruned_target_node_name_from_quick_pruned = node_name_map[target_node_quick.name]

        # compare both pruned graphs and they should be the same
        assert pruned_graph == pruned_graph_from_quick_pruned
        assert pruned_target_node_name == pruned_target_node_name_from_quick_pruned


@pytest.mark.parametrize(
    "target_columns,expected_node_num",
    [
        (["CUST_ID"], 2),
        (["CUST_ID", "VALUE"], 3),
        (["CUST_ID", "VALUE", "MASK"], 4),
    ],
)
def test_quick_prune__simple_case(dataframe, target_columns, expected_node_num):
    """Test quick pruning a simple graph"""
    target_nodes = [dataframe[col].node for col in target_columns]
    pruned_graph, node_name_map = dataframe.graph.quick_prune(
        target_node_names=[node.name for node in target_nodes]
    )
    mapped_node_names = [node_name_map[node.name] for node in target_nodes]
    check_quick_pruned_graph(dataframe.graph, pruned_graph, target_nodes, mapped_node_names)
    assert len(pruned_graph.nodes) == expected_node_num


@pytest.mark.parametrize("target_node_num,expected_node_num", [(1, 6), (2, 7), (3, 7), (4, 9)])
def test_quick_prune(
    order_size_feature_join_node,
    order_size_agg_by_cust_id_graph,
    order_size_feature_node,
    lookup_node,
    target_node_num,
    expected_node_num,
):
    """Test quick pruning a complex graph"""
    graph, order_size_agg_by_cust_id = order_size_agg_by_cust_id_graph
    target_nodes = [
        order_size_feature_join_node,
        order_size_agg_by_cust_id,
        order_size_feature_node,
        lookup_node,
    ][:target_node_num]
    quick_pruned_graph, node_name_map = graph.quick_prune(
        target_node_names=[node.name for node in target_nodes]
    )
    mapped_node_names = [node_name_map[node.name] for node in target_nodes]
    check_quick_pruned_graph(
        graph,
        quick_pruned_graph,
        target_nodes,
        mapped_node_names,
    )
    assert len(quick_pruned_graph.nodes) == expected_node_num
