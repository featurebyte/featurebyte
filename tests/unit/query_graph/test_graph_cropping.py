"""
Test for graph cropping related logics
"""
import pytest

from featurebyte.query_graph.graph import QueryGraph


def check_cropped_graph(original_graph, cropped_graph, target_nodes, mapped_node_names):
    """Check the cropped graph by comparing the pruned graphs from the original graph & the cropped graph"""
    for target_node, mapped_node_name in zip(target_nodes, mapped_node_names):
        pruned_graph, node_name_map = original_graph.prune(
            target_node=target_node, aggressive=False
        )
        pruned_target_node_name = node_name_map[target_node.name]

        cropped_graph = QueryGraph(**cropped_graph.dict())
        target_node_cropped = cropped_graph.get_node_by_name(mapped_node_name)
        pruned_graph_cropped, node_name_map = cropped_graph.prune(
            target_node=target_node_cropped, aggressive=False
        )
        pruned_target_node_name_cropped = node_name_map[target_node_cropped.name]
        assert pruned_graph == pruned_graph_cropped
        assert pruned_target_node_name == pruned_target_node_name_cropped


@pytest.mark.parametrize(
    "target_columns,expected_node_num",
    [
        (["CUST_ID"], 2),
        (["CUST_ID", "VALUE"], 3),
        (["CUST_ID", "VALUE", "MASK"], 4),
    ],
)
def test_crop__simple_case(dataframe, target_columns, expected_node_num):
    """Test cropping a simple graph"""
    target_nodes = [dataframe[col].node for col in target_columns]
    cropped_graph, node_name_map = dataframe.graph.crop(
        target_node_names=[node.name for node in target_nodes]
    )
    mapped_node_names = [node_name_map[node.name] for node in target_nodes]
    check_cropped_graph(dataframe.graph, cropped_graph, target_nodes, mapped_node_names)
    assert len(cropped_graph.nodes) == expected_node_num


@pytest.mark.parametrize("target_node_num,expected_node_num", [(1, 6), (2, 7), (3, 7), (4, 9)])
def test_crop(
    order_size_feature_join_node,
    order_size_agg_by_cust_id_graph,
    order_size_feature_node,
    lookup_node,
    target_node_num,
    expected_node_num,
):
    """Test cropping a complex graph"""
    graph, order_size_agg_by_cust_id = order_size_agg_by_cust_id_graph
    target_nodes = [
        order_size_feature_join_node,
        order_size_agg_by_cust_id,
        order_size_feature_node,
        lookup_node,
    ][:target_node_num]
    cropped_graph, node_name_map = graph.crop(
        target_node_names=[node.name for node in target_nodes]
    )
    mapped_node_names = [node_name_map[node.name] for node in target_nodes]
    check_cropped_graph(
        graph,
        cropped_graph,
        target_nodes,
        mapped_node_names,
    )
    assert len(cropped_graph.nodes) == expected_node_num
