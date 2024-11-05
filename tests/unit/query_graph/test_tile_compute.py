from featurebyte.query_graph.sql.tile_compute import OnDemandTileComputePlan
from featurebyte.query_graph.sql.tile_util import get_max_window_sizes


def test_combine_tile_tables(query_graph_with_similar_groupby_nodes, source_info):
    nodes, graph = query_graph_with_similar_groupby_nodes
    plan = OnDemandTileComputePlan("REQUEST_TABLE", source_info)

    # Check that each groupby node has the same tile_id. Their respective tile sqls have to be
    # merged into one
    tile_id = "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725"
    for node in nodes:
        assert node.parameters.tile_id == tile_id

    for node in nodes:
        plan.process_node(graph, node)

    # Check maximum window size is tracked correctly
    max_window_sizes = get_max_window_sizes(
        tile_info_list=plan.tile_infos, key_name="tile_table_id"
    )
    assert max_window_sizes[tile_id] == 172800
