from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.tile_compute import OnDemandTileComputePlan


def test_combine_tile_tables(query_graph_with_similar_groupby_nodes):
    nodes, graph = query_graph_with_similar_groupby_nodes
    plan = OnDemandTileComputePlan(["2022-04-20 10:00:00"], "REQUEST_TABLE", SourceType.SNOWFLAKE)

    # Check that each groupby node has the same tile_id. Their respective tile sqls have to be
    # merged into one
    tile_id = "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725"
    for node in nodes:
        assert node.parameters.tile_id == tile_id

    for node in nodes:
        plan.process_node(graph, node)

    # Check maximum window size is tracked correctly
    assert plan.get_max_window_size(tile_id) == 172800
