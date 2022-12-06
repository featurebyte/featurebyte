from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.tile_compute import OnDemandTileComputePlan


def test_combine_tile_tables(query_graph_with_similar_groupby_nodes):
    nodes, graph = query_graph_with_similar_groupby_nodes
    plan = OnDemandTileComputePlan("2022-04-20 10:00:00", SourceType.SNOWFLAKE)

    # Check that each groupby node has the same tile_id. Their respective tile sqls have to be
    # merged into one
    tile_id = "fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d"
    for node in nodes:
        assert node.parameters.tile_id == tile_id

    for node in nodes:
        plan.process_node(graph, node)

    # Check maximum window size is tracked correctly
    assert plan.get_max_window_size(tile_id) == 172800
