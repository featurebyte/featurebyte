"""
Tests for featurebyte.query_graph.feature_common
"""

from bson import ObjectId

from featurebyte.enum import AggFunc, DBVarType
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec


def test_aggregation_spec__from_groupby_query_node(
    query_graph_with_groupby,
    groupby_node_aggregation_id,
    expected_pruned_graph_and_node_1,
    expected_pruned_graph_and_node_2,
    adapter,
):
    """
    Test constructing list of AggregationSpec from groupby query graph node
    """
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    agg_specs = TileBasedAggregationSpec.from_groupby_query_node(
        query_graph_with_groupby,
        groupby_node,
        adapter=adapter,
        agg_result_name_include_serving_names=True,
    )
    expected_agg_specs = [
        TileBasedAggregationSpec(
            node_name=groupby_node.name,
            window=7200,
            offset=None,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
            aggregation_id=f"avg_{groupby_node_aggregation_id}",
            keys=["cust_id"],
            serving_names=["CUSTOMER_ID"],
            serving_names_mapping=None,
            value_by=None,
            merge_expr=(
                f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                f"SUM(count_value_avg_{groupby_node_aggregation_id})"
            ),
            feature_name="a_2h_average",
            is_order_dependent=False,
            tile_value_columns=[
                f"sum_value_avg_{groupby_node_aggregation_id}",
                f"count_value_avg_{groupby_node_aggregation_id}",
            ],
            entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
            dtype=DBVarType.FLOAT,
            agg_func=AggFunc.AVG,
            agg_result_name_include_serving_names=True,
            **expected_pruned_graph_and_node_1,
        ),
        TileBasedAggregationSpec(
            node_name=groupby_node.name,
            window=172800,
            offset=None,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
            aggregation_id=f"avg_{groupby_node_aggregation_id}",
            keys=["cust_id"],
            serving_names=["CUSTOMER_ID"],
            serving_names_mapping=None,
            value_by=None,
            merge_expr=(
                f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                f"SUM(count_value_avg_{groupby_node_aggregation_id})"
            ),
            feature_name="a_48h_average",
            is_order_dependent=False,
            tile_value_columns=[
                f"sum_value_avg_{groupby_node_aggregation_id}",
                f"count_value_avg_{groupby_node_aggregation_id}",
            ],
            entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
            dtype=DBVarType.FLOAT,
            agg_func=AggFunc.AVG,
            agg_result_name_include_serving_names=True,
            **expected_pruned_graph_and_node_2,
        ),
    ]
    assert agg_specs == expected_agg_specs


def test_aggregation_spec__override_serving_names(
    query_graph_with_groupby,
    groupby_node_aggregation_id,
    expected_pruned_graph_and_node_1,
    expected_pruned_graph_and_node_2,
    adapter,
):
    """
    Test constructing list of AggregationSpec with serving names mapping provided
    """
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    serving_names_mapping = {
        "CUSTOMER_ID": "NEW_CUST_ID",
    }
    agg_specs = TileBasedAggregationSpec.from_groupby_query_node(
        query_graph_with_groupby,
        groupby_node,
        adapter=adapter,
        serving_names_mapping=serving_names_mapping,
        agg_result_name_include_serving_names=True,
    )
    expected_agg_specs = [
        TileBasedAggregationSpec(
            node_name=groupby_node.name,
            window=7200,
            offset=None,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
            aggregation_id=f"avg_{groupby_node_aggregation_id}",
            keys=["cust_id"],
            serving_names=["NEW_CUST_ID"],
            serving_names_mapping=serving_names_mapping,
            value_by=None,
            merge_expr=(
                f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                f"SUM(count_value_avg_{groupby_node_aggregation_id})"
            ),
            feature_name="a_2h_average",
            is_order_dependent=False,
            tile_value_columns=[
                f"sum_value_avg_{groupby_node_aggregation_id}",
                f"count_value_avg_{groupby_node_aggregation_id}",
            ],
            entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
            dtype=DBVarType.FLOAT,
            agg_func=AggFunc.AVG,
            agg_result_name_include_serving_names=True,
            **expected_pruned_graph_and_node_1,
        ),
        TileBasedAggregationSpec(
            node_name=groupby_node.name,
            window=172800,
            offset=None,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
            aggregation_id=f"avg_{groupby_node_aggregation_id}",
            keys=["cust_id"],
            serving_names=["NEW_CUST_ID"],
            serving_names_mapping=serving_names_mapping,
            value_by=None,
            merge_expr=(
                f"SUM(sum_value_avg_{groupby_node_aggregation_id}) / "
                f"SUM(count_value_avg_{groupby_node_aggregation_id})"
            ),
            feature_name="a_48h_average",
            is_order_dependent=False,
            tile_value_columns=[
                f"sum_value_avg_{groupby_node_aggregation_id}",
                f"count_value_avg_{groupby_node_aggregation_id}",
            ],
            entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
            dtype=DBVarType.FLOAT,
            agg_func=AggFunc.AVG,
            agg_result_name_include_serving_names=True,
            **expected_pruned_graph_and_node_2,
        ),
    ]
    assert agg_specs == expected_agg_specs
