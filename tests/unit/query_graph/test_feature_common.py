"""
Tests for featurebyte.query_graph.feature_common
"""

from featurebyte.query_graph.sql.specs import WindowAggregationSpec


def test_aggregation_spec__from_groupby_query_node(
    query_graph_with_groupby, groupby_node_aggregation_id
):
    """
    Test constructing list of AggregationSpec from groupby query graph node
    """
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    agg_specs = WindowAggregationSpec.from_groupby_query_node(groupby_node)
    expected_agg_specs = [
        WindowAggregationSpec(
            window=7200,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
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
        ),
        WindowAggregationSpec(
            window=172800,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
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
        ),
    ]
    assert agg_specs == expected_agg_specs


def test_aggregation_spec__override_serving_names(
    query_graph_with_groupby, groupby_node_aggregation_id
):
    """
    Test constructing list of AggregationSpec with serving names mapping provided
    """
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    serving_names_mapping = {
        "CUSTOMER_ID": "NEW_CUST_ID",
    }
    agg_specs = WindowAggregationSpec.from_groupby_query_node(
        groupby_node, serving_names_mapping=serving_names_mapping
    )
    expected_agg_specs = [
        WindowAggregationSpec(
            window=7200,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
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
        ),
        WindowAggregationSpec(
            window=172800,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
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
        ),
    ]
    assert agg_specs == expected_agg_specs
