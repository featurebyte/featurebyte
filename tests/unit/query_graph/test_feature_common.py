"""
Tests for featurebyte.query_graph.feature_common
"""

from featurebyte.query_graph.feature_common import AggregationSpec


def test_aggregation_spec__from_groupby_query_node(query_graph_with_groupby):
    """
    Test constructing list of AggregationSpec from groupby query graph node
    """
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    agg_specs = AggregationSpec.from_groupby_query_node(groupby_node)
    expected_agg_specs = [
        AggregationSpec(
            window=7200,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
            aggregation_id="avg_53307fe1790a553cf1ca703e44b92619ad86dc8f",
            keys=["cust_id"],
            serving_names=["CUSTOMER_ID"],
            value_by=None,
            merge_expr="SUM(sum_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f) / SUM(count_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f)",
            feature_name="a_2h_average",
        ),
        AggregationSpec(
            window=172800,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
            aggregation_id="avg_53307fe1790a553cf1ca703e44b92619ad86dc8f",
            keys=["cust_id"],
            serving_names=["CUSTOMER_ID"],
            value_by=None,
            merge_expr="SUM(sum_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f) / SUM(count_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f)",
            feature_name="a_48h_average",
        ),
    ]
    assert agg_specs == expected_agg_specs


def test_aggregation_spec__override_serving_names(query_graph_with_groupby):
    """
    Test constructing list of AggregationSpec with serving names mapping provided
    """
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    serving_names_mapping = {
        "CUSTOMER_ID": "NEW_CUST_ID",
    }
    agg_specs = AggregationSpec.from_groupby_query_node(
        groupby_node, serving_names_mapping=serving_names_mapping
    )
    expected_agg_specs = [
        AggregationSpec(
            window=7200,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
            aggregation_id="avg_53307fe1790a553cf1ca703e44b92619ad86dc8f",
            keys=["cust_id"],
            serving_names=["NEW_CUST_ID"],
            value_by=None,
            merge_expr="SUM(sum_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f) / SUM(count_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f)",
            feature_name="a_2h_average",
        ),
        AggregationSpec(
            window=172800,
            frequency=3600,
            blind_spot=900,
            time_modulo_frequency=1800,
            tile_table_id="fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d",
            aggregation_id="avg_53307fe1790a553cf1ca703e44b92619ad86dc8f",
            keys=["cust_id"],
            serving_names=["NEW_CUST_ID"],
            value_by=None,
            merge_expr="SUM(sum_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f) / SUM(count_value_avg_53307fe1790a553cf1ca703e44b92619ad86dc8f)",
            feature_name="a_48h_average",
        ),
    ]
    assert agg_specs == expected_agg_specs
