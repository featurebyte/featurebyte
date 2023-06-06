"""
Tests for featurebyte.query_graph.feature_common
"""

from bson import ObjectId

from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec


def test_aggregation_spec__from_groupby_query_node(
    query_graph_with_groupby,
    groupby_node_aggregation_id,
    expected_pruned_graph_and_node_1,
    expected_pruned_graph_and_node_2,
):
    """
    Test constructing list of AggregationSpec from groupby query graph node
    """
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    agg_specs = TileBasedAggregationSpec.from_groupby_query_node(
        query_graph_with_groupby, groupby_node, adapter=get_sql_adapter(SourceType.SNOWFLAKE)
    )
    expected_agg_specs = [
        TileBasedAggregationSpec(
            window=7200,
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
            **expected_pruned_graph_and_node_1,
        ),
        TileBasedAggregationSpec(
            window=172800,
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
            **expected_pruned_graph_and_node_2,
        ),
    ]
    assert agg_specs == expected_agg_specs


def test_aggregation_spec__override_serving_names(
    query_graph_with_groupby,
    groupby_node_aggregation_id,
    expected_pruned_graph_and_node_1,
    expected_pruned_graph_and_node_2,
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
        adapter=get_sql_adapter(SourceType.SNOWFLAKE),
        serving_names_mapping=serving_names_mapping,
    )
    expected_agg_specs = [
        TileBasedAggregationSpec(
            window=7200,
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
            **expected_pruned_graph_and_node_1,
        ),
        TileBasedAggregationSpec(
            window=172800,
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
            **expected_pruned_graph_and_node_2,
        ),
    ]
    assert agg_specs == expected_agg_specs
