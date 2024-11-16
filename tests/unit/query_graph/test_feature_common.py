"""
Tests for featurebyte.query_graph.feature_common
"""

import pytest
from bson import ObjectId
from sqlglot import expressions
from sqlglot.expressions import Identifier

from featurebyte.enum import AggFunc, DBVarType
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec


@pytest.fixture(name="graph_and_groupby_node")
def graph_and_groupby_node_fixture(query_graph_with_groupby):
    """
    Groupby node fixture
    """
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    return query_graph_with_groupby, groupby_node


@pytest.fixture(name="expected_tile_based_aggregation_specs")
def expected_tile_based_aggregation_specs_fixture(
    graph_and_groupby_node,
    groupby_node_aggregation_id,
    expected_pruned_graph_and_node_1,
    expected_pruned_graph_and_node_2,
):
    """
    Expected aggregation specs
    """
    _, groupby_node = graph_and_groupby_node
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
            merge_expr=expressions.Div(
                this=expressions.Sum(
                    this=Identifier(
                        this=f"sum_value_avg_{groupby_node_aggregation_id}",
                    )
                ),
                expression=expressions.Sum(
                    this=Identifier(
                        this=f"count_value_avg_{groupby_node_aggregation_id}",
                    )
                ),
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
            merge_expr=expressions.Div(
                this=expressions.Sum(
                    this=Identifier(
                        this=f"sum_value_avg_{groupby_node_aggregation_id}",
                    )
                ),
                expression=expressions.Sum(
                    this=Identifier(
                        this=f"count_value_avg_{groupby_node_aggregation_id}",
                    )
                ),
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
    return expected_agg_specs


def test_aggregation_spec__from_groupby_query_node(
    graph_and_groupby_node, expected_tile_based_aggregation_specs, adapter
):
    """
    Test constructing list of AggregationSpec from groupby query graph node
    """
    query_graph_with_groupby, groupby_node = graph_and_groupby_node
    agg_specs = TileBasedAggregationSpec.from_groupby_query_node(
        query_graph_with_groupby,
        groupby_node,
        adapter=adapter,
        agg_result_name_include_serving_names=True,
    )
    assert agg_specs == expected_tile_based_aggregation_specs


def test_aggregation_spec__override_serving_names(
    graph_and_groupby_node,
    expected_tile_based_aggregation_specs,
    adapter,
):
    """
    Test constructing list of AggregationSpec with serving names mapping provided
    """
    query_graph_with_groupby, groupby_node = graph_and_groupby_node
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
    # Update the expected tile specs since serving_names_mapping is provided
    for spec in expected_tile_based_aggregation_specs:
        spec.serving_names = ["NEW_CUST_ID"]
        spec.serving_names_mapping = serving_names_mapping
    assert agg_specs == expected_tile_based_aggregation_specs


def test_tile_based_aggregation_spec__on_demand_tile_tables(
    graph_and_groupby_node, expected_tile_based_aggregation_specs, adapter
):
    """
    Test tile_table_id in the specs are updated if on demand tile tables mapping is provided
    """
    on_demand_tile_tables_mapping = {
        "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725": "__MY_TEMP_TILE_TABLE"
    }
    query_graph_with_groupby, groupby_node = graph_and_groupby_node
    agg_specs = TileBasedAggregationSpec.from_groupby_query_node(
        query_graph_with_groupby,
        groupby_node,
        adapter=adapter,
        agg_result_name_include_serving_names=True,
        on_demand_tile_tables_mapping=on_demand_tile_tables_mapping,
    )
    # Update the expected tile specs since on_demand_tile_tables_mapping is provided
    for agg_spec in expected_tile_based_aggregation_specs:
        agg_spec.tile_table_id = "__MY_TEMP_TILE_TABLE"
    assert agg_specs == expected_tile_based_aggregation_specs
