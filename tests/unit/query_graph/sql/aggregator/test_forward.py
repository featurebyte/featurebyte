"""
Test forward aggregator
"""

from __future__ import annotations

import pytest
from sqlglot import select

from featurebyte.enum import DBVarType
from featurebyte.query_graph.node.generic import ForwardAggregateParameters
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.aggregator.forward import ForwardAggregator
from featurebyte.query_graph.sql.specs import AggregationSource, ForwardAggregateSpec
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture(name="forward_node_parameters")
def forward_node_parameters_fixture(entity_id):
    return ForwardAggregateParameters(
        name="target",
        timestamp_col="timestamp_col",
        window="7d",
        table_details=TableDetails(table_name="table"),
        keys=["cust_id", "other_key"],
        parent="value",
        agg_func="sum",
        value_by="col_float",
        serving_names=["serving_cust_id"],
        entity_ids=[entity_id],
    )


@pytest.fixture(name="forward_node_parameters_with_offset")
def forward_node_parameters_with_offset_fixture(forward_node_parameters):
    forward_node_parameters.offset = "1d"
    return forward_node_parameters


@pytest.fixture(name="forward_spec")
def forward_spec_fixture(forward_node_parameters, entity_id):
    """
    forward spec fixture
    """
    return ForwardAggregateSpec(
        node_name="forward_aggregate_1",
        feature_name=forward_node_parameters.name,
        serving_names=["serving_cust_id", "other_serving_key"],
        serving_names_mapping=None,
        parameters=forward_node_parameters,
        aggregation_source=AggregationSource(
            expr=select("*").from_("tab"), query_node_name="input_1"
        ),
        entity_ids=[entity_id],
        parent_dtype=DBVarType.FLOAT,
        agg_result_name_include_serving_names=True,
        is_deployment_sql=False,
    )


@pytest.fixture(name="forward_spec_with_offset")
def forward_spec_with_offset_fixture(forward_node_parameters_with_offset, entity_id):
    """
    forward spec with offset fixture
    """
    return ForwardAggregateSpec(
        node_name="forward_aggregate_1",
        feature_name=forward_node_parameters_with_offset.name,
        serving_names=["serving_cust_id", "other_serving_key"],
        serving_names_mapping=None,
        parameters=forward_node_parameters_with_offset,
        aggregation_source=AggregationSource(
            expr=select("*").from_("tab"), query_node_name="input_1"
        ),
        entity_ids=[entity_id],
        parent_dtype=DBVarType.FLOAT,
        agg_result_name_include_serving_names=True,
        is_deployment_sql=False,
    )


def test_forward_aggregator(forward_spec, update_fixtures, source_info):
    """
    Test forward aggregator
    """
    aggregator = ForwardAggregator(source_info=source_info)
    aggregator.update(forward_spec)

    result = aggregator.update_aggregation_table_expr(
        select("a", "b", "c").from_("REQUEST_TABLE"), "POINT_INT_TIME", ["a", "b", "c"], 0
    )

    result_sql = result.updated_table_expr.sql(pretty=True)
    assert_equal_with_expected_fixture(
        result_sql,
        "tests/fixtures/aggregator/expected_forward_aggregator.sql",
        update_fixtures,
    )


def test_forward_aggregator_offset(forward_spec_with_offset, update_fixtures, source_info):
    """
    Test forward aggregator with offset
    """
    aggregator = ForwardAggregator(source_info=source_info)
    aggregator.update(forward_spec_with_offset)

    result = aggregator.update_aggregation_table_expr(
        select("a", "b", "c").from_("REQUEST_TABLE"), "POINT_INT_TIME", ["a", "b", "c"], 0
    )

    result_sql = result.updated_table_expr.sql(pretty=True)
    assert_equal_with_expected_fixture(
        result_sql,
        "tests/fixtures/aggregator/expected_forward_aggregator_offset.sql",
        update_fixtures,
    )
