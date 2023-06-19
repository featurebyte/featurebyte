"""
Test forward aggregator
"""
import textwrap

import pytest
from sqlglot import select

from featurebyte import SourceType
from featurebyte.query_graph.node.mixin import BaseGroupbyParameters
from featurebyte.query_graph.sql.aggregator.forward import ForwardAggregator
from featurebyte.query_graph.sql.specs import AggregationSource, ForwardAggregateSpec


@pytest.fixture(name="forward_node_parameters")
def forward_node_parameters_fixture(entity_id):
    return BaseGroupbyParameters(
        keys=["cust_id"],
        serving_names=["serving_cust_id"],
        parent="value",
        agg_func="sum",
        natural_key_column="scd_key",
        effective_timestamp_column="effective_ts",
        name="asat_feature",
        entity_ids=[entity_id],
    )


@pytest.fixture(name="forward_spec")
def forward_spec_fixture(forward_node_parameters, entity_id):
    """
    forward spec fixture
    """
    return ForwardAggregateSpec(
        serving_names=["serving_cust_id"],
        serving_names_mapping=None,
        parameters=forward_node_parameters,
        aggregation_source=AggregationSource(
            expr=select("*").from_("tab"), query_node_name="input_1"
        ),
        entity_ids=[entity_id],
    )


def test_forward_aggregator(forward_spec):
    """
    Test forward aggregator
    """
    aggregator = ForwardAggregator(source_type=SourceType.SNOWFLAKE)
    aggregator.update(forward_spec)

    result = aggregator.update_aggregation_table_expr(
        select("a", "b", "c").from_("REQUEST_TABLE"), "POINT_INT_TIME", ["a", "b", "c"], 0
    )

    expected = textwrap.dedent(
        """
        SELECT
          a,
          b,
          c
        FROM REQUEST_TABLE
        """
    ).strip()
    assert result.updated_table_expr.sql(pretty=True) == expected
