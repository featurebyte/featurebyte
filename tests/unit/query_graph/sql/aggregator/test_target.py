"""
Test target aggregator
"""
import textwrap

import pytest
from sqlglot import select

from featurebyte import SourceType
from featurebyte.query_graph.node.mixin import BaseGroupbyParameters
from featurebyte.query_graph.sql.aggregator.target import TargetAggregator
from featurebyte.query_graph.sql.specs import AggregationSource, TargetSpec


@pytest.fixture(name="target_node_parameters")
def target_node_parameters_fixture(entity_id):
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


@pytest.fixture(name="target_spec")
def target_spec_fixture(target_node_parameters, entity_id):
    """
    Target spec fixture
    """
    return TargetSpec(
        serving_names=["serving_cust_id"],
        serving_names_mapping=None,
        parameters=target_node_parameters,
        aggregation_source=AggregationSource(
            expr=select("*").from_("tab"), query_node_name="input_1"
        ),
        entity_ids=[entity_id],
    )


def test_target_aggregator(target_spec):
    """
    Test target aggregator
    """
    aggregator = TargetAggregator(source_type=SourceType.SNOWFLAKE)
    aggregator.update(target_spec)

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
