"""
Test forward aggregator
"""
from __future__ import annotations

import textwrap

import pytest
from sqlglot import select

from featurebyte import SourceType
from featurebyte.query_graph.node.generic import ForwardAggregateParameters
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.aggregator.forward import ForwardAggregator
from featurebyte.query_graph.sql.specs import AggregationSource, ForwardAggregateSpec


@pytest.fixture(name="forward_node_parameters")
def forward_node_parameters_fixture(entity_id):
    return ForwardAggregateParameters(
        name="target",
        timestamp_col="timestamp_col",
        horizon="7d",
        table_details=TableDetails(table_name="table"),
        keys=["cust_id", "other_key"],
        parent="value",
        agg_func="sum",
        value_by="col_float",
        serving_names=["serving_cust_id"],
        entity_ids=[entity_id],
    )


@pytest.fixture(name="forward_spec")
def forward_spec_fixture(forward_node_parameters, entity_id):
    """
    forward spec fixture
    """
    return ForwardAggregateSpec(
        serving_names=["serving_cust_id", "other_serving_key"],
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
          c,
          "T0"."_fb_internal_forward_sum_value_cust_id_other_key_col_float_input_1" AS "_fb_internal_forward_sum_value_cust_id_other_key_col_float_input_1"
        FROM REQUEST_TABLE
        LEFT JOIN (
          SELECT
            INNER_."POINT_IN_TIME",
            INNER_."serving_cust_id",
            INNER_."other_serving_key",
            OBJECT_AGG(
              CASE
                WHEN INNER_."col_float" IS NULL
                THEN '__MISSING__'
                ELSE CAST(INNER_."col_float" AS TEXT)
              END,
              TO_VARIANT(
                INNER_."_fb_internal_forward_sum_value_cust_id_other_key_col_float_input_1_inner"
              )
            ) AS "_fb_internal_forward_sum_value_cust_id_other_key_col_float_input_1"
          FROM (
            SELECT
              REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
              REQ."serving_cust_id" AS "serving_cust_id",
              REQ."other_serving_key" AS "other_serving_key",
              SOURCE_TABLE."col_float" AS "col_float",
              SUM(SOURCE_TABLE."value") AS "_fb_internal_forward_sum_value_cust_id_other_key_col_float_input_1_inner"
            FROM "REQUEST_TABLE_POINT_IN_TIME_serving_cust_id_other_serving_key" AS REQ
            INNER JOIN (
              SELECT
                *
              FROM tab
            ) AS SOURCE_TABLE
              ON (
                DATE_PART(EPOCH_SECOND, SOURCE_TABLE."timestamp_col") > FLOOR(DATE_PART(EPOCH_SECOND, REQ."POINT_IN_TIME"))
                AND DATE_PART(EPOCH_SECOND, SOURCE_TABLE."timestamp_col") <= FLOOR(DATE_PART(EPOCH_SECOND, REQ."POINT_IN_TIME") + 604800.0)
              )
              AND REQ."serving_cust_id" = SOURCE_TABLE."cust_id"
              AND REQ."other_serving_key" = SOURCE_TABLE."other_key"
            GROUP BY
              REQ."POINT_IN_TIME",
              REQ."serving_cust_id",
              REQ."other_serving_key",
              SOURCE_TABLE."col_float"
          ) AS INNER_
          GROUP BY
            INNER_."POINT_IN_TIME",
            INNER_."serving_cust_id",
            INNER_."other_serving_key"
        ) AS T0
          ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
          AND REQ."serving_cust_id" = T0."serving_cust_id"
          AND REQ."other_serving_key" = T0."other_serving_key"
        """
    ).strip()
    assert result.updated_table_expr.sql(pretty=True) == expected
