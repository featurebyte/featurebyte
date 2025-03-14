"""
Unit tests for featurebyte.query_graph.sql.aggregator.lookup.LookupAggregator
"""

from __future__ import annotations

import textwrap
from dataclasses import asdict

import pytest

from featurebyte.query_graph.node.generic import EventLookupParameters, SCDLookupParameters
from featurebyte.query_graph.sql.aggregator.lookup import LookupAggregator
from featurebyte.query_graph.sql.specifications.lookup import LookupSpec


@pytest.fixture
def dimension_lookup_specs(global_graph, lookup_node, source_info):
    """
    Fixture for a list of LookupSpec derived from lookup_node
    """
    return LookupSpec.from_query_graph_node(
        lookup_node,
        graph=global_graph,
        source_info=source_info,
    )


@pytest.fixture
def event_lookup_specs(global_graph, event_lookup_node, source_info):
    """
    Fixture for a list of LookupSpec derived from event_lookup_node
    """
    return LookupSpec.from_query_graph_node(
        event_lookup_node,
        graph=global_graph,
        source_info=source_info,
    )


@pytest.fixture
def scd_lookup_specs_with_current_flag(
    global_graph, scd_lookup_node, is_online_serving, source_info
):
    """
    Fixture for a list of LookupSpec derived from SCD lookup
    """
    return LookupSpec.from_query_graph_node(
        scd_lookup_node,
        graph=global_graph,
        source_info=source_info,
        is_online_serving=is_online_serving,
    )


@pytest.fixture
def scd_lookup_specs_without_current_flag(
    global_graph, scd_lookup_without_current_flag_node, is_online_serving, source_info
):
    """
    Fixture for a list of LookupSpec derived from SCD lookup without current flag column
    """
    return LookupSpec.from_query_graph_node(
        scd_lookup_without_current_flag_node,
        graph=global_graph,
        source_info=source_info,
        is_online_serving=is_online_serving,
    )


@pytest.fixture
def scd_lookup_specs_with_offset(
    global_graph, scd_offset_lookup_node, is_online_serving, source_info
):
    """
    Fixture for a list of LookupSpec derived from SCD lookup with offset
    """
    return LookupSpec.from_query_graph_node(
        scd_offset_lookup_node,
        graph=global_graph,
        source_info=source_info,
        is_online_serving=is_online_serving,
    )


@pytest.fixture
def offline_lookup_aggregator(source_info):
    """
    Fixture for a LookupAggregator for serving offline features
    """
    return LookupAggregator(source_info=source_info, is_online_serving=False)


@pytest.fixture
def online_lookup_aggregator(source_info):
    """
    Fixture for a LookupAggregator for serving online features
    """
    return LookupAggregator(source_info=source_info, is_online_serving=True)


def update_aggregator(aggregator, specs):
    """
    Helper function to update Aggregator using a list of specs
    """
    for spec in specs:
        aggregator.update(spec)


def test_lookup_aggregator__offline_dimension_only(
    offline_lookup_aggregator, dimension_lookup_specs, lookup_node, entity_id
):
    """
    Test lookup aggregator with only dimension lookup
    """
    aggregator = offline_lookup_aggregator
    update_aggregator(aggregator, dimension_lookup_specs)

    direct_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=False))
    assert len(direct_lookup_specs) == 1
    specs = [asdict(spec) for spec in direct_lookup_specs[0]]
    for spec in specs:
        spec.pop("aggregation_source")
    assert specs == [
        {
            "node_name": lookup_node.name,
            "serving_names": ["CUSTOMER_ID"],
            "serving_names_mapping": None,
            "input_column_name": "cust_value_1",
            "feature_name": "CUSTOMER ATTRIBUTE 1",
            "entity_column": "cust_id",
            "entity_ids": [entity_id],
            "scd_parameters": None,
            "event_parameters": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
        },
        {
            "node_name": lookup_node.name,
            "serving_names": ["CUSTOMER_ID"],
            "serving_names_mapping": None,
            "input_column_name": "cust_value_2",
            "feature_name": "CUSTOMER ATTRIBUTE 2",
            "entity_column": "cust_id",
            "entity_ids": [entity_id],
            "scd_parameters": None,
            "event_parameters": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
        },
    ]

    scd_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=True))
    assert len(scd_lookup_specs) == 0


@pytest.mark.parametrize("is_online_serving", [False])
def test_lookup_aggregator__offline_scd_only(
    offline_lookup_aggregator, scd_lookup_specs_with_current_flag, entity_id, scd_lookup_node
):
    """
    Test lookup aggregator with only scd lookups
    """
    aggregator = offline_lookup_aggregator
    update_aggregator(aggregator, scd_lookup_specs_with_current_flag)

    direct_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=False))
    assert len(direct_lookup_specs) == 0

    scd_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=True))
    assert len(scd_lookup_specs) == 1
    specs = [asdict(spec) for spec in scd_lookup_specs[0]]
    for spec in specs:
        spec.pop("aggregation_source")
    assert specs == [
        {
            "node_name": scd_lookup_node.name,
            "serving_names": ["CUSTOMER_ID"],
            "entity_ids": [entity_id],
            "serving_names_mapping": None,
            "input_column_name": "membership_status",
            "feature_name": "Current Membership Status",
            "entity_column": "cust_id",
            "scd_parameters": SCDLookupParameters(
                effective_timestamp_column="event_timestamp",
                natural_key_column="cust_id",
                current_flag_column="is_record_current",
                end_timestamp_column=None,
                offset=None,
            ),
            "event_parameters": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
        }
    ]


@pytest.mark.parametrize("is_online_serving", [True])
def test_lookup_aggregator__online_with_current_flag(
    online_lookup_aggregator,
    scd_lookup_specs_with_current_flag,
    entity_id,
    scd_lookup_node,
):
    """
    Test lookup aggregator with only scd lookups
    """
    aggregator = online_lookup_aggregator
    update_aggregator(aggregator, scd_lookup_specs_with_current_flag)

    # With current flag column, scd lookup is simplified as direct lookup
    direct_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=False))
    assert len(direct_lookup_specs) == 1
    specs = [asdict(spec) for spec in direct_lookup_specs[0]]
    for spec in specs:
        spec.pop("aggregation_source")
    assert specs == [
        {
            "node_name": scd_lookup_node.name,
            "serving_names": ["CUSTOMER_ID"],
            "entity_ids": [entity_id],
            "serving_names_mapping": None,
            "input_column_name": "membership_status",
            "feature_name": "Current Membership Status",
            "entity_column": "cust_id",
            "scd_parameters": SCDLookupParameters(
                effective_timestamp_column="event_timestamp",
                natural_key_column="cust_id",
                current_flag_column="is_record_current",
                end_timestamp_column=None,
                offset=None,
            ),
            "event_parameters": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
        }
    ]

    direct_lookups = aggregator.get_direct_lookups()
    assert len(direct_lookups) == 1
    assert direct_lookups[0].column_names == [
        "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_1"
    ]
    assert direct_lookups[0].join_keys == ["CUSTOMER_ID"]
    expected_sql = textwrap.dedent(
        """
        SELECT
          "CUSTOMER_ID",
          ANY_VALUE("_fb_internal_CUSTOMER_ID_lookup_membership_status_input_1") AS "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_1"
        FROM (
          SELECT
            "cust_id" AS "CUSTOMER_ID",
            "membership_status" AS "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_1"
          FROM (
            SELECT
              "effective_ts" AS "effective_ts",
              "cust_id" AS "cust_id",
              "membership_status" AS "membership_status"
            FROM "db"."public"."customer_profile_table"
            WHERE
              "is_record_current" = TRUE
          )
        )
        GROUP BY
          "CUSTOMER_ID"
        """
    ).strip()
    assert direct_lookups[0].expr.sql(pretty=True) == expected_sql

    scd_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=True))
    assert len(scd_lookup_specs) == 0


@pytest.mark.parametrize("is_online_serving", [True])
def test_lookup_aggregator__online_without_current_flag(
    online_lookup_aggregator,
    scd_lookup_specs_without_current_flag,
    entity_id,
    scd_lookup_without_current_flag_node,
):
    """
    Test lookup aggregator with only scd lookups without a current flag column
    """
    aggregator = online_lookup_aggregator
    update_aggregator(aggregator, scd_lookup_specs_without_current_flag)

    direct_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=False))
    assert len(direct_lookup_specs) == 0

    # If no current flag, SCD join has to be performed even during online serving
    scd_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=True))
    assert len(scd_lookup_specs) == 1
    specs = [asdict(spec) for spec in scd_lookup_specs[0]]
    for spec in specs:
        spec.pop("aggregation_source")
    assert specs == [
        {
            "node_name": scd_lookup_without_current_flag_node.name,
            "serving_names": ["CUSTOMER_ID"],
            "entity_ids": [entity_id],
            "serving_names_mapping": None,
            "input_column_name": "membership_status",
            "feature_name": "Current Membership Status",
            "entity_column": "cust_id",
            "scd_parameters": SCDLookupParameters(
                effective_timestamp_column="event_timestamp",
                natural_key_column="cust_id",
                current_flag_column=None,
                end_timestamp_column=None,
                offset=None,
            ),
            "event_parameters": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
        }
    ]


@pytest.mark.parametrize("is_online_serving", [True])
def test_lookup_aggregator__online_with_offset(
    online_lookup_aggregator,
    scd_lookup_specs_with_offset,
    entity_id,
    scd_offset_lookup_node,
):
    """
    Test lookup aggregator with only scd lookups with offset
    """
    aggregator = online_lookup_aggregator
    update_aggregator(aggregator, scd_lookup_specs_with_offset)

    direct_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=False))
    assert len(direct_lookup_specs) == 0

    # If offset is specified, current flag column cannot be used to simplify online serving
    scd_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=True))
    assert len(scd_lookup_specs) == 1
    specs = [asdict(spec) for spec in scd_lookup_specs[0]]
    for spec in specs:
        spec.pop("aggregation_source")
    assert specs == [
        {
            "node_name": scd_offset_lookup_node.name,
            "serving_names": ["CUSTOMER_ID"],
            "entity_ids": [entity_id],
            "serving_names_mapping": None,
            "input_column_name": "membership_status",
            "feature_name": "Current Membership Status",
            "entity_column": "cust_id",
            "scd_parameters": SCDLookupParameters(
                effective_timestamp_column="event_timestamp",
                natural_key_column="cust_id",
                current_flag_column="is_record_current",
                end_timestamp_column=None,
                offset="14d",
            ),
            "event_parameters": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
        }
    ]


def test_lookup_aggregator__event_table(
    offline_lookup_aggregator, event_lookup_specs, entity_id, event_lookup_node
):
    """
    Test lookup features from EventTable
    """
    aggregator = offline_lookup_aggregator
    update_aggregator(aggregator, event_lookup_specs)

    direct_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=False))
    assert len(direct_lookup_specs) == 1
    specs = [asdict(spec) for spec in direct_lookup_specs[0]]
    for spec in specs:
        spec.pop("aggregation_source")
    assert specs == [
        {
            "node_name": event_lookup_node.name,
            "entity_ids": [entity_id],
            "serving_names": ["ORDER_ID"],
            "serving_names_mapping": None,
            "input_column_name": "order_method",
            "feature_name": "Order Method",
            "entity_column": "order_id",
            "scd_parameters": None,
            "event_parameters": EventLookupParameters(event_timestamp_column="ts"),
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
        }
    ]

    scd_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=True))
    assert len(scd_lookup_specs) == 0
