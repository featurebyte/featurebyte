from __future__ import annotations

from dataclasses import asdict

import pytest

from featurebyte.enum import SourceType
from featurebyte.query_graph.node.generic import SCDLookupParameters
from featurebyte.query_graph.sql.aggregator.lookup import LookupAggregator
from featurebyte.query_graph.sql.specs import LookupSpec


@pytest.fixture
def dimension_lookup_specs(global_graph, lookup_node):
    """
    Fixture for a list of LookupSpec derived from lookup_node
    """
    return LookupSpec.from_lookup_query_node(
        lookup_node,
        graph=global_graph,
        source_type=SourceType.SNOWFLAKE,
    )


@pytest.fixture
def scd_lookup_specs_with_current_flag(global_graph, scd_lookup_node):
    """
    Fixture for a list of LookupSpec derived from SCD lookup
    """
    return LookupSpec.from_lookup_query_node(
        scd_lookup_node,
        graph=global_graph,
        source_type=SourceType.SNOWFLAKE,
    )


@pytest.fixture
def scd_lookup_specs_without_current_flag(global_graph, scd_lookup_without_current_flag_node):
    """
    Fixture for a list of LookupSpec derived from SCD lookup without current flag column
    """
    return LookupSpec.from_lookup_query_node(
        scd_lookup_without_current_flag_node,
        graph=global_graph,
        source_type=SourceType.SNOWFLAKE,
    )


@pytest.fixture
def offline_lookup_aggregator():
    """
    Fixture for a LookupAggregator for serving offline features
    """
    return LookupAggregator(source_type=SourceType.SNOWFLAKE, is_online_serving=False)


@pytest.fixture
def online_lookup_aggregator():
    """
    Fixture for a LookupAggregator for serving online features
    """
    return LookupAggregator(source_type=SourceType.SNOWFLAKE, is_online_serving=True)


def update_aggregator(aggregator, specs):
    """
    Helper function to update Aggregator using a list of specs
    """
    for spec in specs:
        aggregator.update(spec)


def test_lookup_aggregator__dimension_only(offline_lookup_aggregator, dimension_lookup_specs):
    """
    Test lookup aggregator with only dimension lookup
    """
    aggregator = offline_lookup_aggregator
    update_aggregator(aggregator, dimension_lookup_specs)

    direct_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=False))
    assert len(direct_lookup_specs) == 1
    specs = [asdict(spec) for spec in direct_lookup_specs[0]]
    for spec in specs:
        spec.pop("source_expr")
    assert specs == [
        {
            "serving_names": ["CUSTOMER_ID"],
            "serving_names_mapping": None,
            "input_column_name": "cust_value_1",
            "feature_name": "CUSTOMER ATTRIBUTE 1",
            "entity_column": "cust_id",
            "scd_parameters": None,
        },
        {
            "serving_names": ["CUSTOMER_ID"],
            "serving_names_mapping": None,
            "input_column_name": "cust_value_2",
            "feature_name": "CUSTOMER ATTRIBUTE 2",
            "entity_column": "cust_id",
            "scd_parameters": None,
        },
    ]

    scd_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=True))
    assert len(scd_lookup_specs) == 0


def test_lookup_aggregator__online_with_current_flag(
    online_lookup_aggregator,
    scd_lookup_specs_with_current_flag,
):
    """
    Test lookup aggregator with only scd lookups
    """
    aggregator = online_lookup_aggregator
    update_aggregator(aggregator, scd_lookup_specs_with_current_flag)

    direct_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=False))
    assert len(direct_lookup_specs) == 1
    specs = [asdict(spec) for spec in direct_lookup_specs[0]]
    for spec in specs:
        spec.pop("source_expr")
    assert specs == [
        {
            "serving_names": ["CUSTOMER_ID"],
            "serving_names_mapping": None,
            "input_column_name": "membership_status",
            "feature_name": "Current Membership Status",
            "entity_column": "cust_id",
            "scd_parameters": SCDLookupParameters(
                effective_timestamp_column="event_timestamp",
                current_flag_column="is_record_current",
                end_timestamp_column=None,
                offset=None,
            ),
        }
    ]

    scd_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=True))
    assert len(scd_lookup_specs) == 0


def test_lookup_aggregator__online_without_current_flag(
    online_lookup_aggregator,
    scd_lookup_specs_without_current_flag,
):
    """
    Test lookup aggregator with only scd lookups without a current flag column
    """
    aggregator = online_lookup_aggregator
    update_aggregator(aggregator, scd_lookup_specs_without_current_flag)

    direct_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=False))
    assert len(direct_lookup_specs) == 0

    scd_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=True))
    assert len(scd_lookup_specs) == 1
    specs = [asdict(spec) for spec in scd_lookup_specs[0]]
    for spec in specs:
        spec.pop("source_expr")
    assert specs == [
        {
            "serving_names": ["CUSTOMER_ID"],
            "serving_names_mapping": None,
            "input_column_name": "membership_status",
            "feature_name": "Current Membership Status",
            "entity_column": "cust_id",
            "scd_parameters": SCDLookupParameters(
                effective_timestamp_column="event_timestamp",
                current_flag_column=None,
                end_timestamp_column=None,
                offset=None,
            ),
        }
    ]
