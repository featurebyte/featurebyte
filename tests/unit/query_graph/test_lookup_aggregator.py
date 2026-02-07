"""
Unit tests for featurebyte.query_graph.sql.aggregator.lookup.LookupAggregator
"""

from __future__ import annotations

import copy
import textwrap
from dataclasses import asdict

import pytest

from featurebyte import CalendarWindow, TimeInterval
from featurebyte.query_graph.node.generic import EventLookupParameters, SCDLookupParameters
from featurebyte.query_graph.sql.aggregator.lookup import LookupAggregator
from featurebyte.query_graph.sql.specifications.lookup import LookupSpec
from featurebyte.query_graph.sql.specifications.lookup_target import LookupTargetSpec
from tests.unit.query_graph.util import get_combined_aggregation_expr_from_aggregator
from tests.util.helper import assert_equal_with_expected_fixture


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
def daily_snapshots_table_agg_spec(global_graph, snapshots_lookup_feature_node, source_info):
    """
    Fixture for daily snapshots table aggregation specification
    """
    parent_nodes = global_graph.get_input_node_names(snapshots_lookup_feature_node)
    assert len(parent_nodes) == 1
    agg_node = global_graph.get_node_by_name(parent_nodes[0])
    return LookupSpec.from_query_graph_node(
        node=agg_node,
        graph=global_graph,
        source_info=source_info,
        agg_result_name_include_serving_names=True,
    )[0]


@pytest.fixture
def daily_snapshots_table_target_agg_spec(global_graph, snapshots_lookup_target_node, source_info):
    """
    Fixture for daily snapshots table aggregation specification
    """
    parent_nodes = global_graph.get_input_node_names(snapshots_lookup_target_node)
    assert len(parent_nodes) == 1
    agg_node = global_graph.get_node_by_name(parent_nodes[0])
    return LookupTargetSpec.from_query_graph_node(
        node=agg_node,
        graph=global_graph,
        source_info=source_info,
        agg_result_name_include_serving_names=True,
    )[0]


@pytest.fixture
def daily_snapshots_table_agg_spec_blind_spot(daily_snapshots_table_agg_spec):
    """
    Fixture for daily snapshots table aggregation specification with blind spot
    """
    agg_spec = copy.deepcopy(daily_snapshots_table_agg_spec)
    agg_spec.snapshots_parameters.feature_job_setting.blind_spot = CalendarWindow(
        unit="DAY",
        size=3,
    )
    return agg_spec


@pytest.fixture
def monthly_snapshots_table_agg_spec(daily_snapshots_table_agg_spec):
    """
    Fixture for monthly snapshots table aggregation specification
    """
    agg_spec = copy.deepcopy(daily_snapshots_table_agg_spec)
    agg_spec.snapshots_parameters.time_interval = TimeInterval(unit="MONTH", value=1)
    return agg_spec


@pytest.fixture
def monthly_snapshots_table_agg_spec_blind_spot(daily_snapshots_table_agg_spec):
    """
    Fixture for monthly snapshots table aggregation specification with blind spot
    """
    agg_spec = copy.deepcopy(daily_snapshots_table_agg_spec)
    agg_spec.snapshots_parameters.feature_job_setting.blind_spot = CalendarWindow(
        unit="MONTH",
        size=3,
    )
    return agg_spec


@pytest.fixture
def daily_snapshots_table_agg_spec_with_offset(daily_snapshots_table_agg_spec):
    """
    Fixture for daily snapshots table aggregation specification with offset
    """
    agg_spec = copy.deepcopy(daily_snapshots_table_agg_spec)
    agg_spec.snapshots_parameters.offset_size = 2
    return agg_spec


@pytest.fixture
def monthly_snapshots_table_agg_spec_with_offset(daily_snapshots_table_agg_spec):
    """
    Fixture for monthly snapshots table aggregation specification with offset
    """
    agg_spec = copy.deepcopy(daily_snapshots_table_agg_spec)
    agg_spec.snapshots_parameters.time_interval = TimeInterval(unit="MONTH", value=1)
    agg_spec.snapshots_parameters.offset_size = 1
    return agg_spec


@pytest.fixture
def daily_snapshots_table_target_agg_spec_with_offset(daily_snapshots_table_target_agg_spec):
    """
    Fixture for
    """
    agg_spec = copy.deepcopy(daily_snapshots_table_target_agg_spec)
    agg_spec.snapshots_parameters.offset_size = 3
    return agg_spec


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
            "snapshots_parameters": None,
            "use_forecast_point": False,
            "forecast_point_schema": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
            "is_deployment_sql": False,
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
            "snapshots_parameters": None,
            "use_forecast_point": False,
            "forecast_point_schema": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
            "is_deployment_sql": False,
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
            "snapshots_parameters": None,
            "use_forecast_point": False,
            "forecast_point_schema": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
            "is_deployment_sql": False,
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
            "snapshots_parameters": None,
            "use_forecast_point": False,
            "forecast_point_schema": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
            "is_deployment_sql": False,
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
            "snapshots_parameters": None,
            "use_forecast_point": False,
            "forecast_point_schema": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
            "is_deployment_sql": False,
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
            "snapshots_parameters": None,
            "use_forecast_point": False,
            "forecast_point_schema": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
            "is_deployment_sql": False,
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
            "snapshots_parameters": None,
            "use_forecast_point": False,
            "forecast_point_schema": None,
            "is_parent_lookup": False,
            "agg_result_name_include_serving_names": True,
            "is_deployment_sql": False,
        }
    ]

    scd_lookup_specs = list(aggregator.iterate_grouped_lookup_specs(is_scd=True))
    assert len(scd_lookup_specs) == 0


@pytest.mark.parametrize(
    "test_case_name",
    [
        "snapshots_daily",
        "snapshots_daily_with_blind_spot",
        "snapshots_daily_with_offset",
        "snapshots_daily_target_with_offset",
        "snapshots_monthly",
        "snapshots_monthly_with_blind_spot",
        "snapshots_monthly_with_offset",
    ],
)
def test_snapshots_table_lookup(request, test_case_name, update_fixtures, source_info):
    """
    Test lookup aggregator for snapshots table
    """
    test_case_mapping = {
        "snapshots_daily": "daily_snapshots_table_agg_spec",
        "snapshots_daily_with_blind_spot": "daily_snapshots_table_agg_spec_blind_spot",
        "snapshots_daily_with_offset": "daily_snapshots_table_agg_spec_with_offset",
        "snapshots_daily_target_with_offset": "daily_snapshots_table_target_agg_spec_with_offset",
        "snapshots_monthly": "monthly_snapshots_table_agg_spec",
        "snapshots_monthly_with_blind_spot": "monthly_snapshots_table_agg_spec_blind_spot",
        "snapshots_monthly_with_offset": "monthly_snapshots_table_agg_spec_with_offset",
    }
    fixture_name = test_case_mapping[test_case_name]
    fixture_obj = request.getfixturevalue(fixture_name)
    if isinstance(fixture_obj, list):
        agg_specs = fixture_obj
    else:
        agg_specs = [fixture_obj]

    aggregator = LookupAggregator(source_info=source_info)
    for agg_spec in agg_specs:
        aggregator.update(agg_spec)
    result_expr = get_combined_aggregation_expr_from_aggregator(aggregator)
    assert_equal_with_expected_fixture(
        result_expr.sql(pretty=True),
        f"tests/fixtures/aggregator/expected_lookup_aggregator_{test_case_name}.sql",
        update_fixture=update_fixtures,
    )
