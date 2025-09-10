"""
Unit tests for featurebyte.query_graph.sql.aggregator.asat.AsAtAggregator
"""

import textwrap

import pytest
from sqlglot.expressions import select

from featurebyte import TimeInterval
from featurebyte.enum import DBVarType
from featurebyte.query_graph.node.generic import AggregateAsAtParameters, SnapshotsLookupParameters
from featurebyte.query_graph.sql.aggregator.asat import AsAtAggregator
from featurebyte.query_graph.sql.aggregator.forward_asat import ForwardAsAtAggregator
from featurebyte.query_graph.sql.specifications.aggregate_asat import AggregateAsAtSpec
from featurebyte.query_graph.sql.specifications.forward_aggregate_asat import (
    ForwardAggregateAsAtSpec,
)
from featurebyte.query_graph.sql.specs import AggregationSource
from tests.unit.query_graph.util import get_combined_aggregation_expr_from_aggregator
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture
def aggregate_as_at_node_parameters(entity_id):
    return AggregateAsAtParameters(
        keys=["cust_id"],
        serving_names=["serving_cust_id"],
        parent="value",
        agg_func="sum",
        natural_key_column="scd_key",
        effective_timestamp_column="effective_ts",
        name="asat_feature",
        entity_ids=[entity_id],
    )


@pytest.fixture
def aggregate_as_at_node_parameters_with_end_timestamp(aggregate_as_at_node_parameters):
    params = aggregate_as_at_node_parameters.model_copy()
    params.end_timestamp_column = "end_ts"
    return params


@pytest.fixture
def scd_table_sql():
    return select("*").from_("SCD_TABLE")


@pytest.fixture
def scd_aggregation_source(scd_table_sql):
    return AggregationSource(expr=scd_table_sql, query_node_name="input_1")


@pytest.fixture
def aggregation_spec_without_end_timestamp(
    aggregate_as_at_node_parameters, scd_aggregation_source, entity_id
):
    return AggregateAsAtSpec(
        node_name="aggregate_as_at_1",
        feature_name=aggregate_as_at_node_parameters.name,
        serving_names=["serving_cust_id"],
        serving_names_mapping=None,
        parameters=aggregate_as_at_node_parameters,
        aggregation_source=scd_aggregation_source,
        entity_ids=[entity_id],
        parent_dtype=DBVarType.FLOAT,
        agg_result_name_include_serving_names=True,
    )


@pytest.fixture
def aggregation_spec_with_end_timestamp(
    aggregate_as_at_node_parameters_with_end_timestamp, scd_aggregation_source, entity_id
):
    return AggregateAsAtSpec(
        node_name="aggregate_as_at_2",
        feature_name=aggregate_as_at_node_parameters_with_end_timestamp.name,
        serving_names=["serving_cust_id"],
        serving_names_mapping=None,
        parameters=aggregate_as_at_node_parameters_with_end_timestamp,
        aggregation_source=scd_aggregation_source,
        entity_ids=[entity_id],
        parent_dtype=DBVarType.FLOAT,
        agg_result_name_include_serving_names=True,
    )


@pytest.fixture
def aggregation_spec_with_serving_names_mapping(
    aggregate_as_at_node_parameters_with_end_timestamp, scd_aggregation_source, entity_id
):
    return AggregateAsAtSpec(
        node_name="aggregate_as_at_3",
        feature_name=aggregate_as_at_node_parameters_with_end_timestamp.name,
        serving_names=["serving_cust_id"],
        serving_names_mapping={"serving_cust_id": "new_serving_cust_id"},
        parameters=aggregate_as_at_node_parameters_with_end_timestamp,
        aggregation_source=scd_aggregation_source,
        entity_ids=[entity_id],
        parent_dtype=DBVarType.FLOAT,
        agg_result_name_include_serving_names=True,
    )


@pytest.fixture
def aggregation_specs_same_source_different_agg_funcs(
    aggregate_as_at_node_parameters_with_end_timestamp,
    scd_aggregation_source,
    entity_id,
):
    params = aggregate_as_at_node_parameters_with_end_timestamp

    params_1 = params.model_copy()
    params_1.agg_func = "min"

    params_2 = params.model_copy()
    params_2.agg_func = "max"

    specs = []
    for i, params in enumerate([params_1, params_2]):
        specs.append(
            AggregateAsAtSpec(
                node_name=f"aggregate_as_at_{i}",
                feature_name=params.name,
                serving_names=["serving_cust_id"],
                serving_names_mapping=None,
                parameters=params,
                aggregation_source=scd_aggregation_source,
                entity_ids=[entity_id],
                parent_dtype=DBVarType.FLOAT,
                agg_result_name_include_serving_names=True,
            )
        )

    return specs


@pytest.fixture
def aggregation_specs_same_source_different_keys(
    aggregate_as_at_node_parameters_with_end_timestamp,
    scd_aggregation_source,
    entity_id,
):
    params = aggregate_as_at_node_parameters_with_end_timestamp

    params_1 = params.model_copy()
    params_1.__dict__.update({"keys": ["key_1"], "serving_names": ["serving_key_1"]})

    params_2 = params.model_copy()
    params_2.__dict__.update({"keys": ["key_2"], "serving_names": ["serving_key_2"]})

    specs = []
    for i, params in enumerate([params_1, params_2]):
        specs.append(
            AggregateAsAtSpec(
                node_name=f"aggregate_as_at_{i}",
                feature_name=params.name,
                serving_names=params.serving_names,
                serving_names_mapping=None,
                parameters=params,
                aggregation_source=scd_aggregation_source,
                entity_ids=[entity_id],
                parent_dtype=DBVarType.FLOAT,
                agg_result_name_include_serving_names=True,
            )
        )

    return specs


@pytest.fixture
def aggregation_spec_with_category(
    aggregate_as_at_node_parameters_with_end_timestamp, scd_aggregation_source, entity_id
):
    parameters = aggregate_as_at_node_parameters_with_end_timestamp.model_copy()
    parameters.value_by = "category_col"
    return AggregateAsAtSpec(
        node_name="aggregate_as_at_1",
        feature_name=parameters.name,
        serving_names=["serving_cust_id"],
        serving_names_mapping=None,
        parameters=parameters,
        aggregation_source=scd_aggregation_source,
        entity_ids=[entity_id],
        parent_dtype=DBVarType.FLOAT,
        agg_result_name_include_serving_names=True,
    )


@pytest.fixture
def aggregation_spec_with_offset(
    aggregate_as_at_node_parameters, scd_aggregation_source, entity_id
):
    parameters = aggregate_as_at_node_parameters.model_copy()
    parameters.offset = "7d"
    return AggregateAsAtSpec(
        node_name="aggregate_as_at_4",
        feature_name=aggregate_as_at_node_parameters.name,
        serving_names=["serving_cust_id"],
        serving_names_mapping=None,
        parameters=parameters,
        aggregation_source=scd_aggregation_source,
        entity_ids=[entity_id],
        parent_dtype=DBVarType.FLOAT,
        agg_result_name_include_serving_names=True,
    )


@pytest.fixture
def forward_aggregation_spec_with_offset(
    aggregate_as_at_node_parameters, scd_aggregation_source, entity_id
):
    parameters = aggregate_as_at_node_parameters.model_copy()
    parameters.offset = "7d"
    return ForwardAggregateAsAtSpec(
        node_name="forward_aggregate_as_at_1",
        feature_name=aggregate_as_at_node_parameters.name,
        serving_names=["serving_cust_id"],
        serving_names_mapping=None,
        parameters=parameters,
        aggregation_source=scd_aggregation_source,
        entity_ids=[entity_id],
        parent_dtype=DBVarType.FLOAT,
        agg_result_name_include_serving_names=True,
    )


@pytest.fixture
def aggregation_spec_with_snapshots_parameters(entity_id):
    node_parameters = AggregateAsAtParameters(
        keys=["cust_id"],
        serving_names=["serving_cust_id"],
        parent="value",
        agg_func="sum",
        effective_timestamp_column="snapshot_datetime_column",
        name="asat_feature",
        entity_ids=[entity_id],
        snapshots_parameters=SnapshotsLookupParameters(
            snapshot_datetime_column="snapshot_datetime_column",
            time_interval=TimeInterval(unit="DAY", value=1),
        ),
    )
    source_expr = select("*").from_("SNAPSHOTS_TABLE")
    return AggregateAsAtSpec(
        node_name="aggregate_as_at_1",
        feature_name=node_parameters.name,
        serving_names=["serving_cust_id"],
        serving_names_mapping=None,
        parameters=node_parameters,
        aggregation_source=AggregationSource(expr=source_expr, query_node_name="input_1"),
        entity_ids=[entity_id],
        parent_dtype=DBVarType.FLOAT,
        agg_result_name_include_serving_names=True,
    )


@pytest.fixture
def aggregation_spec_with_snapshots_parameters_and_offset(
    aggregation_spec_with_snapshots_parameters,
):
    """
    AggregateAsAtSpec for snapshots table with offset
    """
    agg_spec = aggregation_spec_with_snapshots_parameters
    agg_spec.parameters.snapshots_parameters.offset_size = 3
    return agg_spec


def test_asat_aggregate_scd_table_without_end_timestamp(
    aggregation_spec_without_end_timestamp, source_info
):
    """
    Test AsAtAggregator on SCD table without end timestamp
    """
    aggregator = AsAtAggregator(source_info=source_info)
    aggregator.update(aggregation_spec_without_end_timestamp)

    result = aggregator.update_aggregation_table_expr(
        select("a", "b", "c").from_("REQUEST_TABLE"), "POINT_INT_TIME", ["a", "b", "c"], 0
    )

    expected = textwrap.dedent(
        """
        SELECT
          a,
          b,
          c,
          "T0"."_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_input_1" AS "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_input_1"
        FROM REQUEST_TABLE
        LEFT JOIN (
          SELECT
            REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
            REQ."serving_cust_id" AS "serving_cust_id",
            SUM(SCD."value") AS "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_input_1"
          FROM "REQUEST_TABLE_POINT_IN_TIME_serving_cust_id" AS REQ
          INNER JOIN (
            SELECT
              *,
              LEAD("effective_ts") OVER (PARTITION BY "scd_key" ORDER BY "effective_ts" NULLS LAST) AS "__FB_END_TS"
            FROM (
              SELECT
                *
              FROM SCD_TABLE
            )
          ) AS SCD
            ON REQ."serving_cust_id" = SCD."cust_id"
            AND (
              SCD."effective_ts" <= REQ."POINT_IN_TIME"
              AND (
                SCD."__FB_END_TS" > REQ."POINT_IN_TIME" OR SCD."__FB_END_TS" IS NULL
              )
            )
          GROUP BY
            REQ."POINT_IN_TIME",
            REQ."serving_cust_id"
        ) AS T0
          ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
          AND REQ."serving_cust_id" = T0."serving_cust_id"
        """
    ).strip()
    assert result.updated_table_expr.sql(pretty=True) == expected

    request_table_ctes = aggregator.get_common_table_expressions("REQUEST_TABLE")
    assert len(request_table_ctes) == 1
    expected = textwrap.dedent(
        """
        SELECT DISTINCT
          "POINT_IN_TIME",
          "serving_cust_id"
        FROM REQUEST_TABLE
        """
    ).strip()
    assert request_table_ctes[0].name == "REQUEST_TABLE_POINT_IN_TIME_serving_cust_id"
    assert request_table_ctes[0].expr.sql(pretty=True) == expected


def test_asat_aggregate_scd_table_with_end_timestamp(
    aggregation_spec_with_end_timestamp, source_info
):
    """
    Test AsAtAggregator on SCD table with end timestamp (query is simpler because no need to compute
    the end timestamp for the SCD table)
    """
    aggregator = AsAtAggregator(source_info=source_info)
    aggregator.update(aggregation_spec_with_end_timestamp)

    result = aggregator.update_aggregation_table_expr(
        select("a", "b", "c").from_("REQUEST_TABLE"), "POINT_INT_TIME", ["a", "b", "c"], 0
    )

    expected = textwrap.dedent(
        """
        SELECT
          a,
          b,
          c,
          "T0"."_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_input_1" AS "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_input_1"
        FROM REQUEST_TABLE
        LEFT JOIN (
          SELECT
            REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
            REQ."serving_cust_id" AS "serving_cust_id",
            SUM(SCD."value") AS "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_input_1"
          FROM "REQUEST_TABLE_POINT_IN_TIME_serving_cust_id" AS REQ
          INNER JOIN (
            SELECT
              *
            FROM SCD_TABLE
          ) AS SCD
            ON REQ."serving_cust_id" = SCD."cust_id"
            AND (
              SCD."effective_ts" <= REQ."POINT_IN_TIME"
              AND (
                SCD."end_ts" > REQ."POINT_IN_TIME" OR SCD."end_ts" IS NULL
              )
            )
          GROUP BY
            REQ."POINT_IN_TIME",
            REQ."serving_cust_id"
        ) AS T0
          ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
          AND REQ."serving_cust_id" = T0."serving_cust_id"
        """
    ).strip()
    assert result.updated_table_expr.sql(pretty=True) == expected


def test_asat_aggregate_scd_table_with_serving_names_mapping(
    aggregation_spec_with_serving_names_mapping, source_info
):
    """
    Test AsAtAggregator handles serving names mapping properly (new_serving_cust_id is referenced
    instead of the original serving_cust_id)
    """
    aggregator = AsAtAggregator(source_info=source_info)
    aggregator.update(aggregation_spec_with_serving_names_mapping)

    result = aggregator.update_aggregation_table_expr(
        select("a", "b", "c").from_("REQUEST_TABLE"), "POINT_INT_TIME", ["a", "b", "c"], 0
    )

    expected = textwrap.dedent(
        """
        SELECT
          a,
          b,
          c,
          "T0"."_fb_internal_new_serving_cust_id_as_at_sum_value_cust_id_None_input_1" AS "_fb_internal_new_serving_cust_id_as_at_sum_value_cust_id_None_input_1"
        FROM REQUEST_TABLE
        LEFT JOIN (
          SELECT
            REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
            REQ."new_serving_cust_id" AS "new_serving_cust_id",
            SUM(SCD."value") AS "_fb_internal_new_serving_cust_id_as_at_sum_value_cust_id_None_input_1"
          FROM "REQUEST_TABLE_POINT_IN_TIME_new_serving_cust_id" AS REQ
          INNER JOIN (
            SELECT
              *
            FROM SCD_TABLE
          ) AS SCD
            ON REQ."new_serving_cust_id" = SCD."cust_id"
            AND (
              SCD."effective_ts" <= REQ."POINT_IN_TIME"
              AND (
                SCD."end_ts" > REQ."POINT_IN_TIME" OR SCD."end_ts" IS NULL
              )
            )
          GROUP BY
            REQ."POINT_IN_TIME",
            REQ."new_serving_cust_id"
        ) AS T0
          ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
          AND REQ."new_serving_cust_id" = T0."new_serving_cust_id"
        """
    ).strip()
    assert result.updated_table_expr.sql(pretty=True) == expected


def test_same_source_different_agg_funcs(
    aggregation_specs_same_source_different_agg_funcs, source_info
):
    """
    Test that asat_aggregation using the same source and join keys but different agg_func can be
    aggregated in the same groubpy statement
    """
    aggregator = AsAtAggregator(source_info=source_info)
    for spec in aggregation_specs_same_source_different_agg_funcs:
        aggregator.update(spec)

    assert len(aggregator.grouped_specs) == 1

    result = aggregator.update_aggregation_table_expr(
        select("a", "b", "c").from_("REQUEST_TABLE"), "POINT_INT_TIME", ["a", "b", "c"], 0
    )
    expected = textwrap.dedent(
        """
        SELECT
          a,
          b,
          c,
          "T0"."_fb_internal_serving_cust_id_as_at_min_value_cust_id_None_input_1" AS "_fb_internal_serving_cust_id_as_at_min_value_cust_id_None_input_1",
          "T0"."_fb_internal_serving_cust_id_as_at_max_value_cust_id_None_input_1" AS "_fb_internal_serving_cust_id_as_at_max_value_cust_id_None_input_1"
        FROM REQUEST_TABLE
        LEFT JOIN (
          SELECT
            REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
            REQ."serving_cust_id" AS "serving_cust_id",
            MIN(SCD."value") AS "_fb_internal_serving_cust_id_as_at_min_value_cust_id_None_input_1",
            MAX(SCD."value") AS "_fb_internal_serving_cust_id_as_at_max_value_cust_id_None_input_1"
          FROM "REQUEST_TABLE_POINT_IN_TIME_serving_cust_id" AS REQ
          INNER JOIN (
            SELECT
              *
            FROM SCD_TABLE
          ) AS SCD
            ON REQ."serving_cust_id" = SCD."cust_id"
            AND (
              SCD."effective_ts" <= REQ."POINT_IN_TIME"
              AND (
                SCD."end_ts" > REQ."POINT_IN_TIME" OR SCD."end_ts" IS NULL
              )
            )
          GROUP BY
            REQ."POINT_IN_TIME",
            REQ."serving_cust_id"
        ) AS T0
          ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
          AND REQ."serving_cust_id" = T0."serving_cust_id"
        """
    ).strip()
    assert result.updated_table_expr.sql(pretty=True) == expected


def test_same_source_different_keys(aggregation_specs_same_source_different_keys, source_info):
    """
    Test that asat_aggregation using the same source and join keys but different keys will have to
    be aggregated using multiple groupby operations
    """
    aggregator = AsAtAggregator(source_info=source_info)
    for spec in aggregation_specs_same_source_different_keys:
        aggregator.update(spec)

    assert len(aggregator.grouped_specs) == 2

    result = aggregator.update_aggregation_table_expr(
        select("a", "b", "c").from_("REQUEST_TABLE"), "POINT_INT_TIME", ["a", "b", "c"], 0
    )
    expected = textwrap.dedent(
        """
        SELECT
          a,
          b,
          c,
          "T0"."_fb_internal_serving_key_1_as_at_sum_value_key_1_None_input_1" AS "_fb_internal_serving_key_1_as_at_sum_value_key_1_None_input_1",
          "T1"."_fb_internal_serving_key_2_as_at_sum_value_key_2_None_input_1" AS "_fb_internal_serving_key_2_as_at_sum_value_key_2_None_input_1"
        FROM REQUEST_TABLE
        LEFT JOIN (
          SELECT
            REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
            REQ."serving_key_1" AS "serving_key_1",
            SUM(SCD."value") AS "_fb_internal_serving_key_1_as_at_sum_value_key_1_None_input_1"
          FROM "REQUEST_TABLE_POINT_IN_TIME_serving_key_1" AS REQ
          INNER JOIN (
            SELECT
              *
            FROM SCD_TABLE
          ) AS SCD
            ON REQ."serving_key_1" = SCD."key_1"
            AND (
              SCD."effective_ts" <= REQ."POINT_IN_TIME"
              AND (
                SCD."end_ts" > REQ."POINT_IN_TIME" OR SCD."end_ts" IS NULL
              )
            )
          GROUP BY
            REQ."POINT_IN_TIME",
            REQ."serving_key_1"
        ) AS T0
          ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
          AND REQ."serving_key_1" = T0."serving_key_1"
        LEFT JOIN (
          SELECT
            REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
            REQ."serving_key_2" AS "serving_key_2",
            SUM(SCD."value") AS "_fb_internal_serving_key_2_as_at_sum_value_key_2_None_input_1"
          FROM "REQUEST_TABLE_POINT_IN_TIME_serving_key_2" AS REQ
          INNER JOIN (
            SELECT
              *
            FROM SCD_TABLE
          ) AS SCD
            ON REQ."serving_key_2" = SCD."key_2"
            AND (
              SCD."effective_ts" <= REQ."POINT_IN_TIME"
              AND (
                SCD."end_ts" > REQ."POINT_IN_TIME" OR SCD."end_ts" IS NULL
              )
            )
          GROUP BY
            REQ."POINT_IN_TIME",
            REQ."serving_key_2"
        ) AS T1
          ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME"
          AND REQ."serving_key_2" = T1."serving_key_2"
        """
    ).strip()
    assert result.updated_table_expr.sql(pretty=True) == expected


def test_asat_aggregate_with_category(aggregation_spec_with_category, source_info):
    """
    Test AsAtAggregator with category parameter
    """
    aggregator = AsAtAggregator(source_info=source_info)
    aggregator.update(aggregation_spec_with_category)

    result = aggregator.update_aggregation_table_expr(
        select("a", "b", "c").from_("REQUEST_TABLE"), "POINT_INT_TIME", ["a", "b", "c"], 0
    )

    expected = textwrap.dedent(
        """
        SELECT
          a,
          b,
          c,
          "T0"."_fb_internal_serving_cust_id_as_at_sum_value_cust_id_category_col_input_1" AS "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_category_col_input_1"
        FROM REQUEST_TABLE
        LEFT JOIN (
          SELECT
            INNER_."POINT_IN_TIME",
            INNER_."serving_cust_id",
            OBJECT_AGG(
              CASE
                WHEN INNER_."category_col" IS NULL
                THEN '__MISSING__'
                ELSE CAST(INNER_."category_col" AS TEXT)
              END,
              TO_VARIANT(
                INNER_."_fb_internal_serving_cust_id_as_at_sum_value_cust_id_category_col_input_1_inner"
              )
            ) AS "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_category_col_input_1"
          FROM (
            SELECT
              "POINT_IN_TIME",
              "serving_cust_id",
              "category_col",
              "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_category_col_input_1_inner"
            FROM (
              SELECT
                "POINT_IN_TIME",
                "serving_cust_id",
                "category_col",
                "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_category_col_input_1_inner",
                ROW_NUMBER() OVER (PARTITION BY "POINT_IN_TIME", "serving_cust_id" ORDER BY "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_category_col_input_1_inner" DESC) AS "__fb_object_agg_row_number"
              FROM (
                SELECT
                  REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
                  REQ."serving_cust_id" AS "serving_cust_id",
                  SCD."category_col" AS "category_col",
                  SUM(SCD."value") AS "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_category_col_input_1_inner"
                FROM "REQUEST_TABLE_POINT_IN_TIME_serving_cust_id" AS REQ
                INNER JOIN (
                  SELECT
                    *
                  FROM SCD_TABLE
                ) AS SCD
                  ON REQ."serving_cust_id" = SCD."cust_id"
                  AND (
                    SCD."effective_ts" <= REQ."POINT_IN_TIME"
                    AND (
                      SCD."end_ts" > REQ."POINT_IN_TIME" OR SCD."end_ts" IS NULL
                    )
                  )
                GROUP BY
                  REQ."POINT_IN_TIME",
                  REQ."serving_cust_id",
                  SCD."category_col"
              )
            )
            WHERE
              "__fb_object_agg_row_number" <= 500
          ) AS INNER_
          GROUP BY
            INNER_."POINT_IN_TIME",
            INNER_."serving_cust_id"
        ) AS T0
          ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
          AND REQ."serving_cust_id" = T0."serving_cust_id"
        """
    ).strip()
    assert result.updated_table_expr.sql(pretty=True) == expected


def test_aggregate_asat_with_offset(aggregation_spec_with_offset, source_info):
    """
    Test AsAtAggregator with offset parameter
    """

    aggregator = AsAtAggregator(source_info=source_info)
    aggregator.update(aggregation_spec_with_offset)

    result = aggregator.update_aggregation_table_expr(
        select("a", "b", "c").from_("REQUEST_TABLE"), "POINT_INT_TIME", ["a", "b", "c"], 0
    )
    expected = textwrap.dedent(
        """
        SELECT
          a,
          b,
          c,
          "T0"."_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_7d_input_1" AS "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_7d_input_1"
        FROM REQUEST_TABLE
        LEFT JOIN (
          SELECT
            REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
            REQ."serving_cust_id" AS "serving_cust_id",
            SUM(SCD."value") AS "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_7d_input_1"
          FROM "REQUEST_TABLE_POINT_IN_TIME_serving_cust_id" AS REQ
          INNER JOIN (
            SELECT
              *,
              LEAD("effective_ts") OVER (PARTITION BY "scd_key" ORDER BY "effective_ts" NULLS LAST) AS "__FB_END_TS"
            FROM (
              SELECT
                *
              FROM SCD_TABLE
            )
          ) AS SCD
            ON REQ."serving_cust_id" = SCD."cust_id"
            AND (
              SCD."effective_ts" <= DATE_ADD(REQ."POINT_IN_TIME", -604800000000.0, 'MICROSECOND')
              AND (
                SCD."__FB_END_TS" > DATE_ADD(REQ."POINT_IN_TIME", -604800000000.0, 'MICROSECOND')
                OR SCD."__FB_END_TS" IS NULL
              )
            )
          GROUP BY
            REQ."POINT_IN_TIME",
            REQ."serving_cust_id"
        ) AS T0
          ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
          AND REQ."serving_cust_id" = T0."serving_cust_id"
        """
    ).strip()
    assert result.updated_table_expr.sql(pretty=True) == expected


def test_forward_aggregate_asat_with_offset(forward_aggregation_spec_with_offset, source_info):
    """
    Test ForwardAsAtAggregator with offset parameter
    """

    aggregator = ForwardAsAtAggregator(source_info=source_info)
    aggregator.update(forward_aggregation_spec_with_offset)

    result = aggregator.update_aggregation_table_expr(
        select("a", "b", "c").from_("REQUEST_TABLE"), "POINT_INT_TIME", ["a", "b", "c"], 0
    )
    expected = textwrap.dedent(
        """
        SELECT
          a,
          b,
          c,
          "T0"."_fb_internal_serving_cust_id_forward_as_at_sum_value_cust_id_None_7d_input_1" AS "_fb_internal_serving_cust_id_forward_as_at_sum_value_cust_id_None_7d_input_1"
        FROM REQUEST_TABLE
        LEFT JOIN (
          SELECT
            REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
            REQ."serving_cust_id" AS "serving_cust_id",
            SUM(SCD."value") AS "_fb_internal_serving_cust_id_forward_as_at_sum_value_cust_id_None_7d_input_1"
          FROM "REQUEST_TABLE_POINT_IN_TIME_serving_cust_id" AS REQ
          INNER JOIN (
            SELECT
              *,
              LEAD("effective_ts") OVER (PARTITION BY "scd_key" ORDER BY "effective_ts" NULLS LAST) AS "__FB_END_TS"
            FROM (
              SELECT
                *
              FROM SCD_TABLE
            )
          ) AS SCD
            ON REQ."serving_cust_id" = SCD."cust_id"
            AND (
              SCD."effective_ts" <= DATE_ADD(REQ."POINT_IN_TIME", 604800000000.0, 'MICROSECOND')
              AND (
                SCD."__FB_END_TS" > DATE_ADD(REQ."POINT_IN_TIME", 604800000000.0, 'MICROSECOND')
                OR SCD."__FB_END_TS" IS NULL
              )
            )
          GROUP BY
            REQ."POINT_IN_TIME",
            REQ."serving_cust_id"
        ) AS T0
          ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
          AND REQ."serving_cust_id" = T0."serving_cust_id"
        """
    ).strip()
    assert result.updated_table_expr.sql(pretty=True) == expected


@pytest.mark.parametrize(
    "test_case_name",
    [
        "snapshots",
        "snapshots_with_offset",
    ],
)
def test_snapshots_aggregate_asat(request, test_case_name, update_fixtures, source_info):
    """
    Test asat aggregator for snapshots table
    """
    test_case_mapping = {
        "snapshots": "aggregation_spec_with_snapshots_parameters",
        "snapshots_with_offset": "aggregation_spec_with_snapshots_parameters_and_offset",
    }
    fixture_name = test_case_mapping[test_case_name]
    fixture_obj = request.getfixturevalue(fixture_name)
    if isinstance(fixture_obj, list):
        agg_specs = fixture_obj
    else:
        agg_specs = [fixture_obj]

    aggregator = AsAtAggregator(source_info=source_info)
    for agg_spec in agg_specs:
        aggregator.update(agg_spec)
    result_expr = get_combined_aggregation_expr_from_aggregator(aggregator)
    assert_equal_with_expected_fixture(
        result_expr.sql(pretty=True),
        f"tests/fixtures/aggregator/expected_asat_aggregator_{test_case_name}.sql",
        update_fixture=update_fixtures,
    )
