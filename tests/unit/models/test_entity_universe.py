"""
Tests for entity_universe.py
"""

import textwrap
from datetime import datetime

import pytest
from bson import ObjectId

from featurebyte import SourceType
from featurebyte.models.entity_universe import (
    EntityUniverseModel,
    EntityUniverseParams,
    get_combined_universe,
    get_entity_universe_constructor,
)
from featurebyte.models.parent_serving import EntityLookupInfo, EntityLookupStep
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.enum import NodeType


def get_nodes_from_feature(feature, node_type):
    """
    Get node from feature
    """
    return list(feature.graph.iterate_nodes(feature.node, node_type))


def get_node_from_feature(feature, node_type):
    """
    Get node from feature
    """
    return get_nodes_from_feature(feature, node_type)[0]


@pytest.fixture
def lookup_graph_and_node(scd_lookup_feature):
    """
    Fixture for a lookup aggregate node
    """
    graph = scd_lookup_feature.graph
    lookup_node = get_node_from_feature(scd_lookup_feature, NodeType.LOOKUP)
    return graph, lookup_node


@pytest.fixture
def aggregate_asat_graph_and_node(aggregate_asat_feature):
    """
    Fixture for an aggregate_asat aggregate node
    """
    graph = aggregate_asat_feature.graph
    aggregate_asat_node = get_node_from_feature(aggregate_asat_feature, NodeType.AGGREGATE_AS_AT)
    return graph, aggregate_asat_node


@pytest.fixture
def aggregate_asat_no_entity_graph_and_node(aggregate_asat_no_entity_feature):
    """
    Fixture for an aggregate_asat aggregate node (no entity)
    """
    graph = aggregate_asat_no_entity_feature.graph
    aggregate_asat_node = get_node_from_feature(aggregate_asat_no_entity_feature, NodeType.AGGREGATE_AS_AT)
    return graph, aggregate_asat_node


@pytest.fixture
def lookup_graph_and_node_same_input(scd_lookup_feature):
    """
    Fixture for a new lookup feature that has the same input as the original lookup feature
    """
    new_feature = scd_lookup_feature.notnull()
    new_feature.name = "new_lookup_feature"
    graph = new_feature.graph
    lookup_node = get_node_from_feature(new_feature, NodeType.LOOKUP)
    return graph, lookup_node


@pytest.fixture
def item_aggregate_graph_and_node(filtered_non_time_based_feature):
    """
    Fixture for an item aggregate node
    """
    graph = filtered_non_time_based_feature.graph
    item_aggregate_node = get_node_from_feature(filtered_non_time_based_feature, NodeType.ITEM_GROUPBY)
    return graph, item_aggregate_node


@pytest.fixture
def window_aggregate_graph_and_node(float_feature_different_job_setting):
    """
    Fixture for a groupby node with entity
    """
    graph = float_feature_different_job_setting.graph
    groupby_node = get_node_from_feature(float_feature_different_job_setting, NodeType.GROUPBY)
    return graph, groupby_node


@pytest.fixture
def window_aggregate_no_entity_graph_and_node(feature_without_entity):
    """
    Fixture for a groupby node without entity
    """
    graph = feature_without_entity.graph
    groupby_node = get_node_from_feature(feature_without_entity, NodeType.GROUPBY)
    return graph, groupby_node


@pytest.fixture
def window_aggregate_multiple_windows(float_feature_multiple_windows):
    """
    Fixture for groupby node supporting complex feature with multiple windows
    """
    graph = float_feature_multiple_windows.graph
    groupby_nodes = get_nodes_from_feature(float_feature_multiple_windows, NodeType.GROUPBY)
    return graph, groupby_nodes


@pytest.fixture
def join_steps(snowflake_scd_table_with_entity):
    """
    Fixture for a join steps to be applied when constructing entity universe
    """
    return [
        EntityLookupStep(
            id=ObjectId(),
            table=snowflake_scd_table_with_entity.cached_model,
            parent=EntityLookupInfo(
                key="col_text",
                serving_name="cust_id",
                entity_id=ObjectId(),
            ),
            child=EntityLookupInfo(
                key="cust_id_child",
                serving_name="cust_id_child_serving_name",
                entity_id=ObjectId(),
            ),
        )
    ]


def assert_one_item_and_format_sql(universe_template):
    """
    Helper function to check that universe template has a single item and return formatted sql
    """
    assert len(universe_template) == 1
    return universe_template[0].sql(pretty=True)


def test_lookup_feature(catalog, lookup_graph_and_node):
    """
    Test lookup feature's universe
    """
    _ = catalog
    graph, node = lookup_graph_and_node
    constructor = get_entity_universe_constructor(graph, node, SourceType.SNOWFLAKE)
    expected = textwrap.dedent(
        """
        SELECT DISTINCT
          "col_text" AS "cust_id"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "effective_timestamp" AS "effective_timestamp",
            "end_timestamp" AS "end_timestamp",
            "date_of_birth" AS "date_of_birth",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."scd_table"
          WHERE
            "effective_timestamp" >= __fb_last_materialized_timestamp
            AND "effective_timestamp" < __fb_current_feature_timestamp
        )
        WHERE
          "col_text" IS NOT NULL
        """
    ).strip()
    assert assert_one_item_and_format_sql(constructor.get_entity_universe_template()) == expected


def test_aggregate_asat_universe(catalog, aggregate_asat_graph_and_node):
    """
    Test aggregate as-at feature's universe
    """
    _ = catalog
    graph, node = aggregate_asat_graph_and_node
    constructor = get_entity_universe_constructor(graph, node, SourceType.SNOWFLAKE)
    expected = textwrap.dedent(
        """
        SELECT DISTINCT
          "col_boolean" AS "gender"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "effective_timestamp" AS "effective_timestamp",
            "end_timestamp" AS "end_timestamp",
            "date_of_birth" AS "date_of_birth",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."scd_table"
          WHERE
            "effective_timestamp" >= __fb_last_materialized_timestamp
            AND "effective_timestamp" < __fb_current_feature_timestamp
        )
        WHERE
          "col_boolean" IS NOT NULL
        """
    ).strip()
    assert assert_one_item_and_format_sql(constructor.get_entity_universe_template()) == expected


def test_aggregate_asat_no_entity_universe(catalog, aggregate_asat_no_entity_graph_and_node):
    """
    Test aggregate as-at feature's universe (no entity)
    """
    _ = catalog
    graph, node = aggregate_asat_no_entity_graph_and_node
    constructor = get_entity_universe_constructor(graph, node, SourceType.SNOWFLAKE)
    expected = textwrap.dedent(
        """
        SELECT
          1 AS "dummy_entity"
        """
    ).strip()
    assert assert_one_item_and_format_sql(constructor.get_entity_universe_template()) == expected


def test_item_aggregate_universe(catalog, item_aggregate_graph_and_node):
    """
    Test item aggregate feature's universe
    """
    _ = catalog
    graph, node = item_aggregate_graph_and_node
    constructor = get_entity_universe_constructor(graph, node, SourceType.SNOWFLAKE)
    expected = textwrap.dedent(
        """
        SELECT DISTINCT
          CAST("event_id_col" AS BIGINT) AS "transaction_id"
        FROM (
          SELECT
            L."event_id_col" AS "event_id_col",
            L."item_id_col" AS "item_id_col",
            L."item_type" AS "item_type",
            L."item_amount" AS "item_amount",
            L."created_at" AS "created_at",
            L."event_timestamp" AS "event_timestamp",
            R."event_timestamp" AS "event_timestamp_event_table"
          FROM (
            SELECT
              "event_id_col" AS "event_id_col",
              "item_id_col" AS "item_id_col",
              "item_type" AS "item_type",
              "item_amount" AS "item_amount",
              "created_at" AS "created_at",
              "event_timestamp" AS "event_timestamp"
            FROM "sf_database"."sf_schema"."items_table"
          ) AS L
          INNER JOIN (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "col_char" AS "col_char",
              "col_text" AS "col_text",
              "col_binary" AS "col_binary",
              "col_boolean" AS "col_boolean",
              "event_timestamp" AS "event_timestamp",
              "cust_id" AS "cust_id"
            FROM "sf_database"."sf_schema"."sf_table"
            WHERE
              "event_timestamp" >= __fb_last_materialized_timestamp
              AND "event_timestamp" < __fb_current_feature_timestamp
          ) AS R
            ON L."event_id_col" = R."col_int"
          WHERE
            (
              L."item_amount" > 10
            )
        )
        WHERE
          "event_id_col" IS NOT NULL
        """
    ).strip()
    assert assert_one_item_and_format_sql(constructor.get_entity_universe_template()) == expected


def test_combined_universe(catalog, lookup_graph_and_node, aggregate_asat_graph_and_node):
    """
    Test combined universe
    """
    _ = catalog
    # Note: in practice the two universes should have the same serving name, though that is not the
    # case in this test.
    universe = get_combined_universe(
        [
            EntityUniverseParams(
                graph=lookup_graph_and_node[0],
                node=lookup_graph_and_node[1],
                join_steps=None,
            ),
            EntityUniverseParams(
                graph=aggregate_asat_graph_and_node[0],
                node=aggregate_asat_graph_and_node[1],
                join_steps=None,
            ),
        ],
        SourceType.SNOWFLAKE,
    )
    expected = textwrap.dedent(
        """
        SELECT DISTINCT
          "col_boolean" AS "gender"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "effective_timestamp" AS "effective_timestamp",
            "end_timestamp" AS "end_timestamp",
            "date_of_birth" AS "date_of_birth",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."scd_table"
          WHERE
            "effective_timestamp" >= __fb_last_materialized_timestamp
            AND "effective_timestamp" < __fb_current_feature_timestamp
        )
        WHERE
          "col_boolean" IS NOT NULL
        UNION
        SELECT DISTINCT
          "col_text" AS "cust_id"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "effective_timestamp" AS "effective_timestamp",
            "end_timestamp" AS "end_timestamp",
            "date_of_birth" AS "date_of_birth",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."scd_table"
          WHERE
            "effective_timestamp" >= __fb_last_materialized_timestamp
            AND "effective_timestamp" < __fb_current_feature_timestamp
        )
        WHERE
          "col_text" IS NOT NULL
        """
    ).strip()
    assert universe.sql(pretty=True) == expected


def test_combined_universe_deduplicate(catalog, lookup_graph_and_node, lookup_graph_and_node_same_input):
    """
    Test combined universe doesn't duplicate the same universe
    """
    _ = catalog
    universe = get_combined_universe(
        [
            EntityUniverseParams(
                graph=lookup_graph_and_node[0],
                node=lookup_graph_and_node[1],
                join_steps=None,
            ),
            EntityUniverseParams(
                graph=lookup_graph_and_node_same_input[0],
                node=lookup_graph_and_node_same_input[1],
                join_steps=None,
            ),
        ],
        SourceType.SNOWFLAKE,
    )
    expected = textwrap.dedent(
        """
        SELECT DISTINCT
          "col_text" AS "cust_id"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "effective_timestamp" AS "effective_timestamp",
            "end_timestamp" AS "end_timestamp",
            "date_of_birth" AS "date_of_birth",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."scd_table"
          WHERE
            "effective_timestamp" >= __fb_last_materialized_timestamp
            AND "effective_timestamp" < __fb_current_feature_timestamp
        )
        WHERE
          "col_text" IS NOT NULL
        """
    ).strip()
    assert universe.sql(pretty=True) == expected


def test_combined_universe__join_steps(catalog, lookup_graph_and_node, join_steps):
    """
    Test combined universe with join steps
    """
    _ = catalog
    universe = get_combined_universe(
        [
            EntityUniverseParams(
                graph=lookup_graph_and_node[0],
                node=lookup_graph_and_node[1],
                join_steps=join_steps,
            ),
        ],
        SourceType.SNOWFLAKE,
    )
    expected = textwrap.dedent(
        """
        SELECT DISTINCT
          CHILD."cust_id_child" AS "cust_id_child_serving_name"
        FROM (
          SELECT DISTINCT
            "col_text" AS "cust_id"
          FROM (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "col_text" AS "col_text",
              "col_binary" AS "col_binary",
              "col_boolean" AS "col_boolean",
              "effective_timestamp" AS "effective_timestamp",
              "end_timestamp" AS "end_timestamp",
              "date_of_birth" AS "date_of_birth",
              "created_at" AS "created_at",
              "cust_id" AS "cust_id"
            FROM "sf_database"."sf_schema"."scd_table"
            WHERE
              "effective_timestamp" >= __fb_last_materialized_timestamp
              AND "effective_timestamp" < __fb_current_feature_timestamp
          )
          WHERE
            "col_text" IS NOT NULL
        ) AS PARENT
        LEFT JOIN "sf_database"."sf_schema"."scd_table" AS CHILD
          ON PARENT."cust_id" = CHILD."col_text"
        """
    ).strip()
    assert universe.sql(pretty=True) == expected


def test_combined_universe__output_dummy_entity_universe(catalog, window_aggregate_no_entity_graph_and_node):
    """
    Test combined universe should include dummy entity universe only when there are no other entity
    universes to be combined
    """
    _ = catalog
    universe = get_combined_universe(
        [
            EntityUniverseParams(
                graph=window_aggregate_no_entity_graph_and_node[0],
                node=window_aggregate_no_entity_graph_and_node[1],
                join_steps=None,
            ),
        ],
        SourceType.SNOWFLAKE,
    )
    expected = textwrap.dedent(
        """
        SELECT
          1 AS "dummy_entity"
        """
    ).strip()
    assert universe.sql(pretty=True) == expected


def test_combined_universe__exclude_dummy_entity_universe(
    catalog, window_aggregate_graph_and_node, window_aggregate_no_entity_graph_and_node
):
    """
    Test combined universe should exclude dummy entity universe only when there are other entity
    universes to be combined
    """
    _ = catalog
    universe = get_combined_universe(
        [
            EntityUniverseParams(
                graph=window_aggregate_no_entity_graph_and_node[0],
                node=window_aggregate_no_entity_graph_and_node[1],
                join_steps=None,
            ),
            EntityUniverseParams(
                graph=window_aggregate_graph_and_node[0],
                node=window_aggregate_graph_and_node[1],
                join_steps=None,
            ),
        ],
        SourceType.SNOWFLAKE,
    )
    expected = textwrap.dedent(
        """
        SELECT DISTINCT
          CAST("cust_id" AS BIGINT) AS "cust_id"
        FROM online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c
        WHERE
          "AGGREGATION_RESULT_NAME" = '_fb_internal_cust_id_window_w86400_sum_420f46a4414d6fc926c85a1349835967a96bf4c2'
          AND "cust_id" IS NOT NULL
        """
    ).strip()
    assert universe.sql(pretty=True) == expected


def test_combined_universe__window_aggregate_multiple_windows(
    catalog, window_aggregate_graph_and_node, window_aggregate_multiple_windows
):
    """
    Test constructing universe for a window aggregate involving multiple windows
    """
    _ = catalog
    universe = get_combined_universe(
        *(
            [
                EntityUniverseParams(
                    graph=window_aggregate_graph_and_node[0],
                    node=window_aggregate_graph_and_node[1],
                    join_steps=None,
                ),
            ]
            + [
                EntityUniverseParams(
                    graph=window_aggregate_multiple_windows[0],
                    node=groupby_node,
                    join_steps=None,
                )
                for groupby_node in window_aggregate_multiple_windows[1]
            ],
        ),
        SourceType.SNOWFLAKE,
    )
    expected = textwrap.dedent(
        """
        SELECT DISTINCT
          CAST("cust_id" AS BIGINT) AS "cust_id"
        FROM online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c
        WHERE
          "AGGREGATION_RESULT_NAME" = '_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'
          AND "cust_id" IS NOT NULL
        UNION
        SELECT DISTINCT
          CAST("cust_id" AS BIGINT) AS "cust_id"
        FROM online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c
        WHERE
          "AGGREGATION_RESULT_NAME" = '_fb_internal_cust_id_window_w7200_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'
          AND "cust_id" IS NOT NULL
        UNION
        SELECT DISTINCT
          CAST("cust_id" AS BIGINT) AS "cust_id"
        FROM online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c
        WHERE
          "AGGREGATION_RESULT_NAME" = '_fb_internal_cust_id_window_w86400_sum_420f46a4414d6fc926c85a1349835967a96bf4c2'
          AND "cust_id" IS NOT NULL
        """
    ).strip()
    assert universe.sql(pretty=True) == expected


def test_entity_universe_model_get_entity_universe_expr(catalog, lookup_graph_and_node):
    """
    Test EntityUniverseModel get_entity_universe_expr() method
    """
    _ = catalog
    graph, node = lookup_graph_and_node
    constructor = get_entity_universe_constructor(graph, node, SourceType.SNOWFLAKE)
    query_template = constructor.get_entity_universe_template()[0]
    entity_universe_model = EntityUniverseModel(query_template=SqlglotExpressionModel.create(query_template))
    expected = textwrap.dedent(
        """
        SELECT DISTINCT
          "col_text" AS "cust_id"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "effective_timestamp" AS "effective_timestamp",
            "end_timestamp" AS "end_timestamp",
            "date_of_birth" AS "date_of_birth",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."scd_table"
          WHERE
            "effective_timestamp" >= CAST('2022-10-15 09:00:00' AS TIMESTAMP)
            AND "effective_timestamp" < CAST('2022-10-15 10:00:00' AS TIMESTAMP)
        )
        WHERE
          NOT "col_text" IS NULL
        """
    ).strip()
    actual = entity_universe_model.get_entity_universe_expr(
        current_feature_timestamp=datetime(2022, 10, 15, 10, 0, 0),
        last_materialized_timestamp=datetime(2022, 10, 15, 9, 0, 0),
    ).sql(pretty=True)
    assert actual == expected
