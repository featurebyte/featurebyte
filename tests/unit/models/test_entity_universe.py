"""
Tests for entity_universe.py
"""
import textwrap
from datetime import datetime

import pytest

from featurebyte import SourceType
from featurebyte.models.entity_universe import (
    EntityUniverseModel,
    get_combined_universe,
    get_entity_universe_constructor,
)
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.enum import NodeType


def get_node_from_feature(feature, node_type):
    """
    Get node from feature
    """
    return next(feature.graph.iterate_nodes(feature.node, node_type))


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
def lookup_graph_and_node_same_input(scd_lookup_feature):
    """
    Fixture for a new lookup feature that has the same input as the original lookup feature
    """
    new_feature = scd_lookup_feature.notnull()
    new_feature.name = "new_lookup_feature"
    graph = new_feature.graph
    lookup_node = get_node_from_feature(new_feature, NodeType.LOOKUP)
    return graph, lookup_node


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
        """
    ).strip()
    assert constructor.get_entity_universe_template().sql(pretty=True) == expected


def test_aggregate_asat_universe(catalog, aggregate_asat_graph_and_node):
    """
    Test lookup feature's universe
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
        """
    ).strip()
    assert constructor.get_entity_universe_template().sql(pretty=True) == expected


def test_combined_universe(catalog, lookup_graph_and_node, aggregate_asat_graph_and_node):
    """
    Test combined universe
    """
    _ = catalog
    # Note: in practice the two universes should have the same serving name, though that is not the
    # case in this test.
    universe = get_combined_universe(
        [lookup_graph_and_node, aggregate_asat_graph_and_node], SourceType.SNOWFLAKE
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
        """
    ).strip()
    assert universe.sql(pretty=True) == expected


def test_combined_universe_deduplicate(
    catalog, lookup_graph_and_node, lookup_graph_and_node_same_input
):
    """
    Test combined universe doesn't duplicate the same universe
    """
    _ = catalog
    universe = get_combined_universe(
        [lookup_graph_and_node, lookup_graph_and_node_same_input], SourceType.SNOWFLAKE
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
    query_template = constructor.get_entity_universe_template()
    entity_universe_model = EntityUniverseModel(
        query_template=SqlglotExpressionModel.create(query_template)
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
            "effective_timestamp" >= CAST('2022-10-15 09:00:00' AS TIMESTAMP)
            AND "effective_timestamp" < CAST('2022-10-15 10:00:00' AS TIMESTAMP)
        )
        """
    ).strip()
    actual = entity_universe_model.get_entity_universe_expr(
        current_feature_timestamp=datetime(2022, 10, 15, 10, 0, 0),
        last_materialized_timestamp=datetime(2022, 10, 15, 9, 0, 0),
    ).sql(pretty=True)
    assert actual == expected
