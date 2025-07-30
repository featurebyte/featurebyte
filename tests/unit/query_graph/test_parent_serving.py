"""
Tests sql generation for parent features serving
"""

import textwrap

from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.parent_serving import construct_request_table_with_parent_entities


def test_construct_request_table_with_parent_entities(parent_serving_preparation):
    result = construct_request_table_with_parent_entities(
        "REQUEST_TABLE",
        request_table_columns=["a", "b"],
        join_steps=parent_serving_preparation.join_steps,
        feature_store_details=parent_serving_preparation.feature_store_details,
    )
    expected = textwrap.dedent(
        """
        SELECT
          REQ."a" AS "a",
          REQ."b" AS "b",
          REQ."COL_INT" AS "COL_INT"
        FROM (
          SELECT
            REQ."a",
            REQ."b",
            "T0"."COL_INT" AS "COL_INT"
          FROM "REQUEST_TABLE" AS REQ
          LEFT JOIN (
            SELECT
              "COL_TEXT",
              ANY_VALUE("COL_INT") AS "COL_INT"
            FROM (
              SELECT
                "col_text" AS "COL_TEXT",
                "col_int" AS "COL_INT"
              FROM (
                SELECT
                  "col_int" AS "col_int",
                  "col_float" AS "col_float",
                  "col_char" AS "col_char",
                  "col_text" AS "col_text",
                  "col_binary" AS "col_binary",
                  "col_boolean" AS "col_boolean",
                  "event_timestamp" AS "event_timestamp",
                  "created_at" AS "created_at",
                  "cust_id" AS "cust_id"
                FROM "sf_database"."sf_schema"."dimension_table"
              )
            )
            GROUP BY
              "COL_TEXT"
          ) AS T0
            ON REQ."COL_TEXT" = T0."COL_TEXT"
        ) AS REQ
        """
    ).strip()
    assert result.table_expr.sql(pretty=True) == expected
    assert result.parent_entity_columns == ["COL_INT"]
    assert result.new_request_table_name == "JOINED_PARENTS_REQUEST_TABLE"
    assert result.new_request_table_columns == ["a", "b", "COL_INT"]


def test_construct_request_table_with_parent_entities_table_details(parent_serving_preparation):
    """
    Test construct_request_table_with_parent_entities when the request table is a table in the
    warehouse
    """
    result = construct_request_table_with_parent_entities(
        None,
        request_table_columns=["a", "b"],
        join_steps=parent_serving_preparation.join_steps,
        feature_store_details=parent_serving_preparation.feature_store_details,
        request_table_details=TableDetails(
            database_name="my_db", schema_name="my_schema", table_name="my_table"
        ),
    )
    expected = textwrap.dedent(
        """
        SELECT
          REQ."a" AS "a",
          REQ."b" AS "b",
          REQ."COL_INT" AS "COL_INT"
        FROM (
          SELECT
            REQ."a",
            REQ."b",
            "T0"."COL_INT" AS "COL_INT"
          FROM "my_db"."my_schema"."my_table" AS REQ
          LEFT JOIN (
            SELECT
              "COL_TEXT",
              ANY_VALUE("COL_INT") AS "COL_INT"
            FROM (
              SELECT
                "col_text" AS "COL_TEXT",
                "col_int" AS "COL_INT"
              FROM (
                SELECT
                  "col_int" AS "col_int",
                  "col_float" AS "col_float",
                  "col_char" AS "col_char",
                  "col_text" AS "col_text",
                  "col_binary" AS "col_binary",
                  "col_boolean" AS "col_boolean",
                  "event_timestamp" AS "event_timestamp",
                  "created_at" AS "created_at",
                  "cust_id" AS "cust_id"
                FROM "sf_database"."sf_schema"."dimension_table"
              )
            )
            GROUP BY
              "COL_TEXT"
          ) AS T0
            ON REQ."COL_TEXT" = T0."COL_TEXT"
        ) AS REQ
        """
    ).strip()
    assert result.table_expr.sql(pretty=True) == expected
    assert result.parent_entity_columns == ["COL_INT"]
    assert result.new_request_table_name == "JOINED_PARENTS"
    assert result.new_request_table_columns == ["a", "b", "COL_INT"]
