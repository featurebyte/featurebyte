"""
Tests sql generation for parent features serving
"""
import textwrap

from featurebyte.query_graph.sql.parent_serving import construct_request_table_with_parent_entities


def test_construct_request_table_with_parent_entities(parent_serving_preparation):
    out = construct_request_table_with_parent_entities(
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
          FROM REQUEST_TABLE AS REQ
          LEFT JOIN (
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
              FROM "sf_database"."sf_schema"."sf_table"
            )
          ) AS T0
            ON REQ."COL_TEXT" = T0."COL_TEXT"
        ) AS REQ
        """
    ).strip()
    assert out.sql(pretty=True) == expected
