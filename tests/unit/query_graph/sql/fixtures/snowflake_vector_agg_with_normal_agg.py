"""
Snowflake vector agg with normal agg test fixture module
"""
import textwrap

SNOWFLAKE_VECTOR_AGG_WITH_NORMAL_AGG_QUERY = textwrap.dedent(
    """
        SELECT
          INNER_."serving_name",
          INNER_."POINT_IN_TIME",
          OBJECT_AGG(
            CASE
              WHEN INNER_."value_by" IS NULL
              THEN '__MISSING__'
              ELSE CAST(INNER_."value_by" AS TEXT)
            END,
            TO_VARIANT(INNER_."result_0_inner")
          ) AS "result_0",
          OBJECT_AGG(
            CASE
              WHEN INNER_."value_by" IS NULL
              THEN '__MISSING__'
              ELSE CAST(INNER_."value_by" AS TEXT)
            END,
            TO_VARIANT(INNER_."result_1_inner")
          ) AS "result_1"
        FROM (
          SELECT
            REQ."serving_name" AS "serving_name",
            REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
            REQ."value_by" AS "value_by",
            GROUP_BY_RESULT."result_1_inner" AS "result_1_inner",
            T0.result_0 AS "result_0"
          FROM REQ, (
            SELECT
              REQ."serving_name" AS "serving_name",
              REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
              AGG_0.VECTOR_AGG_RESULT AS "result_0"
            FROM REQ, TABLE(
              VECTOR_AGGREGATE_MAX(TABLE."parent") OVER (PARTITION BY REQ."serving_name", REQ."POINT_IN_TIME")
            ) AS "AGG_0"
          ) AS T0
          INNER JOIN (
            SELECT
              REQ."serving_name" AS "serving_name",
              REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
              REQ."value_by" AS "value_by",
              MAX(TABLE."parent") AS "result_1_inner"
            FROM REQ
            GROUP BY
              REQ."serving_name",
              REQ."POINT_IN_TIME",
              REQ."value_by"
          ) AS GROUP_BY_RESULT
            ON GROUP_BY_RESULT."serving_name" = T0."serving_name"
            AND GROUP_BY_RESULT."POINT_IN_TIME" = T0."POINT_IN_TIME"
            AND GROUP_BY_RESULT."value_by" = T0."value_by"
        ) AS INNER_
        GROUP BY
          INNER_."serving_name",
          INNER_."POINT_IN_TIME"
    """
)
