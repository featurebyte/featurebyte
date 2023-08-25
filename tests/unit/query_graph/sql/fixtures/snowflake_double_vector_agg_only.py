"""
Snowflake double vector agg only test fixture module
"""
import textwrap

SNOWFLAKE_DOUBLE_VECTOR_AGG_ONLY_QUERY = textwrap.dedent(
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
            VECTOR_T0."serving_name" AS "serving_name",
            VECTOR_T0."POINT_IN_TIME" AS "POINT_IN_TIME",
            VECTOR_T0."value_by" AS "value_by",
            VECTOR_T0."result_0" AS "result_0",
            VECTOR_T1."result_1" AS "result_1"
          FROM (
            SELECT
              INITIAL_DATA."serving_name" AS "serving_name",
              INITIAL_DATA."POINT_IN_TIME" AS "POINT_IN_TIME",
              AGG_0."VECTOR_AGG_RESULT" AS "result_0"
            FROM (
              SELECT
                REQ."serving_name" AS "serving_name",
                REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
                TABLE."parent" AS "parent"
              FROM REQ
            ) AS INITIAL_DATA, TABLE(
              VECTOR_AGGREGATE_MAX(parent) OVER (PARTITION BY INITIAL_DATA."serving_name", INITIAL_DATA."POINT_IN_TIME")
            ) AS "AGG_0"
          ) AS VECTOR_T0
          INNER JOIN (
            SELECT
              INITIAL_DATA."serving_name" AS "serving_name",
              INITIAL_DATA."POINT_IN_TIME" AS "POINT_IN_TIME",
              AGG_1."VECTOR_AGG_RESULT" AS "result_1"
            FROM (
              SELECT
                REQ."serving_name" AS "serving_name",
                REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
                TABLE."parent" AS "parent"
              FROM REQ
            ) AS INITIAL_DATA, TABLE(
              VECTOR_AGGREGATE_MAX(parent) OVER (PARTITION BY INITIAL_DATA."serving_name", INITIAL_DATA."POINT_IN_TIME")
            ) AS "AGG_1"
          ) AS VECTOR_T1
            ON VECTOR_T0."serving_name" = VECTOR_T1."serving_name"
            AND VECTOR_T0."POINT_IN_TIME" = VECTOR_T1."POINT_IN_TIME"
            AND VECTOR_T0."value_by" = VECTOR_T1."value_by"
        ) AS INNER_
        GROUP BY
          INNER_."serving_name",
          INNER_."POINT_IN_TIME"
    """
)
