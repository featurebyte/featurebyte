"""
Snowflake double vector agg no value by test fixture module
"""
import textwrap

SNOWFLAKE_DOUBLE_VECTOR_AGG_NO_VALUE_BY_QUERY = textwrap.dedent(
    """
        SELECT
          VECTOR_T0."serving_name" AS "serving_name",
          VECTOR_T0."POINT_IN_TIME" AS "POINT_IN_TIME",
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
    """
)
