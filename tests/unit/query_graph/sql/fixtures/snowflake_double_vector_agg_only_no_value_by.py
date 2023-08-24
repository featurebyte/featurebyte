"""
Snowflake double vector agg no value by test fixture module
"""
import textwrap

SNOWFLAKE_DOUBLE_VECTOR_AGG_NO_VALUE_BY_QUERY = textwrap.dedent(
    """
        SELECT
          REQ."serving_name" AS "serving_name",
          REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
          T0.result_0 AS "result_0",
          T1.result_1 AS "result_1"
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
            AGG_1.VECTOR_AGG_RESULT AS "result_1"
          FROM REQ, TABLE(
            VECTOR_AGGREGATE_MAX(TABLE."parent") OVER (PARTITION BY REQ."serving_name", REQ."POINT_IN_TIME")
          ) AS "AGG_1"
        ) AS T1
          ON T0."serving_name" = T1."serving_name" AND T0."POINT_IN_TIME" = T1."POINT_IN_TIME"
    """
)
