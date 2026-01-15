WITH DEPLOYMENT_REQUEST_TABLE AS (
  SELECT
    REQ."cust_id",
    {{ CURRENT_TIMESTAMP }} AS POINT_IN_TIME
  FROM (
    SELECT DISTINCT
      CAST("store_id" AS BIGINT) AS "cust_id"
    FROM (
      SELECT
        "col_int" AS "col_int",
        "col_float" AS "col_float",
        "col_char" AS "col_char",
        "col_text" AS "col_text",
        "col_binary" AS "col_binary",
        "col_boolean" AS "col_boolean",
        "date" AS "date",
        "store_id" AS "store_id",
        "another_timestamp_col" AS "another_timestamp_col"
      FROM "sf_database"."sf_schema"."time_series_table"
      WHERE
        (
          "date" >= TO_CHAR(
            DATEADD(MONTH, -1, DATEADD(MONTH, -3, {{ CURRENT_TIMESTAMP }})),
            'YYYY-MM-DD HH24:MI:SS'
          )
          AND "date" <= TO_CHAR(DATEADD(MONTH, 1, {{ CURRENT_TIMESTAMP }}), 'YYYY-MM-DD HH24:MI:SS')
        )
        AND (
          TO_TIMESTAMP("date", 'yyyy-mm-DD hh24:mi:ss') >= DATEADD(MONTH, -3, DATE_TRUNC('MONTH', {{ CURRENT_TIMESTAMP }}))
          AND TO_TIMESTAMP("date", 'yyyy-mm-DD hh24:mi:ss') < DATE_TRUNC('MONTH', {{ CURRENT_TIMESTAMP }})
        )
    )
    WHERE
      NOT "store_id" IS NULL
  ) AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."cust_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_project_1" AS "_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_project_1"
  FROM DEPLOYMENT_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "store_id" AS "cust_id",
      SUM("col_float") AS "_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_project_1"
    FROM (
      SELECT
        "col_int" AS "col_int",
        "col_float" AS "col_float",
        "col_char" AS "col_char",
        "col_text" AS "col_text",
        "col_binary" AS "col_binary",
        "col_boolean" AS "col_boolean",
        "date" AS "date",
        "store_id" AS "store_id",
        "another_timestamp_col" AS "another_timestamp_col"
      FROM "sf_database"."sf_schema"."time_series_table"
      WHERE
        (
          "date" >= TO_CHAR(
            DATEADD(MONTH, -1, DATEADD(MONTH, -3, {{ CURRENT_TIMESTAMP }})),
            'YYYY-MM-DD HH24:MI:SS'
          )
          AND "date" <= TO_CHAR(DATEADD(MONTH, 1, {{ CURRENT_TIMESTAMP }}), 'YYYY-MM-DD HH24:MI:SS')
        )
        AND (
          TO_TIMESTAMP("date", 'YYYY-MM-DD HH24:MI:SS') >= DATEADD(MONTH, -3, DATE_TRUNC('month', {{ CURRENT_TIMESTAMP }}))
          AND TO_TIMESTAMP("date", 'YYYY-MM-DD HH24:MI:SS') < DATE_TRUNC('month', {{ CURRENT_TIMESTAMP }})
        )
    )
    GROUP BY
      "store_id"
  ) AS T0
    ON REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."cust_id",
  CAST("_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_project_1" AS DOUBLE) AS "col_float_sum_3month",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG