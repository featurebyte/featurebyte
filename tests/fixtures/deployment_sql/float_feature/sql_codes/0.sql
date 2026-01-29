WITH _FB_AGGREGATED AS (
  SELECT
    "cust_id" AS "cust_id",
    SUM("col_float") AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
    {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
  FROM (
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
      "event_timestamp" >= CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 - 86400 AS TIMESTAMP)
      AND "event_timestamp" < CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 AS TIMESTAMP)
  )
  GROUP BY
    "cust_id"
)
SELECT
  AGG."cust_id",
  CAST("_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS DOUBLE) AS "sum_1d",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG