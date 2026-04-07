WITH _FB_AGGREGATED AS (
  SELECT
    "cust_id" AS "cust_id",
    SUM("col_float") AS "_fb_internal_cust_id_window_w86400_sum_58d51b03babb56d9c8829eb11257b24a7e088596",
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
      (
        (
          "event_timestamp" >= DATEADD(MONTH, -1, DATEADD(MINUTE, -1440, {{ CURRENT_TIMESTAMP }}))
          AND "event_timestamp" <= DATEADD(MONTH, 1, {{ CURRENT_TIMESTAMP }})
        )
        AND (
          "col_float" < 10
        )
      )
      AND (
        "event_timestamp" >= CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 - 86400 AS TIMESTAMP)
        AND "event_timestamp" < CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 AS TIMESTAMP)
      )
  )
  GROUP BY
    "cust_id"
)
SELECT
  AGG."cust_id",
  CAST("_fb_internal_cust_id_window_w86400_sum_58d51b03babb56d9c8829eb11257b24a7e088596" AS DOUBLE) AS "sum_1d_lt_10",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG