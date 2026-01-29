WITH _FB_AGGREGATED AS (
  SELECT
    "cust_id",
    "_fb_internal_cust_id_window_w7776000_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2",
    {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
  FROM (
    SELECT
      ROW_NUMBER() OVER (PARTITION BY "cust_id" ORDER BY "event_timestamp" DESC NULLS LAST) AS "__FB_GROUPBY_HELPER_ROW_NUMBER",
      "cust_id" AS "cust_id",
      FIRST_VALUE("event_timestamp") OVER (PARTITION BY "cust_id" ORDER BY "event_timestamp" DESC NULLS LAST) AS "_fb_internal_cust_id_window_w7776000_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2"
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
        "event_timestamp" >= CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 - 7776000 AS TIMESTAMP)
        AND "event_timestamp" < CAST(DATE_PART(EPOCH_SECOND, {{ CURRENT_TIMESTAMP }}) - 600 AS TIMESTAMP)
    )
  )
  WHERE
    "__FB_GROUPBY_HELPER_ROW_NUMBER" = 1
)
SELECT
  AGG."cust_id",
  CAST((
    DATEDIFF(
      MICROSECOND,
      "_fb_internal_cust_id_window_w7776000_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2",
      "POINT_IN_TIME"
    ) * CAST(1 AS BIGINT) / CAST(86400000000 AS BIGINT)
  ) AS DOUBLE) AS "time_since_latest_event_timestamp",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG