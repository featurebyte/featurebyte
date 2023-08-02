SELECT
  index,
  "cust_id",
  SUM("col_float") AS value_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "event_timestamp"), 300, 600, 30) AS index
  FROM (
    SELECT
      *
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
        "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
        AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
    )
    WHERE
      "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
      AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
  )
)
GROUP BY
  index,
  "cust_id"
