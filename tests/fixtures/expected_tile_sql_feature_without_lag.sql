SELECT
  index,
  "cust_id",
  SUM("col_float") AS value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
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
        "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
        AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
    )
    WHERE
      "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
      AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
  )
)
GROUP BY
  index,
  "cust_id"
