SELECT
  index,
  "cust_id",
  SUM("lag_col") AS value_sum_23fef7e510b53d16fcdcebb208add139a307f458
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
        "cust_id" AS "cust_id",
        LAG("col_float", 1) OVER (PARTITION BY "cust_id" ORDER BY "event_timestamp") AS "lag_col"
      FROM "sf_database"."sf_schema"."sf_table"
    )
    WHERE
      "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
      AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
  )
)
GROUP BY
  index,
  "cust_id"
