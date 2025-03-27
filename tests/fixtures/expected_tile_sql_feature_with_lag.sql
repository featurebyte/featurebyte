WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id",
      LAG("col_float", 1) OVER (PARTITION BY "cust_id" ORDER BY "event_timestamp" NULLS LAST) AS "input_col_sum_2005a2fb58d595f8177ab790020f757a340d9725"
    FROM "sf_database"."sf_schema"."sf_table"
  )
  WHERE
    "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
    AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
)
SELECT
  index,
  "cust_id",
  SUM("input_col_sum_2005a2fb58d595f8177ab790020f757a340d9725") AS value_sum_2005a2fb58d595f8177ab790020f757a340d9725
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 300, 600, 30) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"
