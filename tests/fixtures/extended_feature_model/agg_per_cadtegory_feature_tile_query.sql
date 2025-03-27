WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id",
      "col_int" AS "col_int",
      "col_float" AS "input_col_sum_254bde514925221168a524ba7467c9b6ef83685d"
    FROM "sf_database"."sf_schema"."sf_table"
    WHERE
      "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
      AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
  )
  WHERE
    "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
    AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
)
SELECT
  index,
  "cust_id",
  "col_int",
  SUM("input_col_sum_254bde514925221168a524ba7467c9b6ef83685d") AS value_sum_254bde514925221168a524ba7467c9b6ef83685d
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 300, 600, 30) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id",
  "col_int"
