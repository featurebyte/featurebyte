WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "ts" AS "ts",
      "cust_id" AS "cust_id",
      "a" AS "a",
      "b" AS "b",
      LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts") AS "prev_a"
    FROM "db"."public"."event_table"
    WHERE
      (
        "a" > 1000
      )
  )
  WHERE
    "ts" >= CAST(__FB_START_DATE AS TIMESTAMP)
    AND "ts" < CAST(__FB_END_DATE AS TIMESTAMP)
)
SELECT
  index,
  "cust_id",
  COUNT(*) AS value_count_e2199e4018415026611a08411ff11e9ec48e0ca5
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 600, 1, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"
