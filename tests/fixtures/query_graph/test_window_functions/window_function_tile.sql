WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "ts" AS "ts",
      "cust_id" AS "cust_id",
      LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts") AS "input_col_count_c757fc5f20a0a22e1a234ed081c52553f524aa19"
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
  COUNT(*) AS value_count_c757fc5f20a0a22e1a234ed081c52553f524aa19
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP), 600, 1, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"
