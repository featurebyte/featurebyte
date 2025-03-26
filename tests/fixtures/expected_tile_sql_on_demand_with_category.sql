WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "ts" AS "ts",
      "cust_id" AS "cust_id",
      "product_type" AS "product_type",
      "a" AS "input_col_avg_dfee6d136c6d6db110606afd33ae34deb9b5e96f"
    FROM "db"."public"."event_table"
  )
  WHERE
    "ts" >= CAST(__FB_START_DATE AS TIMESTAMP)
    AND "ts" < CAST(__FB_END_DATE AS TIMESTAMP)
)
SELECT
  index,
  "cust_id",
  "product_type",
  SUM("input_col_avg_dfee6d136c6d6db110606afd33ae34deb9b5e96f") AS sum_value_avg_dfee6d136c6d6db110606afd33ae34deb9b5e96f,
  COUNT("input_col_avg_dfee6d136c6d6db110606afd33ae34deb9b5e96f") AS count_value_avg_dfee6d136c6d6db110606afd33ae34deb9b5e96f
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP), 1800, 900, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id",
  "product_type"
