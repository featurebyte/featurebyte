SELECT
  index,
  "cust_id",
  "product_type",
  SUM("a") AS sum_value_avg_4478d266b052ffb6b332e2ec9c2e486fca6c23c6,
  COUNT("a") AS count_value_avg_4478d266b052ffb6b332e2ec9c2e486fca6c23c6
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
  FROM (
    SELECT
      *
    FROM (
      SELECT
        "ts" AS "ts",
        "cust_id" AS "cust_id",
        "a" AS "a",
        "b" AS "b",
        (
          "a" + "b"
        ) AS "c"
      FROM "db"."public"."event_table"
    )
    WHERE
      "ts" >= CAST(__FB_START_DATE AS TIMESTAMP)
      AND "ts" < CAST(__FB_END_DATE AS TIMESTAMP)
  )
)
GROUP BY
  index,
  "cust_id",
  "product_type"
